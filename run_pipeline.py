import click
import yaml
import datetime
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.database import Base, Job, PipelineRun
from connectors.remotive import RemotiveConnector
from utils.dedup import is_duplicate
from utils.application_filter import has_already_applied
from utils.llm_analysis import analyze_job_with_ollama
from utils.scoring import score_job
from utils.resume_selector import select_resume
from utils.logger import setup_logger
import config

logger = setup_logger("run_pipeline")

CONNECTORS = {
    "remotive": RemotiveConnector,
    # "remoteok": RemoteOKConnector,  # Will uncomment when implemented
}

engine = create_engine(config.DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@click.group()
def cli():
    """Career Copilot Data Pipeline CLI."""
    pass

def _load_profile(profile_path: str):
    with open(profile_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def _should_preserve_final_status(job: Job) -> bool:
    return bool(job.llm_status == "completed" and job.status not in (None, "new", "applied"))

def _run_fetch(source: str, dry_run: bool):
    if source not in CONNECTORS:
        logger.warning(f"Source '{source}' not fully implemented. Defaulting to 'remotive'.")
        source = 'remotive'

    logger.info(f"Starting fetch for source '{source}' (dry_run={dry_run})")

    session = SessionLocal()
    
    # Create PipelineRun record (only save if not dry-run)
    run = PipelineRun(
        source=source,
        started_at=datetime.datetime.utcnow(),
        status="running",
        jobs_fetched=0,
        jobs_new=0,
        jobs_duplicates=0
    )
    
    if not dry_run:
        session.add(run)
        session.commit()
    
    try:
        connector_class = CONNECTORS[source]
        connector = connector_class()
        raw_jobs = connector.fetch_jobs()
        
        run.jobs_fetched = len(raw_jobs)
        
        for raw_job in raw_jobs:
            try:
                normalized = connector.normalize(raw_job)
                logger.debug(f"Normalized job: '{normalized.get('title')}' at '{normalized.get('company')}'")
                
                if is_duplicate(normalized, session):
                    run.jobs_duplicates += 1
                    continue
                    
                job_record = Job(**normalized)
                if not dry_run:
                    session.add(job_record)
                
                run.jobs_new += 1
                
                # Batch commit to avoid losing all progress if an error occurs late in a large batch
                if not dry_run and run.jobs_new % 20 == 0:
                    session.commit()
                
            except Exception as e:
                session.rollback()
                logger.error(f"Error processing job: {e}")
                
        run.status = "completed"
        run.completed_at = datetime.datetime.utcnow()
        if not dry_run:
            session.commit()
            
        logger.info(f"Pipeline completed: {run.jobs_fetched} fetched, {run.jobs_new} new, {run.jobs_duplicates} duplicates from {source}.")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Pipeline failed: {e}")
        run.status = "failed"
        run.error_message = str(e)
        run.completed_at = datetime.datetime.utcnow()
        if not dry_run:
            session.commit()
            
    finally:
        session.close()

def _run_evaluate(profile: str, dry_run: bool, all_jobs: bool):
    candidate_profile = _load_profile(profile)
    logger.info(f"Starting job evaluation using profile '{profile}' (dry_run={dry_run})")
    session = SessionLocal()
    
    try:
        query = session.query(Job)
        if all_jobs:
            jobs_to_evaluate = query.filter(Job.status != "applied").all()
        else:
            jobs_to_evaluate = query.filter(Job.status == "new").all()
        total_eval = len(jobs_to_evaluate)
        scope = "non-applied" if all_jobs else "new"
        logger.info(f"Found {total_eval} {scope} jobs to evaluate.")
        
        counts = {"shortlisted": 0, "review": 0, "rejected": 0, "applied": 0, "errors": 0}
        final_counts = {"shortlisted": 0, "review": 0, "rejected": 0, "applied": 0, "new": 0}
        
        for idx, job in enumerate(jobs_to_evaluate, 1):
            try:
                # Utilities map off dictionaries; synthesize one from ORM
                job_dict = {c.name: getattr(job, c.name) for c in job.__table__.columns}
                
                # Check application history constraint natively
                if has_already_applied(job_dict, session):
                    job.rule_status = "applied"
                    job.status = "applied"
                    counts["applied"] += 1
                else:
                    scoring_result = score_job(job_dict, candidate_profile)
                    previous_status = job.status
                    preserve_final_status = _should_preserve_final_status(job)

                    job.fit_score = scoring_result.get("fit_score", 0)
                    job.remote_eligibility = scoring_result.get("remote_eligibility")
                    job.rule_status = scoring_result.get("recommended_status", "review")

                    if previous_status in (None, "new"):
                        job.status = job.rule_status
                    elif previous_status == "applied":
                        job.status = "applied"
                    elif not preserve_final_status:
                        job.status = job.rule_status
                    
                    # Keep the resume recommendation in sync with the current active queue.
                    if job.status in ["shortlisted", "review"]:
                        resume_result = select_resume(job_dict, candidate_profile)
                        job.recommended_resume = resume_result.get("resume_name")
                    elif job.rule_status not in ["shortlisted", "review"] and not preserve_final_status:
                        job.recommended_resume = None
                        
                    # Count deterministic outcome separately from final status.
                    if job.rule_status in counts:
                        counts[job.rule_status] += 1
                    else:
                        counts["review"] += 1

                if job.status in final_counts:
                    final_counts[job.status] += 1
                else:
                    final_counts["review"] += 1

                if not dry_run and idx % 20 == 0:
                    session.commit()
                    
            except Exception as e:
                session.rollback()
                logger.error(f"Error evaluating job {job.id}: {e}")
                counts["errors"] += 1
                
        if not dry_run:
            session.commit()
            
        logger.info(
            "Evaluation complete. "
            f"Rule status -> Shortlisted: {counts['shortlisted']}, Review: {counts['review']}, "
            f"Rejected: {counts['rejected']}, Applied: {counts['applied']}, Errors: {counts['errors']}. "
            f"Final status -> Shortlisted: {final_counts['shortlisted']}, Review: {final_counts['review']}, "
            f"Rejected: {final_counts['rejected']}, Applied: {final_counts['applied']}."
        )
        
    except Exception as e:
        session.rollback()
        logger.error(f"Evaluation pipeline failed: {e}")
    finally:
        session.close()

def _run_analyze(profile: str, model: str, target_status: str, limit: int, dry_run: bool):
    candidate_profile = _load_profile(profile)
    limit = max(1, limit)
    logger.info(
        f"Starting LLM analysis using profile '{profile}', model '{model}', "
        f"status='{target_status}' (dry_run={dry_run}, limit={limit})"
    )
    session = SessionLocal()

    try:
        jobs_to_analyze = (
            session.query(Job)
            .filter(Job.status == target_status)
            .order_by(Job.fit_score.desc(), Job.id.asc())
            .limit(limit)
            .all()
        )
        logger.info(f"Found {len(jobs_to_analyze)} jobs to analyze.")

        counts = {"promoted": 0, "kept_review": 0, "rejected": 0, "failed": 0, "unchanged": 0}

        for idx, job in enumerate(jobs_to_analyze, 1):
            try:
                original_status = job.status
                job_dict = {c.name: getattr(job, c.name) for c in job.__table__.columns}
                analysis = analyze_job_with_ollama(job_dict, candidate_profile, model)

                if analysis.get("llm_status") == "failed":
                    job.llm_fit_score = None
                    job.llm_strengths = json.dumps([], ensure_ascii=False)
                    job.fit_explanation = None
                    job.skill_gaps = json.dumps([], ensure_ascii=False)
                    job.recommendation = None
                    job.llm_confidence = None
                    job.llm_status = "failed"
                    counts["failed"] += 1
                    logger.warning(
                        f"LLM analysis failed for '{job.title}' @ '{job.company}': "
                        f"{analysis.get('error', 'unknown error')}"
                    )
                else:
                    job.llm_fit_score = analysis.get("llm_fit_score")
                    job.llm_strengths = json.dumps(analysis.get("llm_strengths", []), ensure_ascii=False)
                    job.fit_explanation = analysis.get("fit_explanation")
                    job.skill_gaps = json.dumps(analysis.get("skill_gaps", []), ensure_ascii=False)
                    job.recommendation = analysis.get("recommendation")
                    job.llm_confidence = analysis.get("llm_confidence")
                    job.llm_status = analysis.get("llm_status")

                    if analysis.get("recommended_resume"):
                        job.recommended_resume = analysis.get("recommended_resume")

                    confidence = analysis.get("llm_confidence") or 0
                    recommendation = analysis.get("recommendation")

                    if recommendation == "shortlist" and confidence >= config.LLM_PROMOTION_CONFIDENCE:
                        job.status = "shortlisted"
                        counts["promoted"] += 1
                    elif recommendation == "reject" and confidence >= config.LLM_PROMOTION_CONFIDENCE:
                        job.status = "rejected"
                        counts["rejected"] += 1
                    elif original_status == "review":
                        job.status = "review"
                        counts["kept_review"] += 1
                    else:
                        job.status = original_status
                        counts["unchanged"] += 1

                    logger.info(
                        f"LLM analyzed '{job.title}' @ '{job.company}' -> "
                        f"recommendation={recommendation}, confidence={confidence}, status={job.status}"
                    )

                if not dry_run and idx % 10 == 0:
                    session.commit()
            except Exception as e:
                session.rollback()
                job.llm_status = "failed"
                counts["failed"] += 1
                logger.warning(f"Unexpected LLM analysis failure for job {job.id}: {e}")

        if not dry_run:
            session.commit()

        logger.info(
            "LLM analysis complete. "
            f"Analyzed {len(jobs_to_analyze)} {target_status} jobs: "
            f"promoted={counts['promoted']}, kept_review={counts['kept_review']}, "
            f"rejected={counts['rejected']}, unchanged={counts['unchanged']}, failed={counts['failed']}"
        )
    except Exception as e:
        session.rollback()
        logger.error(f"LLM analysis pipeline failed: {e}")
    finally:
        session.close()

def _display_jobs_by_status(target_status: str, limit: int):
    session = SessionLocal()
    try:
        jobs = (
            session.query(Job)
            .filter(Job.status == target_status)
            .order_by(Job.fit_score.desc(), Job.id.asc())
            .limit(max(1, limit))
            .all()
        )

        if not jobs:
            click.echo(f"No jobs found with status='{target_status}'.")
            return

        click.echo(f"{target_status.upper()} JOBS")
        click.echo("")

        for idx, job in enumerate(jobs, 1):
            fit_score = job.fit_score if job.fit_score is not None else "-"
            recommendation = job.recommendation or "-"
            confidence = job.llm_confidence if job.llm_confidence is not None else "-"
            resume = job.recommended_resume or "-"
            click.echo(f"{idx}. {job.title} -- {job.company}")
            click.echo(f"Score: {fit_score}")

            if recommendation != "-" and confidence != "-":
                click.echo(f"LLM: {recommendation} (confidence {confidence})")
            elif recommendation != "-":
                click.echo(f"LLM: {recommendation}")
            else:
                click.echo("LLM: -")

            click.echo(f"Resume: {resume}")

            if idx < len(jobs):
                click.echo("")
    finally:
        session.close()

@cli.command()
@click.option('--source', required=True, type=click.Choice(['remotive', 'remoteok', 'all']), help='Job source to fetch from')
@click.option('--dry-run', is_flag=True, help='Run pipeline without inserting jobs into database')
def fetch(source: str, dry_run: bool):
    """Fetch remote jobs from the specified source."""
    _run_fetch(source, dry_run)

@cli.command()
@click.option('--profile', default='profile.yaml', help='Path to candidate profile YAML')
@click.option('--dry-run', is_flag=True, help='Evaluate without saving to DB')
@click.option('--all-jobs', is_flag=True, help='Re-evaluate all non-applied jobs instead of only status=new')
def evaluate(profile: str, dry_run: bool, all_jobs: bool):
    """Evaluate raw jobs against candidate profile and assign scores."""
    try:
        _load_profile(profile)
    except Exception as e:
        logger.error(f"Failed to load profile {profile}: {e}")
        return
    _run_evaluate(profile, dry_run, all_jobs)

@cli.command()
@click.option('--profile', default='profile.yaml', help='Path to candidate profile YAML')
@click.option('--model', default=config.OLLAMA_MODEL, help='Ollama model name')
@click.option('--status', 'target_status', default=config.LLM_STATUS_DEFAULT, type=click.Choice(['review', 'shortlisted', 'rejected']), help='Job status bucket to analyze')
@click.option('--limit', default=config.LLM_MAX_JOBS_PER_RUN, type=int, show_default=True, help='Maximum number of jobs to analyze')
@click.option('--dry-run', is_flag=True, help='Analyze without saving to DB')
def analyze(profile: str, model: str, target_status: str, limit: int, dry_run: bool):
    """Analyze selected jobs with Ollama and conservatively adjust status."""
    try:
        _load_profile(profile)
    except Exception as e:
        logger.error(f"Failed to load profile {profile}: {e}")
        return
    _run_analyze(profile, model, target_status, limit, dry_run)

@cli.command(name='full-run')
@click.option('--source', default='remotive', type=click.Choice(['remotive', 'remoteok', 'all']), show_default=True, help='Job source to fetch from')
@click.option('--profile', default='profile.yaml', help='Path to candidate profile YAML')
@click.option('--model', default=config.OLLAMA_MODEL, help='Ollama model name')
@click.option('--analyze-status', default=config.LLM_STATUS_DEFAULT, type=click.Choice(['review', 'shortlisted', 'rejected']), show_default=True, help='Job status bucket to analyze after evaluation')
@click.option('--analyze-limit', default=config.LLM_MAX_JOBS_PER_RUN, type=int, show_default=True, help='Maximum number of jobs to analyze')
@click.option('--dry-run', is_flag=True, help='Run the full pipeline without saving DB changes')
def full_run(source: str, profile: str, model: str, analyze_status: str, analyze_limit: int, dry_run: bool):
    """Run fetch, evaluate, and analyze in one command."""
    try:
        _load_profile(profile)
    except Exception as e:
        logger.error(f"Failed to load profile {profile}: {e}")
        return

    logger.info(
        f"Starting full pipeline run: source='{source}', profile='{profile}', "
        f"model='{model}', analyze_status='{analyze_status}', analyze_limit={max(1, analyze_limit)}, "
        f"dry_run={dry_run}"
    )
    _run_fetch(source, dry_run)
    _run_evaluate(profile, dry_run, all_jobs=False)
    _run_analyze(profile, model, analyze_status, analyze_limit, dry_run)
    logger.info("Full pipeline run complete.")

@cli.command()
@click.option('--limit', default=20, type=int, show_default=True, help='Maximum number of shortlisted jobs to display')
def shortlist(limit: int):
    """Display shortlisted jobs with rule and LLM summary fields."""
    _display_jobs_by_status("shortlisted", limit)

@cli.command(name='review')
@click.option('--limit', default=20, type=int, show_default=True, help='Maximum number of review jobs to display')
def review_command(limit: int):
    """Display review jobs with rule and LLM summary fields."""
    _display_jobs_by_status("review", limit)

@cli.command()
@click.option('--limit', default=20, type=int, show_default=True, help='Maximum number of rejected jobs to display')
def rejected(limit: int):
    """Display rejected jobs with rule and LLM summary fields."""
    _display_jobs_by_status("rejected", limit)

if __name__ == '__main__':
    cli()
