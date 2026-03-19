import click
import yaml
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.database import Base, Job, PipelineRun
from connectors.remotive import RemotiveConnector
from utils.dedup import is_duplicate
from utils.application_filter import has_already_applied
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

@cli.command()
@click.option('--source', required=True, type=click.Choice(['remotive', 'remoteok', 'all']), help='Job source to fetch from')
@click.option('--dry-run', is_flag=True, help='Run pipeline without inserting jobs into database')
def fetch(source: str, dry_run: bool):
    """Fetch remote jobs from the specified source."""
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

@cli.command()
@click.option('--profile', default='profile.yaml', help='Path to candidate profile YAML')
@click.option('--dry-run', is_flag=True, help='Evaluate without saving to DB')
@click.option('--all-jobs', is_flag=True, help='Re-evaluate all non-applied jobs instead of only status=new')
def evaluate(profile: str, dry_run: bool, all_jobs: bool):
    """Evaluate raw jobs against candidate profile and assign scores."""
    try:
        with open(profile, 'r', encoding='utf-8') as f:
            candidate_profile = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load profile {profile}: {e}")
        return

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
        
        for idx, job in enumerate(jobs_to_evaluate, 1):
            try:
                # Utilities map off dictionaries; synthesize one from ORM
                job_dict = {c.name: getattr(job, c.name) for c in job.__table__.columns}
                
                # Check application history constraint natively
                if has_already_applied(job_dict, session):
                    job.status = "applied"
                    counts["applied"] += 1
                else:
                    scoring_result = score_job(job_dict, candidate_profile)
                    
                    job.fit_score = scoring_result.get("fit_score", 0)
                    job.remote_eligibility = scoring_result.get("remote_eligibility")
                    job.status = scoring_result.get("recommended_status", "review")
                    
                    # Only select specialized resume if it survives rejection
                    if job.status in ["shortlisted", "review"]:
                        resume_result = select_resume(job_dict, candidate_profile)
                        job.recommended_resume = resume_result.get("resume_name")
                        
                    # Safely mark status in counting dictionary
                    if job.status in counts:
                        counts[job.status] += 1
                    else:
                        # Fallback for unexpected statuses
                        counts["review"] += 1
                        
                if not dry_run and idx % 20 == 0:
                    session.commit()
                    
            except Exception as e:
                session.rollback()
                logger.error(f"Error evaluating job {job.id}: {e}")
                counts["errors"] += 1
                
        if not dry_run:
            session.commit()
            
        logger.info(f"Evaluation complete. Shortlisted: {counts['shortlisted']}, Review: {counts['review']}, Rejected: {counts['rejected']}, Applied: {counts['applied']}, Errors: {counts['errors']}")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Evaluation pipeline failed: {e}")
    finally:
        session.close()

if __name__ == '__main__':
    cli()
