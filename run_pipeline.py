import sys
import asyncio
import click

# Force UTF-8 output on Windows so Unicode chars (e.g. UTC −08:00) don't crash.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore
import yaml
import datetime
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.database import Base, Job, PipelineRun, ApplicationHistory
from connectors.remotive import RemotiveConnector
from connectors.remoteok import RemoteOKConnector
from connectors.weworkremotely import WeWorkRemotelyConnector
from connectors.arbeitnow import ArbeitnowConnector
from connectors.jobicy import JobicyConnector
from connectors.jobspresso import JobspressoConnector
from connectors.dynamitejobs import DynamiteJobsConnector
from connectors.workingnomads import WorkingNomadsConnector
from connectors.getonboard import GetOnBoardConnector
from connectors.himalayas import HimalayasConnector
from utils.dedup import is_duplicate
from utils.application_filter import has_already_applied
from utils.llm_analysis import analyze_job_with_ollama
from utils.scoring import score_job
from utils.resume_selector import select_resume
from utils.logger import setup_logger
from utils.email_report import send_report
import config

logger = setup_logger("run_pipeline")

CONNECTORS = {
    "remotive": RemotiveConnector,
    "remoteok": RemoteOKConnector,
    "weworkremotely": WeWorkRemotelyConnector,
    "arbeitnow": ArbeitnowConnector,
    "jobicy": JobicyConnector,
    "jobspresso": JobspressoConnector,
    "dynamitejobs": DynamiteJobsConnector,
    "workingnomads": WorkingNomadsConnector,
    "getonboard": GetOnBoardConnector,
    "himalayas": HimalayasConnector,
}

# Job listing domains that block Playwright (bot detection / OAuth walls).
# URLs from these domains are opened in the user's default system browser.
SYSTEM_BROWSER_DOMAINS = {
    "remoteok.com",
    "weworkremotely.com",
    "jobicy.com",
    "getonbrd.com",
    "himalayas.app",
}

# Sources disabled from 'all' by default (subscription required or bot-protected).
# Enable individually with --source <name>.
DISABLED_SOURCES = {
    "remoteok",        # subscription required to apply
    "weworkremotely",  # Cloudflare bot protection blocks automated browser
    "workingnomads",   # jobs go via Proxify — requires profile approval before applying
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
    if source == "all":
        for s in CONNECTORS:
            if s in DISABLED_SOURCES:
                logger.info(f"Skipping '{s}' (disabled — use --source {s} to include).")
                continue
            _run_fetch(s, dry_run)
        return

    if source not in CONNECTORS:
        logger.warning(f"Unknown source '{source}'. Available: {', '.join(CONNECTORS)}.")
        return

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

                posted_date = normalized.get("posted_date")
                if posted_date:
                    cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=config.MAX_JOB_AGE_DAYS)
                    if posted_date.tzinfo is None:
                        posted_date = posted_date.replace(tzinfo=datetime.timezone.utc)
                    if posted_date < cutoff:
                        run.jobs_duplicates += 1
                        continue

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
            
        msg = f"Pipeline completed: {run.jobs_fetched} fetched, {run.jobs_new} new, {run.jobs_duplicates} duplicates from {source}."
        if run.jobs_new:
            logger.warning(msg)
        else:
            logger.info(msg)
        
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
            jobs_to_evaluate = query.filter(Job.status.notin_(["applied", "deferred"])).all()
        else:
            jobs_to_evaluate = query.filter(Job.status == "new").all()
        total_eval = len(jobs_to_evaluate)
        scope = "non-applied/non-deferred" if all_jobs else "new"
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
            .filter(Job.status == target_status, Job.llm_status == None)
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
            click.echo(f"{idx}. [id={job.id}] {job.title} -- {job.company}")
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

@cli.command(name='triage')
def triage():
    """Work through review jobs one by one: shortlist, reject, skip, or open in browser."""
    import webbrowser

    session = SessionLocal()
    seen_ids: set = set()
    try:
        while True:
            cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=10)
            job = (
                session.query(Job)
                .filter(
                    Job.status == 'review',
                    Job.id.notin_(seen_ids),
                    (Job.posted_date >= cutoff) | (Job.posted_date == None),
                )
                .order_by(Job.fit_score.desc(), Job.id.asc())
                .first()
            )
            if not job:
                click.echo("No more review jobs.")
                break

            click.echo("")
            click.echo(f"[id={job.id}] {job.title} -- {job.company}")
            click.echo(f"Score: {job.fit_score if job.fit_score is not None else '-'}  |  Source: {job.source or '-'}")
            if job.recommendation:
                click.echo(f"LLM:   {job.recommendation} (confidence {job.llm_confidence or '-'})")
            if job.fit_explanation:
                click.echo(f"Note:  {job.fit_explanation[:120]}")
            click.echo(f"URL:   {job.url or '-'}")
            click.echo("")
            click.echo("  [s] shortlist   [r] reject   [o] open in browser   [n] next   [q] quit")

            while True:
                choice = click.prompt("Action", default="o").strip().lower()
                if choice in ("s", "r", "o", "n", "q"):
                    break
                click.echo("  Please enter s, r, o, n, or q.")

            quit_flag = False
            if choice == "q":
                break
            elif choice == "s":
                job.status = "shortlisted"
                session.commit()
                click.echo(f"  → Shortlisted: {job.title}")
            elif choice == "r":
                job.status = "rejected"
                session.commit()
                click.echo(f"  → Rejected: {job.title}")
            elif choice == "o":
                if job.url:
                    webbrowser.open(job.url)
                    click.echo("  Opened in browser. Press ENTER to continue.")
                    try:
                        input()
                    except Exception:
                        pass
                    # Ask again after viewing
                    click.echo("  [s] shortlist   [r] reject   [a] applied   [n] next   [q] quit")
                    while True:
                        choice2 = click.prompt("Action", default="n").strip().lower()
                        if choice2 in ("s", "r", "a", "n", "q"):
                            break
                    if choice2 == "s":
                        job.status = "shortlisted"
                        session.commit()
                        click.echo(f"  → Shortlisted: {job.title}")
                    elif choice2 == "r":
                        job.status = "rejected"
                        session.commit()
                        click.echo(f"  → Rejected: {job.title}")
                    elif choice2 == "a":
                        job.status = "applied"
                        session.commit()
                        click.echo(f"  → Applied: {job.title}")
                    elif choice2 == "q":
                        quit_flag = True
                else:
                    click.echo("  No URL available.")

            seen_ids.add(job.id)
            session.expire_all()
            if quit_flag:
                break
    finally:
        session.close()

@cli.command()
@click.option('--source', required=True, type=click.Choice(['remotive', 'remoteok', 'weworkremotely', 'arbeitnow', 'jobicy', 'jobspresso', 'dynamitejobs', 'workingnomads', 'getonboard', 'himalayas', 'all']), help='Job source to fetch from')
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
@click.option('--source', default='all', type=click.Choice(['remotive', 'remoteok', 'weworkremotely', 'arbeitnow', 'jobicy', 'jobspresso', 'dynamitejobs', 'workingnomads', 'getonboard', 'himalayas', 'all']), show_default=True, help='Job source to fetch from')
@click.option('--profile', default='profile.yaml', help='Path to candidate profile YAML')
@click.option('--model', default=config.OLLAMA_MODEL, help='Ollama model name')
@click.option('--analyze-status', default=config.LLM_STATUS_DEFAULT, type=click.Choice(['review', 'shortlisted', 'rejected']), show_default=True, help='Job status bucket to analyze after evaluation')
@click.option('--analyze-limit', default=config.LLM_MAX_JOBS_PER_RUN, type=int, show_default=True, help='Maximum number of jobs to analyze')
@click.option('--dry-run', is_flag=True, help='Run the full pipeline without saving DB changes')
@click.option('--email', is_flag=True, help='Send email report if new shortlisted or review jobs were found')
def full_run(source: str, profile: str, model: str, analyze_status: str, analyze_limit: int, dry_run: bool, email: bool):
    """Run fetch, evaluate, and analyze in one command."""
    from dotenv import load_dotenv
    load_dotenv()

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

    # Snapshot counts before run to detect new additions
    session = SessionLocal()
    try:
        from sqlalchemy import func
        before = dict(session.query(Job.status, func.count()).group_by(Job.status).all())
    finally:
        session.close()

    _run_fetch(source, dry_run)
    _run_evaluate(profile, dry_run, all_jobs=False)
    _run_analyze(profile, model, analyze_status, analyze_limit, dry_run)
    logger.info("Full pipeline run complete.")

    if not dry_run:
        click.echo("")
        for status in ("shortlisted", "review"):
            _display_jobs_by_status(status, limit=20)
        _print_stats()

    if email and not dry_run:
        session = SessionLocal()
        try:
            from sqlalchemy import func
            after = dict(session.query(Job.status, func.count()).group_by(Job.status).all())
            new_shortlisted = after.get("shortlisted", 0) - before.get("shortlisted", 0)
            new_review = after.get("review", 0) - before.get("review", 0)

            if new_shortlisted > 0 or new_review > 0:
                # Collect the newly promoted/added jobs for the email
                new_jobs = []
                if new_shortlisted > 0:
                    jobs = (session.query(Job)
                            .filter(Job.status == "shortlisted")
                            .order_by(Job.id.desc())
                            .limit(new_shortlisted).all())
                    new_jobs += [{"title": j.title, "company": j.company,
                                  "fit_score": j.fit_score, "status": "shortlisted",
                                  "source": j.source} for j in jobs]
                if new_review > 0:
                    jobs = (session.query(Job)
                            .filter(Job.status == "review", Job.llm_status == None)
                            .order_by(Job.id.desc())
                            .limit(new_review).all())
                    new_jobs += [{"title": j.title, "company": j.company,
                                  "fit_score": j.fit_score, "status": "review",
                                  "source": j.source} for j in jobs]
                send_report(new_jobs, after)
            else:
                logger.info("No new shortlisted or review jobs — skipping email report.")
        finally:
            session.close()

@cli.command()
@click.option('--profile', default='profile.yaml', show_default=True, help='Path to candidate profile YAML')
@click.option('--status', default='review', type=click.Choice(['review', 'new', 'shortlisted']), show_default=True, help='Job status bucket to rescore')
def rescore(profile: str, status: str):
    """Re-run rule-based scoring on existing jobs and reject those that no longer qualify."""
    candidate_profile = _load_profile(profile)
    session = SessionLocal()
    try:
        jobs = session.query(Job).filter(Job.status == status).all()
        rejected = 0
        for job in jobs:
            job_dict = {c.name: getattr(job, c.name) for c in job.__table__.columns}
            result = score_job(job_dict, candidate_profile)
            if result["recommended_status"] == "rejected":
                job.status = "rejected"
                job.fit_score = result["fit_score"]
                rejected += 1
        session.commit()
        click.echo(f"Rescored {len(jobs)} '{status}' jobs: {rejected} newly rejected, {len(jobs) - rejected} kept.")
    finally:
        session.close()

@cli.command(name='send-test-email')
def send_test_email():
    """Send a test email to verify credentials and SMTP settings."""
    from dotenv import load_dotenv
    load_dotenv()
    dummy_jobs = [
        {"title": "Test: Senior ML Engineer", "company": "Acme Corp", "fit_score": 75, "status": "shortlisted", "source": "remotive"},
        {"title": "Test: Backend Engineer", "company": "Startup Inc", "fit_score": 52, "status": "review", "source": "arbeitnow"},
    ]
    dummy_counts = {"shortlisted": 3, "review": 12, "applied": 1, "deferred": 0, "rejected": 84}
    ok = send_report(dummy_jobs, dummy_counts)
    if ok:
        click.echo(click.style("Test email sent successfully.", fg="green"))
    else:
        click.echo(click.style("Failed to send test email — check logs above.", fg="red"))

@cli.command(name='setup-credentials')
def setup_credentials():
    """Store email credentials securely in Windows Credential Manager."""
    import keyring
    service = "career-copilot"
    click.echo("Storing email credentials in Windows Credential Manager (never written to disk).")
    click.echo("")
    email_from = click.prompt("Sender email address")
    email_to = click.prompt("Recipient email address", default=email_from)
    password = click.prompt("Gmail App Password", hide_input=True)
    keyring.set_password(service, "EMAIL_FROM", email_from)
    keyring.set_password(service, "EMAIL_TO", email_to)
    keyring.set_password(service, "EMAIL_PASSWORD", password)
    click.echo("")
    click.echo(click.style("Credentials saved to Windows Credential Manager.", fg="green"))
    click.echo("You can now remove EMAIL_FROM, EMAIL_TO, and EMAIL_PASSWORD from your .env file.")

@cli.command(name='help')
def help_command():
    """Show a summary of all available commands."""
    lines = [
        ("", "DAILY WORKFLOW", ""),
        ("full-run", "Fetch, evaluate and LLM-analyze new jobs", "--email  --source <src>  --dry-run"),
        ("triage", "Work through review jobs one by one (shortlist / reject / open)", ""),
        ("stats", "Show job counts by status + command tips", ""),
        ("", "", ""),
        ("", "BROWSING JOBS", ""),
        ("shortlist", "List shortlisted jobs", "--limit N"),
        ("review", "List review jobs", "--limit N"),
        ("rejected", "List rejected jobs", "--limit N"),
        ("deferred", "List deferred jobs", "--limit N"),
        ("open-job", "Open a job in browser and apply", "--status shortlisted|review  --job-id N"),
        ("", "", ""),
        ("", "PIPELINE TOOLS", ""),
        ("fetch", "Fetch only (no scoring/LLM). Use --source all for all sources", "--source <src>  --dry-run"),
        ("evaluate", "Score new jobs against your profile", "--profile  --dry-run"),
        ("analyze", "Run LLM analysis on review jobs", "--limit N  --model <model>  --dry-run"),
        ("rescore", "Re-apply scoring rules to existing review jobs", "--status review|new"),
        ("", "", ""),
        ("", "SETUP & EMAIL", ""),
        ("setup-credentials", "Store email credentials in Windows Credential Manager", ""),
        ("send-test-email", "Send a test email to verify SMTP setup", ""),
        ("", "", ""),
        ("", "SOURCES", ""),
        ("", "remotive  arbeitnow  jobicy  jobspresso  dynamitejobs", ""),
        ("", "workingnomads  getonboard  himalayas  (all = all enabled sources)", ""),
        ("", "remoteok  weworkremotely  (disabled by default)", ""),
    ]

    HIGHLIGHT = {"full-run", "open-job"}

    click.echo("")
    click.echo(click.style("  Career Copilot — Command Reference", fg="cyan", bold=True))
    click.echo("")
    for cmd, desc, opts in lines:
        if not cmd and not desc and not opts:
            click.echo("")
        elif not cmd:
            click.echo(click.style(f"  {desc}", fg="yellow", bold=True))
        elif cmd in HIGHLIGHT:
            cmd_str = click.style(f"  {cmd:<22}", fg="magenta", bold=True)
            opts_str = click.style(f"  {opts}", fg="white") if opts else ""
            click.echo(f"{cmd_str}{desc}{opts_str}")
        else:
            cmd_str = click.style(f"  {cmd:<22}", fg="green")
            opts_str = click.style(f"  {opts}", fg="white") if opts else ""
            click.echo(f"{cmd_str}{desc}{opts_str}")
    click.echo("")

def _print_stats():
    session = SessionLocal()
    try:
        from sqlalchemy import func
        counts = dict(session.query(Job.status, func.count()).group_by(Job.status).all())
        order = ["shortlisted", "review", "deferred", "applied", "rejected", "new"]
        click.echo("")
        for status in order:
            n = counts.get(status, 0)
            if status in ("shortlisted", "review") and n > 0:
                click.echo(click.style(f"  {status:<12} {n}", fg="yellow", bold=True))
            else:
                click.echo(f"  {status:<12} {n}")
        extras = set(counts) - set(order)
        for status in sorted(extras):
            click.echo(f"  {status:<12} {counts[status]}")
        click.echo("")
        if counts.get("review", 0) > 0:
            click.echo(click.style("  Tip: run 'triage' to work through review jobs one by one.", fg="cyan"))
        click.echo(click.style("  Tip: run 'full-run' to fetch and evaluate new jobs.", fg="cyan"))
    finally:
        session.close()

@cli.command()
def stats():
    """Show a quick count of jobs by status."""
    _print_stats()

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

@cli.command()
@click.option('--limit', default=20, type=int, show_default=True, help='Maximum number of deferred jobs to display')
def deferred(limit: int):
    """Display deferred jobs (unsupported ATS, pending better support)."""
    _display_jobs_by_status("deferred", limit)

@cli.command(name='open-job')
@click.option('--job-id', type=int, default=None, help='Specific job ID to open (any status)')
@click.option('--status', 'queue_status', default='shortlisted', type=click.Choice(['shortlisted', 'review']), show_default=True, help='Queue to work through when no --job-id given')
@click.option('--profile', default='profile.yaml', help='Path to candidate profile YAML')
@click.option('--headless', is_flag=True, default=False, help='Run headless (default: headful)')
@click.option('--fill/--no-fill', default=True, help='Auto-fill detected form fields from profile (default: on)')
@click.option('--dry-run', is_flag=True, default=False, help='Show what would be filled without touching the form')
def open_job(job_id, queue_status, profile, headless, fill, dry_run):
    """Open a shortlisted (or review) job in the browser, inspect the form, and record the outcome."""
    from playwright.async_api import async_playwright
    from utils.form_inspector import try_click_apply, scan_fields, format_field_report, extract_apply_url
    from utils.form_filler import fill_form, format_fill_report, try_upload_resume
    from utils.ats_detector import detect_ats, MANUAL_ONLY_ATS

    try:
        candidate_profile = _load_profile(profile)
    except Exception as e:
        logger.error(f"Failed to load profile {profile}: {e}")
        return

    session = SessionLocal()
    seen_ids: set[int] = set()
    try:
        while True:
            if job_id is not None:
                job = session.query(Job).filter(Job.id == job_id).first()
                if not job:
                    click.echo(f"No job with id={job_id} found.")
                    return
            else:
                job = (
                    session.query(Job)
                    .filter(
                        Job.status == queue_status,
                        Job.url.isnot(None),
                        Job.url != '',
                        Job.id.notin_(seen_ids),
                    )
                    .order_by(Job.fit_score.desc(), Job.id.asc())
                    .first()
                )
                if not job:
                    click.echo(f"No more {queue_status} jobs. All done!")
                    return

            click.echo("")
            click.echo(f"JOB:     {job.title}")
            click.echo(f"Company: {job.company}")
            click.echo(f"Score:   {job.fit_score if job.fit_score is not None else '-'}")
            if job.recommendation:
                click.echo(f"LLM:     {job.recommendation} (confidence {job.llm_confidence or '-'})")
            click.echo(f"Resume:  {job.recommended_resume or '-'}")
            click.echo(f"ATS:     {job.ats_type or 'unknown'}")
            click.echo(f"URL:     {job.url}")
            click.echo("")

            if not click.confirm("Open this job?", default=True):
                seen_ids.add(int(job.id))
                session.expire_all()
                continue

            job_dict = {c.name: getattr(job, c.name) for c in job.__table__.columns}
            target_url = job.url
            session_result = {"outcome": "done"}  # mutable sentinel shared with async closure

            # Some listing sites block Playwright (bot detection / OAuth walls).
            # Open these in the user's default browser where their session is live.
            _url_lower = (target_url or "").lower()
            if any(domain in _url_lower for domain in SYSTEM_BROWSER_DOMAINS):
                import webbrowser
                source_label = job.source or "listing site"
                click.echo(f"{source_label} — opening in your default browser (bot protection blocks automated browser).")
                webbrowser.open(target_url)
                click.echo("Press ENTER when you are done applying.")
                try:
                    input()
                except Exception:
                    pass
                # Fall through to "Did you apply?" below.
                seen_ids.add(job.id)
                click.echo("")
                if click.confirm("Did you apply?", default=False):
                    job.status = "applied"
                    existing = (
                        session.query(ApplicationHistory)
                        .filter_by(company=job.company, job_title=job.title)
                        .first()
                    )
                    if not existing:
                        history = ApplicationHistory(
                            company=job.company,
                            job_title=job.title,
                            applied_date=datetime.date.today(),
                            source=job.source or "manual",
                        )
                        session.add(history)
                    session.commit()
                    click.echo(f"Marked as applied: {job.title} @ {job.company}")
                else:
                    click.echo("No changes recorded.")
                if job_id is not None:
                    return
                continue

            async def _browser_session():
                async with async_playwright() as pw:
                    browser = await pw.chromium.launch(headless=headless)
                    page = await browser.new_page()

                    try:
                        await page.goto(target_url, wait_until="load", timeout=30000)
                    except Exception as e:
                        click.echo(f"Page load error: {e}")
                        await browser.close()
                        return

                    # On listing aggregators (Remotive etc.) try to resolve the
                    # direct employer apply URL before doing anything else.
                    resolved = await extract_apply_url(page)
                    if resolved and resolved != target_url:
                        click.echo(f"Resolved apply URL: {resolved}")
                        try:
                            await page.goto(resolved, wait_until="load", timeout=30000)
                            await page.wait_for_timeout(2000)
                        except Exception as e:
                            click.echo(f"Could not navigate to resolved URL: {e}")

                    active_page = page
                    clicked, active_page = await try_click_apply(active_page)
                    if clicked:
                        click.echo(f"Apply button clicked. Now at: {active_page.url}")
                        await active_page.wait_for_timeout(2000)
                    else:
                        click.echo("No Apply button detected — scanning current page.")

                    ats = detect_ats(active_page.url)
                    if ats in MANUAL_ONLY_ATS:
                        click.echo(f"\nATS '{ats}' — auto-fill not supported for this platform.")
                        if not click.confirm("Fill manually in the browser?", default=True):
                            click.echo("Skipping — browser will close.")
                            session_result["outcome"] = "skipped"
                            await browser.close()
                            return

                    fields = await scan_fields(active_page)
                    if fields:
                        click.echo(f"\nForm fields detected ({len(fields)}, * = required):")
                        click.echo(format_field_report(fields))
                    else:
                        click.echo("\nNo form fields detected yet (may need manual navigation).")

                    if fill and fields and ats not in MANUAL_ONLY_ATS:
                        mode = "DRY RUN — " if dry_run else ""
                        click.echo(f"\n{mode}Filling form from profile...")
                        actions = await fill_form(active_page, fields, candidate_profile, job_dict, dry_run=dry_run)
                        click.echo(format_fill_report(actions))
                        filled = sum(1 for a in actions if a["action"] in ("filled", "checked", "selected"))
                        skipped = sum(1 for a in actions if a["action"] == "skipped")
                        errors = sum(1 for a in actions if a["action"] == "error")
                        click.echo(f"\n  filled={filled}  skipped={skipped}  errors={errors}")

                    if fill and ats not in MANUAL_ONLY_ATS:
                        resume_status = await try_upload_resume(active_page, candidate_profile, job_dict, dry_run=dry_run)
                        click.echo(f"  resume: {resume_status}")

                    click.echo("")
                    click.echo("Browser is open. Press ENTER here when you are done.")
                    try:
                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(None, input)
                    except Exception:
                        pass

                    try:
                        await browser.close()
                    except Exception:
                        pass

            try:
                asyncio.run(_browser_session())
            except Exception as e:
                click.echo(f"Browser session ended: {e}")

            if session_result["outcome"] == "skipped":
                seen_ids.add(job.id)
                session.expire_all()
                continue

            click.echo("")
            if click.confirm("Did you apply?", default=False):
                job.status = "applied"
                existing = (
                    session.query(ApplicationHistory)
                    .filter_by(company=job.company, job_title=job.title)
                    .first()
                )
                if not existing:
                    history = ApplicationHistory(
                        company=job.company,
                        job_title=job.title,
                        applied_date=datetime.date.today(),
                        source=job.source or "manual",
                    )
                    session.add(history)
                session.commit()
                click.echo(f"Marked as applied: {job.title} @ {job.company}")
            else:
                click.echo("No changes recorded.")

            # After each job (applied or not), stop if a specific id was requested.
            if job_id is not None:
                return

            seen_ids.add(job.id)
            session.expire_all()

    finally:
        session.close()


if __name__ == '__main__':
    cli()
