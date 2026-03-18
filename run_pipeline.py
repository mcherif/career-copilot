import click
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.database import Base, Job, PipelineRun
from connectors.remotive import RemotiveConnector
from utils.dedup import is_duplicate
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

if __name__ == '__main__':
    cli()
