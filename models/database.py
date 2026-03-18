import datetime
from sqlalchemy import Column, Integer, String, Text, DateTime, Date, UniqueConstraint
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True)
    external_id = Column(String, unique=True)
    source = Column(String)
    company = Column(String)
    title = Column(String)
    location = Column(String)
    raw_location_text = Column(String)
    description = Column(Text)
    url = Column(String, unique=True)
    remote_eligibility = Column(String, nullable=True)
    ats_type = Column(String, nullable=True)
    posted_date = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.datetime.utcnow, onupdate=datetime.datetime.utcnow)
    status = Column(String, default="new")

class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id = Column(Integer, primary_key=True)
    source = Column(String)
    started_at = Column(DateTime)
    completed_at = Column(DateTime, nullable=True)
    jobs_fetched = Column(Integer)
    jobs_new = Column(Integer)
    jobs_duplicates = Column(Integer)
    status = Column(String)
    error_message = Column(Text, nullable=True)

class ApplicationHistory(Base):
    __tablename__ = "application_history"

    id = Column(Integer, primary_key=True)
    company = Column(String)
    job_title = Column(String)
    applied_date = Column(Date)
    source = Column(String, default="manual_import")
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("company", "job_title", name="_company_job_uc"),
    )
