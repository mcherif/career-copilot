from models.database import Job
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
engine = create_engine("sqlite:///career_copilot.db", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

DISABLED_SOURCES = {"remoteok", "weworkremotely", "workingnomads"}
US_ONLY_LOCATIONS = {"usa", "united states", "us"}

session = SessionLocal()
try:
    disabled_jobs = session.query(Job).filter(
        Job.status == "review",
        Job.source.in_(DISABLED_SOURCES)
    ).all()

    us_jobs = session.query(Job).filter(
        Job.status == "review",
        Job.raw_location_text.in_(["USA", "United States", "US", "usa", "united states", "us"])
    ).all()

    to_delete = {j.id: j for j in disabled_jobs + us_jobs}.values()
    to_delete = list(to_delete)

    print(f"Found {len(to_delete)} review jobs to purge:")
    for j in to_delete:
        print(f"  [{j.source}] loc={j.raw_location_text!r}  {j.title[:60]}")

    if to_delete:
        confirm = input(f"\nDelete all {len(to_delete)} jobs? [y/N] ").strip().lower()
        if confirm == "y":
            for j in to_delete:
                session.delete(j)
            session.commit()
            print("Deleted.")
        else:
            print("Aborted.")
finally:
    session.close()
