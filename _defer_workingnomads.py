"""
One-off script: mark all non-applied WorkingNomads jobs as deferred.
They go via Proxify which requires profile approval — revisit once advanced.
"""
from models.database import Job
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = create_engine("sqlite:///career_copilot.db", connect_args={"check_same_thread": False})
Session = sessionmaker(bind=engine)
session = Session()

try:
    jobs = session.query(Job).filter(
        Job.source == "workingnomads",
        Job.status.notin_(["applied", "deferred"])
    ).all()
    print(f"Found {len(jobs)} WorkingNomads jobs to defer:")
    for j in jobs:
        print(f"  [{j.status}] {j.title[:60]} -- {j.company}")
    if jobs:
        confirm = input(f"\nDefer all {len(jobs)} jobs? [y/N] ").strip().lower()
        if confirm == "y":
            for j in jobs:
                j.status = "deferred"
            session.commit()
            print("Done.")
        else:
            print("Aborted.")
finally:
    session.close()
