@echo off
cd /d C:\projects\career-copilot
call conda activate career-copilot
python run_pipeline.py full-run --email
