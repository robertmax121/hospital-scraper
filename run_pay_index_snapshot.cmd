@echo off
rem Monthly Pay Index snapshot (scheduled task "WaypointPayIndexSnapshot", 1st @ 6:45 AM).
cd /d C:\Users\rober\hospital-scraper
if not exist logs mkdir logs
"C:\Users\rober\AppData\Local\Programs\Python\Python312\python.exe" pay_index_snapshot.py >> logs\pay_index.log 2>&1
