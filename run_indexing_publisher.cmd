@echo off
rem Daily Google Indexing API publisher (scheduled task "WaypointIndexingPublisher").
rem Full python path — Task Scheduler doesn't inherit the interactive PATH.
cd /d C:\Users\rober\hospital-scraper
if not exist logs mkdir logs
"C:\Users\rober\AppData\Local\Programs\Python\Python312\python.exe" indexing_publisher.py >> logs\indexing_publisher.log 2>&1
