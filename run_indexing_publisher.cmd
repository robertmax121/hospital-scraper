@echo off
rem Daily search-engine URL publishers (scheduled task "WaypointIndexingPublisher").
rem Google Indexing API (200/day) + Bing URL Submission API (adaptive quota).
rem Full python path — Task Scheduler doesn't inherit the interactive PATH.
cd /d C:\Users\rober\hospital-scraper
if not exist logs mkdir logs
"C:\Users\rober\AppData\Local\Programs\Python\Python312\python.exe" indexing_publisher.py >> logs\indexing_publisher.log 2>&1
"C:\Users\rober\AppData\Local\Programs\Python\Python312\python.exe" bing_submitter.py >> logs\bing_submitter.log 2>&1
