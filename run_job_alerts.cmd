@echo off
rem Weekly job-alert digest trigger (scheduled task "WaypointJobAlerts", Mondays 8 AM).
rem Vercel's plan allows only two crons, so the weekly send is triggered from
rem here instead: curls the authenticated endpoint with CRON_SECRET read from
rem the site's env file. The route itself does all the work.
cd /d C:\Users\rober\hospital-scraper
if not exist logs mkdir logs
"C:\Users\rober\AppData\Local\Programs\Python\Python312\python.exe" -c "import urllib.request,re;secret=[l.split('=',1)[1].strip() for l in open(r'C:\Users\rober\waypoint-jobs\.env.local',encoding='utf-8') if l.startswith('CRON_SECRET=')][0];req=urllib.request.Request('https://jobs.waypointrecruit.com/api/cron/job-alerts',headers={'Authorization':'Bearer '+secret});print(urllib.request.urlopen(req,timeout=120).read().decode())" >> logs\job_alerts.log 2>&1
