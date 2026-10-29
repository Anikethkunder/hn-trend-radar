# HN Trend Radar

An automated data engineering pipeline: reads Hacker News every hour, validates it
(data contract + quality gate), stores 7 days of history, builds SQL analytics, and
publishes a live website showing trends, rising stories, and the pipeline's own health.

extract -> transform (silver) -> quality gate -> load (history) -> gold (SQL) -> website

## Setup
1. Create a NEW PUBLIC GitHub repo named `hn-trend-radar` (leave it empty).
2. In a terminal inside THIS folder (it must contain pipeline.py and index.html):
       git init
       git add .
       git commit -m "Initial commit"
       git branch -M main
       git remote add origin https://github.com/<username>/hn-trend-radar.git
       git push -u origin main
3. Settings -> Actions -> General -> Workflow permissions -> Read and write -> Save.
4. Settings -> Pages -> Deploy from a branch -> main, / (root) -> Save.
5. Actions -> "HN Trend Radar" -> Run workflow (run it 2-3 times, a few minutes apart, to see momentum).
6. Open https://<username>.github.io/hn-trend-radar/

## Local test (offline fake data)
    PowerShell:  $env:MOCK="1"; python pipeline.py
    Tests:       python -m unittest discover -s tests -v

## Demo failures (Run workflow form)
- inject_fault = extract        -> source outage, extract fails
- inject_fault = schema_drift   -> data contract catches a missing field
- inject_fault = bad_data       -> quality gate stops the load
- hold_seconds = 60             -> watch the run "In progress"

## Change the schedule
Edit config.json (cron is UTC; IST = UTC + 5:30), run `python set_schedule.py`, commit and push.
