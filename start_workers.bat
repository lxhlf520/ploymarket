@echo off
REM Polymarket activity workers: edit proxy ports as needed.
REM Each worker uses its own tunnel proxy egress IP (independent rate limit).

cd /d %~dp0

start "pm-worker-w1" cmd /k python worker_activity.py --proxy http://127.0.0.1:7890 --worker-id w1 --jobs 3
timeout /t 2 /nobreak >nul
start "pm-worker-w2" cmd /k python worker_activity.py --proxy http://127.0.0.1:7891 --worker-id w2 --jobs 3
timeout /t 2 /nobreak >nul
start "pm-worker-w3" cmd /k python worker_activity.py --worker-id w3 --jobs 3

echo.
echo Started 3 workers (w1/w2 via proxy, w3 direct). Check queue status:
echo   psql -d polymarket -c "SELECT status, count(*) FROM activity_tasks GROUP BY status;"
pause
