@echo off
REM Polymarket activity workers: mihomo instances + workers with auto node rotation.
REM Each worker owns a dedicated mihomo instance (independent egress IP / rate limit)
REM and auto-switches nodes on 429/403 via clash_pool.AsyncNodeRotator.
REM Prereq: python make_worker_clash.py --n 3   (generates clash_worker\*.yaml)

cd /d %~dp0

REM 1) start mihomo instances (w1: mixed 7901 / controller 9101, ...)
call clash_worker\start_mihomo.bat
timeout /t 3 /nobreak >nul

REM 2) start workers, each bound to its own instance
start "pm-worker-w1" cmd /k python worker_activity.py --proxy http://127.0.0.1:7901 --clash-base http://127.0.0.1:9101 --worker-id w1 --jobs 3
timeout /t 2 /nobreak >nul
start "pm-worker-w2" cmd /k python worker_activity.py --proxy http://127.0.0.1:7902 --clash-base http://127.0.0.1:9102 --worker-id w2 --jobs 3
timeout /t 2 /nobreak >nul
start "pm-worker-w3" cmd /k python worker_activity.py --proxy http://127.0.0.1:7903 --clash-base http://127.0.0.1:9103 --worker-id w3 --jobs 3

echo.
echo Started 3 mihomo instances + 3 workers (auto node rotation on 429/403).
echo Stop mihomo instances: clash_worker\stop_mihomo.bat
echo Check queue status:
echo   psql -d polymarket -c "SELECT status, count(*) FROM activity_tasks GROUP BY status;"
pause
