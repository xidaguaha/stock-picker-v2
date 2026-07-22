@echo off
chcp 65001 >nul
title Stock Picker Scheduler v6.10.2 (Watchdog)
echo ========================================
echo   Stock Picker Scheduler v6.10.2
echo   Watchdog mode: auto-restart on crash
echo   Ctrl+C to stop
echo ========================================
cd /d "%~dp0"
set PYTHON_EXE=
REM 优先使用 Python311 (项目依赖安装在此环境)
for %%P in ("%LOCALAPPDATA%\Programs\Python\Python311\python.exe" "%LOCALAPPDATA%\Programs\Python\Python312\python.exe") do (
    if exist %%P (set PYTHON_EXE=%%P& goto :run)
)
where python >nul 2>&1
if %errorlevel% equ 0 (set PYTHON_EXE=python& goto :run)
where py >nul 2>&1
if %errorlevel% equ 0 (set PYTHON_EXE=py -3& goto :run)
echo [ERROR] Python not found
pause
exit /b 1
:run
:loop
echo [Watchdog] Starting scheduler...
%PYTHON_EXE% scheduler.py
if %errorlevel% equ 0 (
    echo.
    echo ========================================
    echo [Watchdog] Scheduler exited normally.
    echo ========================================
    pause
    exit /b 0
)
echo.
echo ========================================
echo [Watchdog] Scheduler crashed (exit code %errorlevel%)
echo   Restarting in 10 seconds...
echo   Press any key to stop now.
echo ========================================
REM 用 choice 替代 timeout，Ctrl+C 和按键都能终止
choice /c YN /n /t 10 /d Y /m "Press N to stop, or wait 10s to restart: "
if errorlevel 2 (
    echo Stopping.
    exit /b 0
)
goto loop
