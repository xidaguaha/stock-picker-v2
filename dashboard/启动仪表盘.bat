@echo off
chcp 65001 >nul 2>&1
title 量化选股系统 - Web 仪表盘
cd /d "%~dp0"

echo ============================================
echo   量化选股系统 - Web 仪表盘启动器
echo ============================================
echo.

REM 查找 Python
set PYTHON_FOUND=

for %%P in (
    "%LOCALAPPDATA%\Programs\Python\Python311\python.exe"
    "%LOCALAPPDATA%\Programs\Python\Python312\python.exe"
    "python"
    "python3"
) do (
    where %%P >nul 2>&1
    if not errorlevel 1 (
        set PYTHON_FOUND=%%P
        goto :FOUND_PYTHON
    )
)

:FOUND_PYTHON
if "%PYTHON_FOUND%"=="" (
    echo [ERROR] 未找到 Python，请确保已安装 Python 3.10+
    pause
    exit /b 1
)

echo [INFO] 使用 Python: %PYTHON_FOUND%
echo [INFO] 正在启动仪表盘...
echo [INFO] 浏览器将自动打开 http://127.0.0.1:5555
echo.
echo 按 Ctrl+C 可停止服务
echo --------------------------------------------

%PYTHON_FOUND% start.py

if errorlevel 1 (
    echo.
    echo [ERROR] 仪表盘启动失败，请检查错误信息
    pause
)

pause >nul
