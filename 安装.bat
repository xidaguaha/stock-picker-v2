@echo off
chcp 65001 >nul
title Stock Picker v6.10.2 - Installer
echo ========================================
echo   Stock Picker v6.10.2 - Installer
echo ========================================
echo.

set PYTHON_EXE=
REM 优先使用 Python311 (项目依赖安装在此环境)
for %%P in ("%LOCALAPPDATA%\Programs\Python\Python311\python.exe" "%LOCALAPPDATA%\Programs\Python\Python312\python.exe") do (
    if exist %%P (set PYTHON_EXE=%%P& goto :check)
)
where python >nul 2>&1
if %errorlevel% equ 0 (set PYTHON_EXE=python& goto :check)
where py >nul 2>&1
if %errorlevel% equ 0 (set PYTHON_EXE=py -3& goto :check)
echo [ERROR] Python not found. Install Python 3.10+
echo https://www.python.org/downloads/
pause
exit /b 1

:check
echo [1/3] Python: %PYTHON_EXE%

echo.
echo [2/3] Installing dependencies...
%PYTHON_EXE% -m pip install -r "%~dp0requirements.txt" -q
if %errorlevel% neq 0 (
    %PYTHON_EXE% -m pip install -r "%~dp0requirements.txt" -i https://mirrors.aliyun.com/pypi/simple/ -q
)
echo   Done.

echo.
echo [3/3] Setting up auto-start...
set STARTUP=%APPDATA%\Microsoft\Windows\Start Menu\Programs\Startup
set LINK=%STARTUP%\StockSchedulerV2.bat
(
    echo @echo off
    echo cd /d "%~dp0"
    echo call "%%~dp0启动调度器.bat"
) > "%LINK%"
echo   Added: %LINK%

echo.
echo ========================================
echo   Install complete!
echo ========================================
echo   Start scheduler: double-click 启动调度器.bat
echo   Remove auto-start: delete %LINK%
echo.
pause
