@echo off
chcp 65001 >nul
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
%PYTHON_EXE% run.py
pause
