@echo off
chcp 65001 >nul
echo ==========================================
echo   盘前数据预热 (手动执行)
echo ==========================================
echo 预热以下数据, 提速竞价推送:
echo   - 股票列表缓存
echo   - 概念板块映射
echo   - K线缓存 (60天)
echo   - Baostock估值 (PE/PB/PS/PCF)
echo   - Baostock行业分类
echo   - Baostock ROE (Top500)
echo ==========================================
cd /d "%~dp0"
set PYTHON_EXE=
for %%P in ("%LOCALAPPDATA%\Programs\Python\Python311\python.exe" "%LOCALAPPDATA%\Programs\Python\Python312\python.exe") do (
    if exist %%P (set PYTHON_EXE=%%P& goto :run)
)
where python >nul 2>&1
if %errorlevel% equ 0 (set PYTHON_EXE=python& goto :run)
set PYTHON_EXE=py -3
:run
%PYTHON_EXE% 盘前预热.py
pause
