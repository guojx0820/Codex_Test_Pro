@echo off
chcp 65001 >nul
setlocal

cd /d %~dp0

where python >nul 2>nul
if errorlevel 1 (
  echo [错误] 未检测到 Python，请先安装 Python 3.10+。
  pause
  exit /b 1
)

if not exist .venv (
  echo [信息] 正在创建虚拟环境...
  python -m venv .venv
)

call .venv\Scripts\activate.bat
python app.py

if errorlevel 1 (
  echo [错误] 程序运行失败，请检查环境。
  pause
)
