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
  python -m venv .venv
)

call .venv\Scripts\activate.bat
python -m pip install --upgrade pip
python -m pip install pyinstaller
pyinstaller --noconfirm --clean --noconsole --onefile --name RSBatchDownloader app.py

echo [完成] exe 已生成至 dist\RSBatchDownloader.exe
pause
