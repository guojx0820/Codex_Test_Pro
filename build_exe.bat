@echo off
setlocal EnableExtensions
cd /d "%~dp0"

where python >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Python was not found. Please install Python 3.10+ and add it to PATH.
  pause
  exit /b 1
)

python -c "import PyInstaller" >nul 2>nul
if errorlevel 1 (
  echo [ERROR] PyInstaller is not installed in current Python environment.
  echo [INFO] This script will not auto-install dependencies to avoid network/proxy failures.
  echo [INFO] Please run manually in a network-enabled environment:
  echo        python -m pip install pyinstaller
  pause
  exit /b 1
)

python -m PyInstaller --noconfirm --clean --noconsole --onefile --name RSBatchDownloader app.py
if errorlevel 1 (
  echo [ERROR] Build failed.
  pause
  exit /b 1
)

echo [DONE] EXE generated: dist\RSBatchDownloader.exe
pause
