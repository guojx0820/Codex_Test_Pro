@echo off
setlocal EnableExtensions
cd /d "%~dp0"

where python >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Python was not found. Please install Python 3.10+ and add it to PATH.
  echo Press any key to exit...
  pause >nul
  exit /b 1
)

python launcher.py
set "EC=%ERRORLEVEL%"
if not "%EC%"=="0" (
  echo [ERROR] Program exited with code %EC%.
  echo Press any key to exit...
  pause >nul
)
exit /b %EC%
