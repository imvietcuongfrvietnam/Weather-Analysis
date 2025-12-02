@echo off
REM ====================================
REM Set Python 3.11 as default
REM ====================================

echo Current Python versions:
py --list
echo.

echo Setting Python 3.11 as default...
echo.

REM Instructions
echo To set Python 3.11 as default, you need to:
echo 1. Open Windows Settings
echo 2. Go to: Apps ^> Apps ^& features ^> App execution aliases
echo 3. Turn OFF "python.exe" and "python3.exe" aliases
echo 4. Add Python 3.11 to PATH:
echo    - Open System Properties ^> Environment Variables
echo    - Add to PATH: C:\Users\%USERNAME%\AppData\Local\Programs\Python\Python311
echo    - Add to PATH: C:\Users\%USERNAME%\AppData\Local\Programs\Python\Python311\Scripts
echo.

echo OR simply use the run.bat script to run the project!
echo.

pause
