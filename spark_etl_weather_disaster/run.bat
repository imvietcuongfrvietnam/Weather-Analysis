@echo off
REM ====================================
REM Quick run script for ETL pipeline
REM ====================================

echo Starting Weather-Disaster ETL Pipeline...
echo.

REM Set UTF-8 encoding
set PYTHONIOENCODING=utf-8

REM Run with Python 3.11 (compatible with PySpark 4.0)
py -3.11 main_etl.py

echo.
echo Pipeline completed!
pause
