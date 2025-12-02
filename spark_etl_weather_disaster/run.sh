#!/bin/bash
####################################
# Quick run script for ETL pipeline
####################################

echo "Starting Weather-Disaster ETL Pipeline..."
echo ""

# Set UTF-8 encoding
export PYTHONIOENCODING=utf-8

# Run with Python 3.11 (compatible with PySpark 4.0)
py -3.11 main_etl.py

echo ""
echo "Pipeline completed!"
