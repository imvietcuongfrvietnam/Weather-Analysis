#!/bin/bash
####################################
# Generate test data ONCE
####################################

echo ""
echo "===================================="
echo "   DATA GENERATOR"
echo "===================================="
echo ""
echo "This will generate fake test data and save to ./data/ directory"
echo "You only need to run this ONCE (or when you want fresh data)"
echo ""

export PYTHONIOENCODING=utf-8
py -3.11 generate_data.py

echo ""
echo "===================================="
echo "   DONE!"
echo "===================================="
echo ""
echo "Now you can run: ./run.sh"
echo ""
