#!/bin/bash
echo Creating venv...
python3.11 -m venv venv
source venv/bin/activate
echo Installing requirements...
pip3 install -r requirements.txt
echo Done.
