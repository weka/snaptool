#!/bin/bash
echo Creating venv...
pybin=`which python3.11`
if [ -z "$pybin" ]; then
   echo python3.11 not found, trying python3.9
   pybin=`which python3.9`
fi
if [ -z "$pybin" ]; then
   echo no appropriate version of python3 found in PATH
   echo please install python3.9 or python3.11
   exit
fi
$pybin -m venv venv
source venv/bin/activate
echo Installing requirements...
pip3 install -r requirements.txt
echo Done.
