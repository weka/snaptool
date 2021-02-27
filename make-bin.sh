#!/usr/bin/env bash

cython -3 snaptool.py --embed
gcc -Os -I /usr/include/python3.6m -o snaptool.bin snaptool.c -lpython3.6m -lpthread -lm -lutil -ldl
