#!/usr/bin/env bash

echo "Begin Building DUCKDB as Python Library"
BUILD_PYTHON=1 GEN=ninja make
cd tools/pythonpkg
python3.9 -m pip install --force-reinstall .
cd ../..
echo "Build successful"
