#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail

echo "Running flake8..."
flake8

echo
echo "Running mypy..."
mypy -p jql

echo
echo "Running pytest..."
coverage run -m pytest tests/

echo
echo "Pytest coverage..."
coverage report -m
