#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail

echo "Running flake8..."
flake8 --extend-exclude 'vendor'

echo
echo "Running mypy..."
mypy --follow-imports skip -p jql

echo
echo "Running pytest..."
coverage run --omit 'vendor/*' -m pytest tests/

echo
echo "Pytest coverage..."
coverage report -m
