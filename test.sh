#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail

echo "Running pytest..."
coverage run -m pytest tests/
#coverage run -m pytest -m wip tests/

echo
echo "Pytest coverage..."
coverage report -m
