#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail

flake8
mypy -p jql

coverage run -m pytest
coverage report
