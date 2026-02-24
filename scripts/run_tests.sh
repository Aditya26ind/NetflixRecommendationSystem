#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   scripts/run_tests.sh           # run all tests
#   scripts/run_tests.sh tests/test_api_routes.py  # run a specific file
#   scripts/run_tests.sh tests/test_api_routes.py::test_health_route  # single test

PYTHONPATH=. pytest "$@"
