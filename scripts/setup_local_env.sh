#!/usr/bin/env bash
set -euo pipefail
# Create python venv and install requirements
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
