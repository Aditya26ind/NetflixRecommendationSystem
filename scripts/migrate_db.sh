#!/usr/bin/env bash
set -euo pipefail
psql "postgresql://${DB_USER:-postgres}:${DB_PASSWORD:-password}@${DB_HOST:-localhost}:${DB_PORT:-5432}/${DB_NAME:-movie_recommendations}" -f sql/create_tables.sql
