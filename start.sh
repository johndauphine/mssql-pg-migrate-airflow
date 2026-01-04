#!/bin/bash
set -e
set -a  # Auto-export all variables
source ~/.secrets/db.env
set +a
exec docker compose up "$@"
