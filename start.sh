#!/bin/bash
set -e
source ~/.secrets/db.env
exec docker compose up "$@"
