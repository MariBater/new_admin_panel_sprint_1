#!/bin/sh

set -e

# Wait for the database to be ready.
/app/wait-for-postgres.sh db

# Run web-specific setup only if the command is for uWSGI.
# This prevents running migrate/collectstatic for the ETL service.
if [ "$1" = "uwsgi" ] || ( [ "$1" = "/app/run_uwsgi.sh" ] ); then
    echo "Running web-specific setup: migrations and collectstatic..."
    python manage.py migrate --noinput
    python manage.py collectstatic --noinput
fi

# Debugging: Print PostgreSQL Environment Variables (Shell)
echo "--- Debugging PostgreSQL Environment Variables (Shell) ---"
env | grep POSTGRES_ || true
echo "----------------------------------------------------------"

exec "$@"