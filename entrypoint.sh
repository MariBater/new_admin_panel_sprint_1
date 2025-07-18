#!/bin/sh

# Ожидание доступности PostgreSQL (опционально, но рекомендуется для надежности)
echo "Waiting for postgres..."
while ! nc -z postgres 5432; do
    sleep 0.1
done
echo "PostgreSQL started"

echo "Applying database migrations..."
python3 manage.py migrate --no-input

echo "Starting uWSGI server..."
exec uwsgi --ini uwsgi.ini
