#!/bin/sh

set -e

host="$1"
shift
# Если host не передан, используем значение из переменной окружения
host="${host:-$POSTGRES_HOST}"
until PGPASSWORD=$POSTGRES_PASSWORD psql -h "$host" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c '\q'; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 1
done

>&2 echo "Postgres is up."
