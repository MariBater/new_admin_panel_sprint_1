#!/bin/sh

# Прерывать выполнение скрипта при любой ошибке
set -e

# Создание схемы БД, если она не существует
echo "Creating database schema if it doesn't exist..."
python manage.py shell -c "from django.db import connection; cursor = connection.cursor(); cursor.execute('CREATE SCHEMA IF NOT EXISTS content;')"

# Применение миграций базы данных
echo "Applying database migrations..."
python manage.py migrate

# Сбор статических файлов
echo "Collecting static files..."
python manage.py collectstatic --noinput

# Запуск сервера uWSGI
echo "Starting server..."
exec uwsgi --strict --ini uwsgi.ini