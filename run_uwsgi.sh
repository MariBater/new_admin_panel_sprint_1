#!/bin/sh

# Прерывать выполнение скрипта при любой ошибке
set -e

# Применение миграций базы данных
echo "Applying database migrations..."
python manage.py migrate --noinput

# Сбор статических файлов
echo "Collecting static files..."
python manage.py collectstatic --noinput

# Запуск сервера uWSGI
echo "Starting server..."
exec uwsgi --strict --ini uwsgi.ini