#!/bin/sh

# Прерывать выполнение скрипта при любой ошибке
set -e

# Компиляция переводов
echo "Compiling translations..."
python manage.py compilemessages --ignore venv

# Запуск сервера uWSGI
echo "Starting server..."
exec uwsgi --strict --ini uwsgi.ini