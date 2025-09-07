# syntax=docker/dockerfile:1
# Укажите образ, который будет использоваться для создания контейнера.
# Вы можете подобрать наиболее подходящий для вас: https://hub.docker.com/_/python
FROM python:3.10

# Выберите папку, в которой будут размещаться файлы проекта внутри контейнера. 
# Имейте в виду, что команда WORKDIR создаст папку /opt/app, если её ещё нет,
# и перейдеёт в эту папку, то есть все команды после WORKDIR будут выполнены в 
# папке /opt/app
WORKDIR /opt/app

# Заведите необходимые переменные окружения
ENV DJANGO_SETTINGS_MODULE='config.settings'
ENV PYTHONPATH=/opt/app

# Скопируйте в контейнер файлы, которые редко меняются.
# Рекомендуем использовать скрипт run_uwsgi.sh в качестве точки входа в приложение.
# Там вы можете выполнить необходимые процедуры перед запуском приложения, 
# такие как сбор статики, создание суперпользователя и даже применение миграций
COPY requirements.txt .
COPY uwsgi/uwsgi.ini .
COPY --chmod=755 uwsgi/run_uwsgi.sh .
COPY --chmod=755 wait-for-postgres.sh .

# Установите зависимости, предварительно обновив менеджер пакетов pip
# Устанавливаем утилиты dos2unix (для исправления окончаний строк) и postgresql-client (для скрипта ожидания)
RUN apt-get update && apt-get install -y --no-install-recommends dos2unix postgresql-client \
    && pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    # Конвертируем окончания строк из Windows (CRLF) в Unix (LF)
    && dos2unix /opt/app/run_uwsgi.sh \
    && dos2unix /opt/app/wait-for-postgres.sh \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Скопируйте все оставшиеся файлы. Для ускорения сборки образа эту команду стоит разместить ближе к концу файла. 
# Точка в команде COPY означает текущую папку. Так мы просим docker скопировать все файлы из текущей папки в текущую папку в контейнере (/opt/app) 
COPY . .

# Укажите порт, на котором приложение будет доступно внутри Docker-сети
EXPOSE 8000

# Укажите, как запускать ваш сервис.
# Сначала дожидаемся доступности БД, затем запускаем основной скрипт
ENTRYPOINT ["/opt/app/wait-for-postgres.sh", "db", "/opt/app/run_uwsgi.sh"]