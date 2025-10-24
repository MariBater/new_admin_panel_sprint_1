# syntax=docker/dockerfile:1
 
FROM python:3.10-slim

# Устанавливаем переменные окружения, чтобы Python не создавал .pyc файлы и не буферизировал вывод
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential postgresql-client libxml2 libjpeg-dev gettext dos2unix libpcre2-dev gosu \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
# Копируем только requirements.txt, чтобы кэшировать этот слой
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Создаем пользователя и директории, устанавливаем права - все в одном слое
RUN groupadd -r appuser && useradd -r -g appuser appuser \
    && mkdir -p /var/run/uwsgi /app/logs /app/static /app/staticfiles /app/media /app/state \
    && chown -R appuser:appuser /var/run/uwsgi /app/logs /app/staticfiles /app/media /app/state \
    && chmod +x /app/docker-entrypoint.sh \
    && dos2unix /app/*.sh

EXPOSE 8000

ENTRYPOINT ["/app/docker-entrypoint.sh"]

CMD ["/app/run_uwsgi.sh"] # Default command for web service, will be executed by entrypoint
