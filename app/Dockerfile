# syntax=docker/dockerfile:1
 
FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential postgresql-client libxml2 libjpeg-dev gettext dos2unix \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Fix line endings for shell scripts
RUN dos2unix /app/run_uwsgi.sh /app/wait-for-postgres.sh

EXPOSE 8000

CMD ["/app/wait-for-postgres.sh", "db", "/app/run_uwsgi.sh"]
