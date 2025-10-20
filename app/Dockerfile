# syntax=docker/dockerfile:1

# --- Build Stage ---
# Use a full image for building, which includes build tools
FROM python:3.10 as builder

WORKDIR /app

# Install build-time dependencies like dos2unix
RUN apt-get update && apt-get install -y --no-install-recommends dos2unix

COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --no-cache-dir --prefix="/install" -r requirements.txt

# Copy scripts and fix line endings
COPY --chmod=755 run_uwsgi.sh .
COPY --chmod=755 wait-for-postgres.sh .
RUN dos2unix /app/run_uwsgi.sh /app/wait-for-postgres.sh

# Use a slim image for a smaller final image size
FROM python:3.10-slim

WORKDIR /app

# Install only runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-client libxml2 libjpeg-dev gettext \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local
COPY --from=builder /app/run_uwsgi.sh .
COPY --from=builder /app/wait-for-postgres.sh .

# Copy application code
COPY . .

EXPOSE 8000

CMD ["/app/wait-for-postgres.sh", "db", "/app/run_uwsgi.sh"]