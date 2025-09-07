# syntax=docker/dockerfile:1

# --- Build Stage ---
# Use a full image for building, which includes build tools
FROM python:3.10 as builder

WORKDIR /opt/app

# Install build-time dependencies like dos2unix
RUN apt-get update && apt-get install -y --no-install-recommends dos2unix

COPY requirements.txt .
RUN pip install --upgrade pip \
    && pip install --no-cache-dir --prefix="/install" -r requirements.txt

# Copy scripts and fix line endings
COPY uwsgi/uwsgi.ini .
COPY --chmod=755 uwsgi/run_uwsgi.sh .
COPY --chmod=755 wait-for-postgres.sh .
RUN dos2unix /opt/app/run_uwsgi.sh /opt/app/wait-for-postgres.sh

# Use a slim image for a smaller final image size
FROM python:3.10-slim

WORKDIR /opt/app

# Install only runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends postgresql-client \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local
COPY --from=builder /opt/app/uwsgi.ini .
COPY --from=builder /opt/app/run_uwsgi.sh .
COPY --from=builder /opt/app/wait-for-postgres.sh .

# Copy application code
COPY . .

EXPOSE 8000

ENTRYPOINT ["/opt/app/wait-for-postgres.sh", "db", "/opt/app/run_uwsgi.sh"]