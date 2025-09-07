import os
import sys
import time

import psycopg

retries = 15
delay = 2

print("Waiting for PostgreSQL...")

for i in range(retries):
    try:
        dsn = (
            f"dbname={os.getenv('POSTGRES_DB')} "
            f"user={os.getenv('POSTGRES_USER')} "
            f"password={os.getenv('POSTGRES_PASSWORD')} "
            f"host={os.getenv('POSTGRES_HOST')} "
            f"port={os.getenv('POSTGRES_PORT')}"
        )
        conn = psycopg.connect(dsn)
        conn.close()
        print("PostgreSQL started.")
        sys.exit(0)
    except psycopg.OperationalError:
        print(f"PostgreSQL is unavailable, waiting {delay} seconds... ({i + 1}/{retries})")
        time.sleep(delay)

print("Could not connect to PostgreSQL. Exiting.")
sys.exit(1)
