#!/bin/sh

set -e

/app/wait-for-postgres.sh db

/app/run_uwsgi.sh