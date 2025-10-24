#!/bin/sh

set -e
# Ждем доступности PostgreSQL
/app/wait-for-postgres.sh db