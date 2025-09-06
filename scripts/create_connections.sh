#!/bin/bash

# Load environment variables from ../.env
ENV_FILE="$(dirname "$0")/../.env"

if [ ! -f "$ENV_FILE" ]; then
    echo "Error: .env file not found at $ENV_FILE"
    exit 1
fi

set -o allexport
source "$ENV_FILE"
set +o allexport

echo "Waiting for Airflow DB to be ready..."
airflow db upgrade

# Create internal (metrics) DB connection
METRICS_URI="postgresql+psycopg2://${METRICS_PG_USER}:${METRICS_PG_PASSWORD}@${METRICS_PG_HOST}:${METRICS_PG_PORT}/${METRICS_PG_DB}"

if ! airflow connections get 'postgres_default' &>/dev/null; then
    echo "Creating Postgres default connection..."
    airflow connections add 'postgres_default' --conn-uri "$METRICS_URI"
else
    echo "Postgres default connection already exists"
fi

if ! airflow connections get "$METRICS_PG_CONN_ID" &>/dev/null; then
    echo "Creating metrics Postgres connection..."
    airflow connections add "$METRICS_PG_CONN_ID" --conn-uri "$METRICS_URI"
else
    echo "Metrics Postgres connection already exists"
fi

if ! airflow connections get 'redis_default' &>/dev/null; then
    echo "Creating Redis connection..."
    airflow connections add 'redis_default' \
        --conn-uri 'redis://redis:6379'
else
    echo "Redis connection already exists"
fi

# Create external (target) DB connection
if ! airflow connections get "$TARGET_PG_CONN_ID" &>/dev/null; then
    echo "Creating target Postgres connection..."
    TARGET_URI="postgresql+psycopg2://${TARGET_PG_USER}:${TARGET_PG_PASSWORD}@${TARGET_PG_HOST}:${TARGET_PG_PORT}/${TARGET_PG_DB}"
    airflow connections add "$TARGET_PG_CONN_ID" --conn-uri "$TARGET_URI"
else
    echo "Target Postgres connection already exists"
fi

echo "Airflow connections ensured!"
echo "Setting Airflow Variables..."

airflow variables set target_pg_conn_id "${TARGET_PG_CONN_ID:-target_pg_conn}"
airflow variables set metrics_pg_conn_id "${METRICS_PG_CONN_ID:-metrics_pg_conn}"

echo "Airflow Variables set!"
