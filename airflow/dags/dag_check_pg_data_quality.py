from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.models.param import Param
from airflow.models import Variable
import pandas as pd
import json

TARGET_CONN_ID = Variable.get("target_pg_conn_id")
METRICS_CONN_ID = Variable.get("metrics_pg_conn_id")

default_params = {
    "schema_name": Param('public', type='string', description='The schema to monitor'),
    "monitor_tables": Param('all', type='string', description='Comma-separated tables to monitor, "all" means all tables'),
}

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def ensure_metrics_table(**context):
    create_sql = """
    CREATE TABLE IF NOT EXISTS monitored_table_quality (
        id SERIAL PRIMARY KEY,
        schema_name TEXT NOT NULL,
        table_name TEXT NOT NULL,
        duplicate_count BIGINT,
        null_columns JSONB,
        outlier_columns JSONB,
        checked_at TIMESTAMPTZ DEFAULT now()
    );
    """
    metrics_hook = PostgresHook(postgres_conn_id=METRICS_CONN_ID)
    metrics_hook.run(create_sql)


def log_data_quality(**context):
    schema = context["params"].get("schema_name", "public")
    tables_param = context["params"].get("monitor_tables", "all").strip()

    target_hook = PostgresHook(postgres_conn_id=TARGET_CONN_ID)
    metrics_hook = PostgresHook(postgres_conn_id=METRICS_CONN_ID)

    if tables_param.lower() == "all":
        records = target_hook.get_records("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE';
        """, parameters=[schema])
        tables = [row[0] for row in records]
    else:
        tables = [t.strip() for t in tables_param.split(",") if t.strip()]

    print(f"Monitoring schema: {schema}, tables = {tables}")

    timestamp_now = target_hook.get_first("SELECT NOW()")[0]

    for table in tables:
        query = f"SELECT * FROM {schema}.{table}"
        df = pd.read_sql(query, target_hook.get_conn())

        # Duplicate count
        dup_count = int(df.duplicated().sum())

        # Null columns
        # null_cols = df.isnull().sum()
        null_columns = {col: int(count) for col, count in df.isnull().sum().items() if count > 0}

        # Outlier detection (numeric columns, z-score method)
        numeric_df = df.select_dtypes(include=['int64', 'float64'])
        outlier_info = {}
        if not numeric_df.empty:
            z_scores = (numeric_df - numeric_df.mean()) / numeric_df.std(ddof=0)
            outliers = (abs(z_scores) > 3).sum()
            outlier_info = {col: int(count) for col, count in outliers.items() if count > 0}

        # Insert
        metrics_hook.run(
            """
            INSERT INTO monitored_table_quality 
            (schema_name, table_name, duplicate_count, null_columns, outlier_columns, checked_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            parameters=(schema, table, dup_count, json.dumps(null_columns), json.dumps(outlier_info), timestamp_now)
        )

with DAG(
    dag_id="dag_check_pg_data_quality",
    default_args=default_args,
    params=default_params,
    schedule_interval="0 6 * * *",  # Every Monday at 6 AM
    start_date=days_ago(1),
    catchup=False,
    tags=["data_quality", "postgres"],
) as dag:

    ensure_table = PythonOperator(
        task_id="ensure_metrics_table",
        python_callable=ensure_metrics_table,
    )

    log_quality = PythonOperator(
        task_id="log_data_quality",
        python_callable=log_data_quality,
    )

    ensure_table >> log_quality
