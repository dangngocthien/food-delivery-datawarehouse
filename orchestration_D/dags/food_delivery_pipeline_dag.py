"""
food_delivery_pipeline_dag.py
Thanh vien D - Orchestration
DAG dieu phoi pipeline: task_ingest (A) -> task_spark_batch (B) -> task_load_warehouse (C)
                         -> task_dbt_run (C) -> task_refresh_dashboard (D)

"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = "/opt/airflow/project"

default_args = {
    "owner": "D-orchestration",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="food_delivery_pipeline_dag",
    default_args=default_args,
    description="Pipeline Do an 18: ingest -> spark batch -> load warehouse -> dbt -> refresh dashboard",
    schedule_interval="@daily",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["doan18", "giao_do_an"],
) as dag:

    task_ingest = BashOperator(
        task_id="task_ingest",
        bash_command=(
            f"python {PROJECT_ROOT}/ingestion_A/simulate_batch.py "
            f"--input {PROJECT_ROOT}/ingestion_A/data/food_delivery_raw.csv"
        ),
    )

    task_spark_batch = BashOperator(
        task_id="task_spark_batch",
        bash_command=(
            "spark-submit --master spark://spark-master:7077 "
            f"{PROJECT_ROOT}/spark_B/batch_processing.py"
        ),
    )

    task_create_tables = BashOperator(
        task_id="task_create_tables",
        bash_command=(
            "python -c \""
            "import os, psycopg2; "
            "conn = psycopg2.connect("
            "host=os.getenv('PG_HOST', 'postgres-warehouse'), "
            "port=os.getenv('PG_PORT', '5432'), "
            "dbname=os.getenv('PG_DB', 'food_delivery_dw'), "
            "user=os.getenv('PG_USER', 'warehouse'), "
            "password=os.getenv('PG_PASSWORD', 'warehouse123')); "
            "conn.autocommit = True; "
            "cur = conn.cursor(); "
            f"cur.execute(open('{PROJECT_ROOT}/warehouse_C/create_tables.sql').read()); "
            "cur.close(); conn.close(); "
            "print('Da tao lai schema warehouse OK')"
            "\""
        ),
    )

    task_load_warehouse = BashOperator(
        task_id="task_load_warehouse",
        bash_command=f"python {PROJECT_ROOT}/warehouse_C/load_to_postgres.py",
    )

    task_dbt_run = BashOperator(
        task_id="task_dbt_run",
        bash_command=(
            f"cd {PROJECT_ROOT}/warehouse_C/dbt_project && "
            "dbt run --profiles-dir . && dbt test --profiles-dir ."
        ),
    )

    task_refresh_dashboard = BashOperator(
        task_id="task_refresh_dashboard",
        bash_command=(
            'echo "Pipeline hoan tat. Mo Metabase tai http://localhost:3000 de xem dashboard."'
        ),
    )

task_ingest >> task_spark_batch >> task_create_tables >> task_load_warehouse >> task_dbt_run >> task_refresh_dashboard
