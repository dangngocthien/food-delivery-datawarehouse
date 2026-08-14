"""
food_delivery_pipeline_dag.py
Thanh vien D - Orchestration
DAG dieu phoi pipeline: task_ingest (A) -> task_spark_batch (B) -> task_load_warehouse (C)
                         -> task_dbt_run (C) -> task_refresh_dashboard (D)

=== CAN XAC NHAN VOI NHOM (TODO) ===
- Ten script batch ingest cua A: PhanCong_DoAn18 chi dat ten "producer.py" cho streaming,
  CHUA co ten file cho script sinh du lieu batch (raw/). Dang tam dat la
  `ingestion_A/generate_batch_data.py` - DOI LAI dung ten A dung, bao trong group chat.
- FACT_TABLE / profiles dbt: xem warehouse_C/dbt_project/profiles.yml va ml/train_eta_model.py.

=== Ten container/service (lay tu docker-compose.yml that cua nhom) ===
- kafka          (A - khong co container "ingestion" rieng)
- spark-master   (B)
- postgres-warehouse (C, db=food_delivery_dw, user=warehouse)
- airflow-webserver / airflow-scheduler (D)
- metabase       (D)

=== Vi sao cac task chay truc tiep trong container Airflow (khong docker exec) ===
docker-compose.yml hien KHONG mount docker.sock, nen khong the `docker exec` sang
container khac. Thay vao do:
  - task_ingest, task_load_warehouse, task_dbt_run: chay truc tiep trong container
    Airflow, vi da co san boto3/minio/kafka-python/psycopg2-binary/dbt-postgres qua
    _PIP_ADDITIONAL_REQUIREMENTS, va thu muc project duoc mount tai /opt/airflow/project.
  - task_spark_batch: dung `spark-submit` nop job QUA MANG toi spark://spark-master:7077.
    CAN THEM "pyspark" vao _PIP_ADDITIONAL_REQUIREMENTS trong docker-compose.yml
    (o khoi x-airflow-common -> environment) de container Airflow co san lenh spark-submit.
    Vi du dong hien tai:
      _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:-boto3 minio kafka-python psycopg2-binary dbt-postgres}
    Doi thanh:
      _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:-boto3 minio kafka-python psycopg2-binary dbt-postgres pyspark==3.5.1}
    (chon version pyspark khop voi version Spark trong spark_B/Dockerfile.spark)
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
        bash_command=f"python {PROJECT_ROOT}/ingestion_A/simulate_batch.py",
    )

    task_spark_batch = BashOperator(
        task_id="task_spark_batch",
        bash_command=(
            "spark-submit --master spark://spark-master:7077 "
            f"{PROJECT_ROOT}/spark_B/batch_processing.py"
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

    task_ingest >> task_spark_batch >> task_load_warehouse >> task_dbt_run >> task_refresh_dashboard
