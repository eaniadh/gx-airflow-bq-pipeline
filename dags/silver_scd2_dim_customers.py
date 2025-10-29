# dags/silver_scd2_dim_customers.py
from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from jinja2 import Template
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import os



with DAG(
    dag_id="silver_scd2_dim_customers",
    schedule=None,                    # triggered by Bronze
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"retries": 0, "retry_delay": timedelta(minutes=3)},
    # Default params (used if dag_run.conf is missing)
    params={},
    render_template_as_native_obj=True,
    tags=["silver", "scd2", "bq"],
) as dag:
    
    SQL_DIR         = os.path.join(os.path.dirname(__file__), "sql")
    PROJECT_ID      = Variable.get("project_id")
    DATASET_BRONZE  = Variable.get("dataset_bronze")
    BRONZE_TABLE    = Variable.get("bronze_table")
    DATASET_SILVER  = Variable.get("dataset_silver")
    SILVER_TABLE    = Variable.get("silver_table")

    def render_sql(**context):
    # allow override via dag_run.conf.process_date, else use Airflow's ds
        dr = context.get("dag_run")
        if dr and getattr(dr, "conf", None):
            process_date = dr.conf.get("process_date") or context["ds"]
        else:
            process_date = context["ds"]

        tmpl_path = f"{dag.folder}/sql/silver_scd2_merge.sql.j2"
        with open(tmpl_path) as f:
            t = Template(f.read())
        return t.render(
            project_id=PROJECT_ID,
            dataset_bronze=DATASET_BRONZE,
            bronze_table=BRONZE_TABLE,
            dataset_silver=DATASET_SILVER,
            silver_table=SILVER_TABLE,
            process_date=process_date,
        )

    render = PythonOperator(
        task_id="render_sql",
        python_callable=render_sql,
    )

    scd2_merge = BigQueryInsertJobOperator(
        task_id="scd2_merge",
        configuration={
            "query": {
                "query": "{{ ti.xcom_pull(task_ids='render_sql') }}",
                "useLegacySql": False,
            }
        },
        location="US",
        gcp_conn_id="google_cloud_default",
    )

    render >> scd2_merge