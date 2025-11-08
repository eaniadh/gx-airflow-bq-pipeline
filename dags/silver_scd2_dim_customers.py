# dags/silver_scd2_dim_customers.py
from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from jinja2 import Template
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import bigquery
import uuid, os

try:
    from openlineage.client.facet import SchemaDatasetFacet, SchemaField
    from openlineage.client import OpenLineageClient
    from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
    _OL = True
except Exception:
    print("[OL] openlineage not available.")
    _OL = False





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
    SILVER_FQN = f"{PROJECT_ID}.{DATASET_SILVER}.{SILVER_TABLE}"
    BRONZE_FQN = f"{PROJECT_ID}.{DATASET_BRONZE}.{BRONZE_TABLE}"
    
    COLUMN_MAP = {
        "customer_id": [("bigquery", BRONZE_FQN, "customer_id")],
        "region":      [("bigquery", BRONZE_FQN, "region")],
        "eff_from":    [("bigquery", BRONZE_FQN, "order_timestamp")],
        "eff_to":      [("bigquery", BRONZE_FQN, "order_timestamp")],
        "is_current":  [("bigquery", BRONZE_FQN, "order_timestamp")],
        "hash_diff":   [
            ("bigquery", BRONZE_FQN, "customer_id"),
            ("bigquery", BRONZE_FQN, "region"),
        ],
    }

    TRANSFORM = {
        "eff_from":   "CAST(order_timestamp AS DATE)",
        "eff_to":     "LEAST(CURRENT_DATE(), CAST(order_timestamp AS DATE))",
        "is_current": "CASE WHEN eff_to IS NULL OR eff_to >= CURRENT_DATE() THEN TRUE ELSE FALSE END",
        "hash_diff":  "TO_HEX(SHA256(CONCAT(CAST(customer_id AS STRING),'|',region)))",
    }
    
    def emit_silver_column_lineage():
        if _OL:
            ns  = os.getenv("OPENLINEAGE_NAMESPACE", "gx-bq-pipeline")
            url = os.getenv("OPENLINEAGE_URL", "http://marquez:5000")
            client = OpenLineageClient(url)
            producer = "app://silver_scd2"
            run_id = str(uuid.uuid4())
            now = datetime.utcnow().isoformat() + "Z"

            fields = {}
            for silver_col, sources in COLUMN_MAP.items():
                inputs = [{"namespace": ns_, "name": name_, "field": field_} for (ns_, name_, field_) in sources]
                entry = {"inputFields": inputs}
                if silver_col in TRANSFORM:
                    entry["transform"] = TRANSFORM[silver_col]
                fields[silver_col] = entry

            facet = {
                "_producer": producer,
                "_schemaURL": "https://openlineage.io/spec/facets/2-0-0/ColumnLineageDatasetFacet",
                "fields": fields,
            }

            client.emit(RunEvent(
                eventType=RunState.COMPLETE,
                eventTime=datetime.utcnow().isoformat() + "Z",
                run=Run(runId=run_id),
                job=Job(namespace=ns, name="silver_scd2_dim_customers.merge_multistmt"),
                inputs=[{"namespace": "bigquery", "name": BRONZE_FQN}],
                outputs=[{
                    "namespace": "bigquery",
                    "name": SILVER_FQN,
                    "facets": {"columnLineage": facet}
                }],
                producer=producer,
            ))
            print("[OL] Emitted Silver column lineage facet.")

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
        
    def emit_manual_lineage(**context):
        ns = os.getenv("OPENLINEAGE_NAMESPACE", "gx-bq-pipeline")
        ol_url = os.getenv("OPENLINEAGE_URL", "http://marquez:5000")
        client = OpenLineageClient(ol_url)
        producer = "app://silver_scd2"  # any stable producer id

        run_id = str(uuid.uuid4())
        dag_id  = context["dag"].dag_id
        task_id = "scd2_merge"
        job_name = f"{dag_id}.{task_id}"
        job = Job(namespace=ns, name=job_name)

        bronze_fqn = BRONZE_FQN
        silver_fqn = SILVER_FQN

        # -------- START event (optional, no facets) ----------
        client.emit(RunEvent(
            eventType=RunState.START,
            eventTime=datetime.utcnow().isoformat() + "Z",
            run=Run(runId=run_id),
            job=job,
            inputs=[Dataset(namespace="bigquery", name=bronze_fqn)],
            outputs=[Dataset(namespace="bigquery", name=silver_fqn)],
            producer=producer
        ))

        # -------- Build Silver schema facet from BigQuery ----
        bq = bigquery.Client(project=PROJECT_ID)
        silver_tbl = bq.get_table(silver_fqn)
        silver_fields = [
            SchemaField(name=f.name, type=f.field_type, description=f.description or None)
            for f in silver_tbl.schema
        ]
        silver_schema = SchemaDatasetFacet(fields=silver_fields)

        # -------- Build Column Lineage facet -----------------
        col_fields = {}
        for out_col, sources in COLUMN_MAP.items():
            inputs = [{"namespace": ns_, "name": name_, "field": field_} for (ns_, name_, field_) in sources]
            entry = {"inputFields": inputs}
            if out_col in TRANSFORM:
                entry["transformationDescription"] = TRANSFORM[out_col]
                entry["transformationType"] = "SQL"
            else:
                entry["transformationDescription"] = f"Direct mapping from {', '.join(s[2] for s in sources)}"
                entry["transformationType"] = "IDENTITY"
            
            col_fields[out_col] = entry

        column_lineage_facet = {
            "_producer": "app://column-lineage",
            "_schemaURL": "https://openlineage.io/spec/facets/2-0-0/ColumnLineageDatasetFacet",
            "fields": col_fields,
        }

        # Compute final state from upstream BigQuery task
        upstream_ti = context["ti"].get_dagrun().get_task_instance("scd2_merge")
        final_state = RunState.COMPLETE if getattr(upstream_ti, "state", None) == "success" else RunState.FAIL

        # -------- COMPLETE/FAIL event with BOTH facets -------
        client.emit(RunEvent(
            eventType=final_state,
            eventTime=datetime.utcnow().isoformat() + "Z",
            run=Run(runId=run_id),
            job=job,
            inputs=[Dataset(namespace="bigquery", name=bronze_fqn)],
            outputs=[Dataset(
                namespace="bigquery",
                name=silver_fqn,
                facets={"schema": silver_schema, "columnLineage": column_lineage_facet}
            )],
            producer=producer
        ))
        print(f"[OL] Emitted COMPLETE with schema + columnLineage for {silver_fqn}")



    render = PythonOperator(
        task_id="render_sql",
        python_callable=render_sql,
    )

    echo_sql = PythonOperator(
        task_id="echo_sql",
        python_callable=lambda ti: print(ti.xcom_pull(task_ids="render_sql")),
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
    
    # emit_cols = PythonOperator(
    #     task_id="emit_column_lineage",
    #     python_callable=emit_silver_column_lineage,
    # )
    
    emit_lineage = PythonOperator(
        task_id="emit_manual_lineage",
        python_callable=emit_manual_lineage,
        provide_context=True,
        trigger_rule="all_done",  # always emit an end event even if merge failed
    )

render >> echo_sql >> scd2_merge >> emit_lineage
