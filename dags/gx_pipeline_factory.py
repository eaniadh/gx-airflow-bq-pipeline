"""
gx_pipeline_factory.py
----------------------
Dynamic DAG generator for Great Expectations → Publish → Consumer pipelines.

Creates for each dataset (from configs/pipelines.yml):
  1️⃣ gx_pipeline__<dataset>   : GX validation + publish
  2️⃣ gx_consumer__<dataset>   : Reads _MANIFEST.json & triggers downstream DAG

The consumer DAG is auto-triggered after GX publish success.
"""

import os, yaml, subprocess
from datetime import timedelta
import pendulum
from datetime import datetime as _dt
import uuid as _uuid
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.email import EmailOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException
from airflow.models.param import Param
from airflow.models import Variable
from google.cloud import bigquery, storage

# open lineage import --------------------------
try:
    from openlineage.client import OpenLineageClient
    from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
    _OL = True
except Exception:
    _OL = False

os.environ.setdefault("OPENLINEAGE_URL", "http://marquez:5000")
os.environ.setdefault("OPENLINEAGE_NAMESPACE", "gx-bq-pipeline")
# ---------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------
def _get_client(client_type):
    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "/opt/airflow/keys/service-account.json")
    if client_type == "bq":
        return bigquery.Client.from_service_account_json(creds)
    return storage.Client.from_service_account_json(creds)

def precheck_resources(bq_table, buckets):
    bq = _get_client("bq")
    st = _get_client("st")
    bq.get_table(bq_table)
    for b in buckets:
        st.get_bucket(b)
    print(f"✅ Precheck OK for {bq_table} and buckets {buckets}")

def wait_for_input_ready(
    bucket_name,
    prefix,                         # e.g. "date={{ ds }}"
    min_files=1,
    required_suffix=".parquet",
    accept_success_marker=True,
    max_list=50,
    max_input_age_minutes=180,
    enforce_today_only=True,
    timezone="UTC",
    ds=None,                        # templated via op_kwargs
):
    """
    Landing precheck:
      - (Optional) Only run if ds == today() in given timezone.
      - Data is 'ready' if:
          a) _SUCCESS present (if accept_success_marker)
         OR
          b) count(files with required_suffix) >= min_files
      - Additionally require freshness: newest matching blob updated <= max_input_age_minutes.
      - If not ready: SKIP (AirflowSkipException), not fail.
    """
    tz = pendulum.timezone(timezone)
    now = pendulum.now(tz)

    if enforce_today_only:
        if ds is None:
            raise ValueError("ds (execution date) not provided to wait_for_input_ready")
        if ds != now.format("YYYY-MM-DD"):
            raise AirflowSkipException(f"Skipping: ds={ds} != today={now.strftime('%Y-%m-%d')} in tz={timezone}")

    st = _get_client("st")
    blobs = list(st.list_blobs(bucket_name, prefix=prefix))
    names = [b.name for b in blobs]

    print(f"Scanned {len(names)} object(s) under gs://{bucket_name}/{prefix}")
    for name in names[:max_list]:
        print(f"  • {name}")
    if len(names) > max_list:
        print(f"  …(+{len(names) - max_list} more)")

    # Accept landing _SUCCESS if present
    if accept_success_marker and any(n.endswith("_SUCCESS") for n in names):
        print("✅ Found _SUCCESS in landing (treating as ready).")
        return True

    # Filter to required suffix files
    file_blobs = [b for b in blobs if b.name.lower().endswith(required_suffix)]
    count = len(file_blobs)
    print(f"Found {count} file(s) ending with {required_suffix}")

    if count < min_files:
        raise AirflowSkipException(
            f"No input ready in gs://{bucket_name}/{prefix} "
            f"(need >= {min_files} {required_suffix} file(s))"
        )

    # Freshness guard: newest file must be within max_input_age_minutes
    newest = max((b.updated for b in file_blobs), default=None)
    if not newest:
        raise AirflowSkipException("No timestamped files found to evaluate freshness")

    newest_dt = pendulum.instance(newest).in_timezone(tz)
    age_min = (now - newest_dt).total_seconds() / 60.0
    print(f"Newest {required_suffix} mtime: {newest_dt.to_iso8601_string()} (age ~{age_min:.1f} min)")

    if age_min > max_input_age_minutes:
        raise AirflowSkipException(
            f"Input too old: newest file age {age_min:.1f} min > {max_input_age_minutes} min "
            f"(tz={timezone})"
        )

    print("✅ Landing has fresh input files. Proceeding.")
    return True

def gx_validate(config, execution_date, **context):
    import os, shlex, subprocess, sys
    from google.cloud import storage

    date = execution_date.strftime("%Y-%m-%d")
    success_all = True
    good_exported = False

    dag_run = context.get("dag_run")
    export_good = (dag_run.conf.get("export_good_on_failure")
                   if dag_run and dag_run.conf
                   else context["params"].get("export_good_on_failure", True))

    def run_and_stream(cmd: str) -> int:
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"        # child prints immediately
        env["TQDM_DISABLE"] = "0"            # show progress bars
        env["GE_LOG_LEVEL"] = "INFO"         # GX logging verbosity
        # NEW: stitch OpenLineage facet to the Airflow task's run
        env["AIRFLOW_JOB_NAME"] = f"{context['ti'].dag_id}.{context['ti'].task_id}" if 'ti' in context else "gx_validate"
        env["AIRFLOW_RUN_ID"]   = context['run_id'] if 'run_id' in context else ""
        # NEW: ensure OL vars reach child
        env["OPENLINEAGE_URL"] = os.getenv("OPENLINEAGE_URL", "http://marquez:5000")
        env["OPENLINEAGE_NAMESPACE"] = os.getenv("OPENLINEAGE_NAMESPACE", "gx-bq-pipeline")
        # NB: DO NOT pass sys.stdout/stderr here; use PIPE and re-print
        proc = subprocess.Popen(
            shlex.split(cmd),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,  # line-buffered,
            env=env,
        )
        # stream to Airflow log line-by-line
        for line in proc.stdout:
            print(line, end="", flush=True)
        return proc.wait()

    for mode in ["basic", "cross"]:
        if not config.get(f"suite_{mode}"):
            print(f"⚠️ No suite_{mode} defined, skipping...")
            continue

        cmd = (
            f"python -u /opt/airflow/scripts/gx_validate.py "
            f"--date {date} --mode {mode} --min-files 1 --allow-republish --export-good-on-failure"
        )
        print(f"▶️ Running GX suite ({mode}): {cmd}")
        rc = run_and_stream(cmd)

        # check if _SUCCESS got written (good rows exist)
        ready_bucket = config["ready_bucket"]
        success_uri = f"gs://{ready_bucket}/date={date}/_SUCCESS"
        bucket, path = success_uri[5:].split("/", 1)
        #if storage.Client().bucket(bucket).blob(path).exists():
        #    good_exported = True
        if _get_client("st").bucket(bucket).blob(path).exists():
            good_exported = True

        if rc != 0:
            print(f"❌ GX {mode} suite failed")
            success_all = False
        else:
            print(f"✅ GX {mode} suite succeeded")

    if success_all:
        print("✅ All GX suites passed successfully.")
        return True

    if export_good and good_exported:
        print("⚠️ GX failed, but exported good rows → allowing downstream Silver to proceed.")
        return {"status": "partial_ok", "good_exported": True}

    print("❌ GX failed and no good rows were exported. Raising exception.")
    raise RuntimeError("One or more GX suites failed and no good rows to export.")



def short_circuit_if_failed(**kwargs):
    success = kwargs["ti"].xcom_pull(task_ids="gx_validate")
    return bool(success)


def check_ready_publish(ready_bucket, date_prefix, max_publish_age_minutes=180, timezone="UTC"):
    """
    Verify GX publish completed for today's slice:
      - Both _MANIFEST.json and _SUCCESS under ready/date=<ds>/
      - Fresh: both updated within max_publish_age_minutes
    Fail the task if missing/stale (indicates broken publish).
    """
    tz = pendulum.timezone(timezone)
    now = pendulum.now(tz)

    st = _get_client("st")
    names_blobs = list(st.list_blobs(ready_bucket, prefix=date_prefix))
    names = [b.name for b in names_blobs]
    print(f"READY scan under gs://{ready_bucket}/{date_prefix}: {len(names)} object(s)")
    for n in names[:50]:
        print(f"  • {n}")

    manifest = next((b for b in names_blobs if b.name.endswith("_MANIFEST.json")), None)
    success  = next((b for b in names_blobs if b.name.endswith("_SUCCESS")), None)

    if not manifest or not success:
        raise ValueError(f"Publish markers missing under gs://{ready_bucket}/{date_prefix}")

    # Freshness
    m_dt = pendulum.instance(manifest.updated).in_timezone(tz)
    s_dt = pendulum.instance(success.updated).in_timezone(tz)
    age_m = (now - m_dt).total_seconds() / 60.0
    age_s = (now - s_dt).total_seconds() / 60.0
    print(f"_MANIFEST.json mtime: {m_dt.to_iso8601_string()} (~{age_m:.1f} min)")
    print(f"_SUCCESS       mtime: {s_dt.to_iso8601_string()} (~{age_s:.1f} min)")

    if age_m > max_publish_age_minutes or age_s > max_publish_age_minutes:
        raise ValueError(
            f"Publish markers are stale (> {max_publish_age_minutes} min). "
            f"manifest_age={age_m:.1f}min, success_age={age_s:.1f}min"
        )

    print("✅ Publish markers present and fresh.")
    return True

def _sync_schema_to_marquez(**context):
    # Figure out which suites exist for this dataset
    suites = ["basic", "cross"]
    # if cfg.get("suite_basic"):
    #     suites.append("basic")
    # if cfg.get("suite_cross"):
    #     suites.append("cross")

    if not suites:
        print("[schema-sync] No suites configured; skipping schema upsert.")
        return

    project, dataset, table = cfg["bq_table"].split(".")

    for mode in suites:
        attach_job = f"gx_pipeline__{name}.gx_validate.{mode}"
        cmd = [
            "python",
            "/opt/airflow/dags/helpers/upsert_bq_schema_to_marquez.py",
            "--project", project,
            "--dataset", dataset,
            "--table", table,
            "--attach-to-job", attach_job,
            "--marquez-url", os.getenv("OPENLINEAGE_URL", "http://marquez:5000"),
            "--ol-namespace", os.getenv("OPENLINEAGE_NAMESPACE", "gx-bq-pipeline"),
            "--dataset-namespace", "bigquery",
        ]
        print(f"[schema-sync] Attaching schema to {attach_job} via:", " ".join(cmd))
        try:
            subprocess.run(cmd, check=True)
            print(f"[schema-sync] ✅ attached to {attach_job}")
        except Exception as e:
            print(f"[schema-sync] ⚠️ attach failed for {attach_job}: {e} (continuing)")

# ---------------------------------------------------------------
# DAG builders
# ---------------------------------------------------------------

def build_gx_pipeline_dag(name, cfg):
    cfg["name"] = name
    default_args = {
        "owner": "airflow",
        "queue": "gx_bq_pipeline",
        "depends_on_past": False,
        "retries": cfg.get("retries", 2),
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "email": [],
        #"email": cfg.get("alert_email"),
        #"on_failure_callback": notify_slack,
    }

    dag = DAG(
        dag_id=f"gx_pipeline__{name}",
        description=f"GX validation + publish for {name}",
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval=cfg.get("schedule", "0 5 * * *"),
        catchup=False,
        tags=["gx", "bq", "gcs"],
        params={
        # UI toggle when you click “Trigger DAG w/ config”
            "export_good_on_failure": Param(False, type="boolean", description="Export GOOD rows even if validation fails"),
        },
    )

    with dag:
        precheck = PythonOperator(
            task_id="precheck_resources",
            python_callable=precheck_resources,
            op_args=[cfg["bq_table"], cfg["buckets"]],
        )

        wait_input = PythonOperator(
            task_id="wait_for_input_ready",
            python_callable=wait_for_input_ready,
            op_args=[cfg["landing_bucket"], "date={{ ds }}"],
            op_kwargs={
                "min_files": cfg.get("landing_min_files", 1),
                "required_suffix": cfg.get("landing_required_suffix", ".parquet"),
                "accept_success_marker": cfg.get("landing_accept_success", True),
                "max_input_age_minutes": cfg.get("max_input_age_minutes", 180),
                "enforce_today_only": cfg.get("enforce_today_only", True),
                "timezone": cfg.get("timezone", "UTC"),
                "ds": "{{ ds }}",
            },
        )

        gx_task = PythonOperator(
            task_id="gx_validate",
            python_callable=gx_validate,
            op_args=[cfg],
        )
        
        sync_schema = PythonOperator(
            task_id="sync_schema",
            python_callable=_sync_schema_to_marquez,
            provide_context=True,
        )

        check_publish = PythonOperator(
            task_id="check_ready_publish",
            python_callable=check_ready_publish,
            op_args=[cfg["ready_bucket"], "date={{ ds }}"],
            op_kwargs={
                "max_publish_age_minutes": cfg.get("max_publish_age_minutes", 180),
                "timezone": cfg.get("timezone", "UTC"),
            },
        )
   
        shortcircuit = ShortCircuitOperator(
            task_id="short_circuit_if_failed",
            python_callable=short_circuit_if_failed,
        )

        trigger_consumer = TriggerDagRunOperator(
            task_id="trigger_consumer",
            trigger_dag_id=f"gx_consumer__{name}",
            wait_for_completion=False,
            reset_dag_run=True,
        )
        precheck >> wait_input >> gx_task >> sync_schema >> check_publish >> shortcircuit >> trigger_consumer
        
    return dag


def build_consumer_dag(name, cfg):
    cfg["name"] = name
    # Read Airflow Variables (with safe defaults)
    project_id     = Variable.get("PROJECT_ID", default_var=cfg.get("project_id"))
    dataset_bronze = Variable.get("DATASET_BRONZE", default_var=cfg["bq_table"].split(".")[1])
    bronze_table   = Variable.get("BRONZE_TABLE",   default_var=cfg["bq_table"].split(".")[2])
    dataset_silver = Variable.get("DATASET_SILVER", default_var="mkt_silver")
    silver_table   = Variable.get("SILVER_TABLE",   default_var="dim_customers_scd2")
    silver_dag_id  = Variable.get("SILVER_DAG_ID",  default_var="silver_scd2_dim_customers")
    default_args = {
        "owner": "airflow",
        "queue": "gx_bq_pipeline",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "email_on_failure": False,
        #"email": cfg.get("alert_email"),
        #"on_failure_callback": notify_slack,
    }

    dag = DAG(
        dag_id=f"gx_consumer__{name}",
        description=f"Trigger Bronze→Silver for {name}",
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval=None,  # triggered by gx_pipeline__<name>
        catchup=False,
        tags=["gx", "consumer", name],
    )

    def _gcs_exists_uri(gs_uri: str) -> bool:
        assert gs_uri.startswith("gs://")
        bucket, path = gs_uri[5:].split("/", 1)
        return _get_client("st").bucket(bucket).blob(path).exists()

    def _bronze_success_exists(**context) -> bool:
        run_date = context["ds"]
        ready_bucket = cfg["ready_bucket"]
        success_uri = f"gs://{ready_bucket}/date={run_date}/_SUCCESS"
        if not _gcs_exists_uri(success_uri):
            print(f"[consumer gate] Missing bronze success: {success_uri} → SKIP")
            return False
        print(f"[consumer gate] Found bronze success: {success_uri}")
        return True
    
    def _emit_consumer_ol(**context):
        if not _OL:
            print("[OL] openlineage client not available; skipping consumer emission.")
            return
        ns = os.getenv("OPENLINEAGE_NAMESPACE", "gx-bq-pipeline")
        url = os.getenv("OPENLINEAGE_URL", "http://marquez:5000")
        client = OpenLineageClient(url)

        run_date = context["ds"]
        # Inputs/outputs aligned with gx_validate.py convention
        gcs_input  = Dataset(namespace="gcs",      name=f"{cfg['ready_bucket']}/date={run_date}/")
        bq_output  = Dataset(namespace="bigquery", name=f"{project_id}.{dataset_silver}.{silver_table}")

        # Name the consumer job clearly
        job_name = f"gx_consumer__{name}.consume_and_trigger"
        run_id = str(_uuid.uuid4())
        now = _dt.utcnow().isoformat() + "Z"
        producer = "app://gx_consumer"
        print(f"[OL] Emitting consumer lineage to {url} in ns={ns}")

        # START
        client.emit(RunEvent(
            eventType=RunState.START,
            eventTime=now,
            run=Run(runId=run_id),
            job=Job(namespace=ns, name=job_name),
            inputs=[gcs_input], outputs=[bq_output],
            producer=producer,
        ))
        # Since this task is a control-plane handoff, immediately COMPLETE
        client.emit(RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=_dt.utcnow().isoformat() + "Z",
            run=Run(runId=run_id),
            job=Job(namespace=ns, name=job_name),
            inputs=[gcs_input], outputs=[bq_output],
            producer=producer,
        ))
        print("[OL] Emitted consumer handoff (GCS → BQ Silver).")
    
    with dag:
        gate = ShortCircuitOperator(
            task_id="wait_for_bronze_success",
            python_callable=_bronze_success_exists,
        )

        def _build_conf(**context):
            run_date = context["ds"]
            return {
                "project_id": project_id,
                "dataset_bronze": dataset_bronze,
                "bronze_table": bronze_table,
                "dataset_silver": dataset_silver,
                "silver_table": silver_table,
                "process_date": run_date,
            }

        prep = PythonOperator(
            task_id="prepare_conf",
            python_callable=_build_conf,
            do_xcom_push=True,
        )
        
        emit_ol = PythonOperator(
            task_id="emit_consumer_lineage",
            python_callable=_emit_consumer_ol,
        )

        trigger_bq = TriggerDagRunOperator(
            task_id="trigger_bronze_to_silver",
            trigger_dag_id=silver_dag_id,
            conf="{{ ti.xcom_pull(task_ids='prepare_conf') | tojson }}",
            wait_for_completion=False,
            reset_dag_run=True,
        )

        gate >> prep >> emit_ol >> trigger_bq

    return dag

# ---------------------------------------------------------------
# Register DAGs dynamically from configs/pipelines.yml
# ---------------------------------------------------------------

CONFIG_PATH = "/opt/airflow/configs/pipelines.yml"
with open(CONFIG_PATH) as f:
    pipelines = yaml.safe_load(f)

for name, cfg in pipelines.items():
    cfg["queue"] = "gx_bq_pipeline"
    globals()[f"gx_pipeline__{name}"] = build_gx_pipeline_dag(name, cfg)
    globals()[f"gx_consumer__{name}"] = build_consumer_dag(name, cfg)
