# planner.py
"""
Traverse lineage via Marquez, check BigQuery partitions, write ops.*, emit log.
"""

import os, json, uuid, logging, re
from datetime import datetime
from app.big_query import BQ
from app.open_lineage import fetch_lineage, extract_job_ios
import google.cloud.logging as gcl

_gcl_client = gcl.Client()                            # uses ADC on Cloud Run
_gcl_client.setup_logging()                           # optional: route stdlib logging to Cloud Logging
_gcl_logger = _gcl_client.logger("backfill-planner")  # log name visible in Cloud Logging
MARQUEZ_URL = os.getenv("MARQUEZ_URL", "http://localhost:5000")
MARQUEZ_NS  = os.getenv("MARQUEZ_NAMESPACE", "gx-bq-pipeline")
AUTO_STUB_TASK_MAP = os.getenv("AUTO_STUB_TASK_MAP", "true").lower() in ("1", "true", "yes")
DATE_DIR_RE = re.compile(r"(^|/)date=\d{4}-\d{2}-\d{2}(/|$)", re.IGNORECASE)

def _is_bootstrap(job_name: str) -> bool:
    return job_name.startswith("bootstrap.")

def _log_plan_created(plan_id, root_bronze_table, requested_date, item_count, project, recommended_cmd=None):
    logging.info(json.dumps({
        "event": "LINEAGE_BACKFILL_PLAN_CREATED",
        "plan_id": plan_id,
        "project": project,
        "root_table": root_bronze_table,
        "date": requested_date,
        "missing_items": int(item_count),
        "recommended_cmd": recommended_cmd or "N/A"
    }))
    payload = {
        "event": "LINEAGE_BACKFILL_PLAN_CREATED",
        "plan_id": plan_id,
        "project": project,
        "root_table": root_bronze_table,
        "date": requested_date,
        "missing_items": int(item_count),
        "recommended_cmd": recommended_cmd or "N/A"
    }
    # Structured log -> jsonPayload
    _gcl_logger.log_struct(payload, severity="INFO")

def _strip_ns(s: str) -> tuple[str, str]:
    return s.split(":", 1) if ":" in s else ("", s)

def _fix_gcs_path(name: str) -> str:
    """
    Normalize GCS 'name' (no 'gcs:' prefix):
    - If 'date=' exists but there is no slash before it, insert one.
    - Collapse double slashes.
    - Ensure trailing slash.
    """
    name = (name or "").strip()
    # If no slash at all but we clearly have a hive dir, insert one before "date="
    if "/" not in name and "date=" in name:
        idx = name.find("date=")
        if idx > 0:
            name = name[:idx] + "/" + name[idx:]
    # Normalize double slashes
    name = name.replace("//", "/")
    # Ensure trailing slash for prefixes
    if not name.endswith("/"):
        name = name + "/"
    return name

def _norm_ds(s: str) -> str:
    """
    Normalize to a canonical internal key (drop namespace).
    bigquery -> project.dataset.table
    gcs      -> bucket/prefix/
    """
    ns, name = _strip_ns(s)
    return _fix_gcs_path(name) if ns == "gcs" else name

def _retarget_date_segment(path: str, target_date: str) -> str:
    """
    Replace any 'date=YYYY-MM-DD' segment with target_date, preserving slashes.
    """
    m = DATE_DIR_RE.search(path or "")
    if not m:
        return path
    s, e = m.span()
    return (path[:s] + f"date={target_date}" + path[e:])

def _pretty_name_for_output(s: str) -> str:
    """
    Format for response:
    - gcs -> bucket/prefix/
    - bq  -> project.dataset.table
    """
    ns, name = _strip_ns(s)
    if ns == "gcs":
        return _fix_gcs_path(name)
    return name


def _maybe_stub_task_map(bq: BQ, job_name: str, partition_field: str | None) -> bool:
    """
    Create/ensure a stub row in ops.task_map for job_name if missing.
    Returns True if we attempted/inserted, False otherwise.
    Will silently no-op if BQ helper does not expose insert_taskmap_stub().
    """
    if not AUTO_STUB_TASK_MAP or not job_name:
        return False

    # guard if helper doesn’t support it yet
    if not hasattr(bq, "insert_taskmap_stub"):
        return False

    try:
        # partition_field is useful context even if dag/task are unknown now
        bq.insert_taskmap_stub(job_name=job_name, dag_id=None, task_id=None, note=f"stubbed {datetime.utcnow().isoformat()}Z")
        return True
    except Exception as e:
        logging.warning(f"[planner] Could not stub task_map for '{job_name}': {e}")
        return False


def build_plan(project: str, root_bronze_table: str, date_str: str, created_by: str):
    run_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    bq = BQ(project)

    bq.ensure_ops_tables()

    lin = fetch_lineage(MARQUEZ_URL, MARQUEZ_NS, root_bronze_table, depth=20)
    job_ios, downstream_datasets = extract_job_ios(lin)

    cadence = bq.load_cadence()
    taskmap = bq.load_taskmap()

    plan_id = str(uuid.uuid4())
    bq.insert_plan_header(plan_id, run_date, root_bronze_table, created_by)

    producer_by_output: dict[str, str] = {}
    bronze_outputs_norm: set[str] = set()

    for io in job_ios:
        job_name = io["job_name"]
        for raw_out in io.get("outputs", []):
            producer_by_output[raw_out] = job_name
            producer_by_output[_norm_ds(raw_out)] = job_name
        if job_name == root_bronze_table:
            bronze_outputs_norm |= {_norm_ds(x) for x in io.get("outputs", [])}

    candidate_ds = {_norm_ds(ds) for ds in downstream_datasets}

    items = []
    priority = 100

    for ds in sorted(candidate_ds):
        
        # 1) normalize to canonical (drops namespace; fixes gcs slashes)
        ds_norm = _norm_ds(ds)

        # 2) retarget the hive date segment to requested date
        ds_target = _retarget_date_segment(ds_norm, date_str)

        # 3) ensure gcs slashes/trailing slash after retarget
        ds_target = _fix_gcs_path(ds_target)
        
        producing_job = producer_by_output.get(ds_norm)
        if not producing_job:
            for k, v in producer_by_output.items():
                if _norm_ds(k) == ds_norm:
                    producing_job = v
                    break
        if (not producing_job) and (ds_norm in bronze_outputs_norm):
            producing_job = root_bronze_table

        # partition field selection
        is_bronze_output = ds_norm in bronze_outputs_norm
        part_field = None if is_bronze_output else cadence.get(producing_job or "", {}).get("partition_field", "_ingestion_date")

        if not bq.partition_exists(ds_target, part_field, date_str):
            tm = taskmap.get(producing_job or "", {})
            dag = tm.get("dag_id")
            task = tm.get("task_id")

            task_map_stubbed = False
            if dag and task:
                cmd = f"airflow tasks run {dag} {task} {date_str} --local"
                reason = f"Missing {date_str} in {ds_target}" + ("" if part_field is None else f" (field: {part_field})")
            elif dag:
                cmd = f"airflow dags backfill {dag} -s {date_str} -e {date_str}"
                reason = f"Missing {date_str}; no task mapping for {producing_job or 'unknown'}"
            else:
                task_map_stubbed = _maybe_stub_task_map(bq, producing_job or "", partition_field=None)
                cmd = "-- Add task_map row for producing job to get precise command"
                reason = f"Missing {date_str}; producing job unmapped: {producing_job or 'unknown'}" + \
                        (" (stubbed ops.task_map)" if task_map_stubbed else "")

            # Persist the item (store the *retargeted* path)
            bq.insert_plan_item(
                plan_id,
                ds_target,
                producing_job or "unknown",
                [run_date],
                cmd,
                reason,
                priority,
            )

            # Return a pretty value (drop 'gcs:' and ensure slashes)
            items.append({
                "downstream_table": _pretty_name_for_output(ds_target),
                "producing_job": producing_job or "unknown",
                "recommended_cmd": cmd,
                "reason": reason,
                "priority": priority,
                "task_map_stubbed": task_map_stubbed,
            })
            priority += 10
    recommended_cmd = items[0]["recommended_cmd"] if items else None
    _log_plan_created(plan_id, root_bronze_table, date_str, len(items), project, recommended_cmd)

    return {
        "plan_id": plan_id,
        "root_bronze_table": root_bronze_table,
        "requested_date": date_str,
        "items": sorted(items, key=lambda x: x["priority"])
    }
