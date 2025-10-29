#!/usr/bin/env python3
import argparse
import os
from datetime import datetime, timezone
from pathlib import Path
import json
from dotenv import load_dotenv
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.exceptions import DataContextError
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account

# ---------------- CLI ----------------
ap = argparse.ArgumentParser(
    description="Validate BigQuery slice via _FILE_NAME and export/publish parquet to GCS with manifest."
)
ap.add_argument("--run-date", "--date", required=True, help="YYYY-MM-DD (matches _FILE_NAME hive partition)")
ap.add_argument("--mode", choices=["basic", "cross"], default="cross",
                help="Validation profile: basic (raw) or cross (derived checks)")
ap.add_argument("--allow-republish", action="store_true",
                help="Allow replacing an already published partition (overwrites final files).")
ap.add_argument("--min-files", type=int, default=1,
                help="Require at least this many staged parquet files before publish.")
ap.add_argument("--dry-run", action="store_true",
                help="Plan-only: do NOT write/delete anything in GCS (validation + exports still run).")
ap.add_argument("--export-good-on-failure", action="store_true",
                help="If set, export GOOD rows even when validation fails (default: do NOT).")
#ap.add_argument("--project", action="store_true", help="GCP project id")
args = ap.parse_args()

RUN_DT = args.run_date
MODE = args.mode
ALLOW_REPUBLISH = args.allow_republish
MIN_FILES = args.min_files
DRY_RUN = args.dry_run
EXPORT_GOOD_ON_FAILURE = bool(args.export_good_on_failure)

# ---------------- ENV / PATHS ----------------
current_script_dir = Path(__file__).resolve().parent
dotenv_path = current_script_dir.parent.parent / "keys" / ".env"
load_dotenv(dotenv_path=dotenv_path)

# Accept both GCP_PROJECT_ID and PROJECT_ID; fall back to default
PROJECT = os.getenv("GCP_PROJECT_ID") or os.getenv("PROJECT_ID") or "alert-study-468915-n0"
DATASET = os.getenv("DATASET", "demo_gx")
TABLE   = os.getenv("TABLE_EXT", "orders_ext")
GX_DIR  = os.getenv("GX_CONTEXT_DIR", str(current_script_dir.parent / "gx"))  # default to repo-local gx/
DS_NAME = os.getenv("GX_DATASOURCE", "bq_orders")
# Permanent assets registered in GX (Option B):
ASSET_BASIC = os.getenv("GX_ASSET_BASIC", "orders_ext")                   # The raw external table asset
ASSET_CROSS = os.getenv("GX_ASSET_CROSS", "orders_with_derived")          # The view with derived columns
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

READY_BUCKET = os.getenv("GX_READY_BUCKET", "gx_data_ready_to_process")
ERROR_BUCKET = os.getenv("GX_ERROR_BUCKET", "gx_data_error")

# Staging/Publish paths
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
final_prefix   = f"date={RUN_DT}/"
staging_prefix = f"date={RUN_DT}/_staging/{RUN_ID}/"
ready_staging_uri = f"gs://{READY_BUCKET}/{staging_prefix}orders-*.parquet"
error_uri         = f"gs://{ERROR_BUCKET}/date={RUN_DT}/orders_bad-*.parquet"

class _FakeBlob:
    """Lightweight stand-in for google.cloud.storage.Blob for DRY-RUN manifest building."""
    def __init__(self, name: str, size: int | None = None):
        self.name = name
        self._size = size
        self.updated = None
        self.md5_hash = None
        self.etag = None
        self.crc32c = None
        self.content_type = None

    @property
    def size(self):
        return self._size

# ---------------- Clients ----------------
def make_storage_client(project: str) -> storage.Client:
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if key_path and os.path.exists(key_path):
        creds = service_account.Credentials.from_service_account_file(
            key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        ).with_quota_project(project)
        return storage.Client(project=project, credentials=creds)
    return storage.Client(project=project)

def make_bq_client(project: str) -> bigquery.Client:
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if key_path and os.path.exists(key_path):
        creds = service_account.Credentials.from_service_account_file(
            key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        return bigquery.Client(project=project, credentials=creds, location=BQ_LOCATION)
    return bigquery.Client(project=project, location=BQ_LOCATION)

gcs = make_storage_client(PROJECT)
bq  = make_bq_client(PROJECT)

# ---------------- GCS Helpers (dry-run aware) ----------------
def _maybe(action: str) -> str:
    return f"[DRY-RUN] {action}" if DRY_RUN else action

def gcs_exists(bucket: str, path: str) -> bool:
    return gcs.bucket(bucket).blob(path).exists()

def gcs_list(bucket: str, prefix: str):
    blobs = list(gcs.list_blobs(bucket, prefix=prefix))
    print(f"[GX] Found {len(blobs)} objects under gs://{bucket}/{prefix}")
    return blobs

def gcs_delete_prefix(bucket: str, prefix: str):
    blobs = gcs_list(bucket, prefix)
    if not blobs:
        return
    if DRY_RUN:
        print(_maybe(f"Would delete {len(blobs)} objects at gs://{bucket}/{prefix}*"))
        return
    with gcs.batch():
        for b in blobs:
            b.delete()

def gcs_copy_prefix(src_bucket: str, src_prefix: str, dst_bucket: str, dst_prefix: str):
    copied = []
    for blob in gcs.list_blobs(src_bucket, prefix=src_prefix):
        rel = blob.name[len(src_prefix):]
        dst_name = dst_prefix + rel
        if DRY_RUN:
            print(_maybe(f"Would copy gs://{src_bucket}/{blob.name} → gs://{dst_bucket}/{dst_name}"))
            copied.append(_FakeBlob(dst_name, getattr(blob, "size", None)))
        else:
            new_blob = gcs.bucket(src_bucket).copy_blob(blob, gcs.bucket(dst_bucket), dst_name)
            copied.append(new_blob)
    return copied

def gcs_write_text(bucket: str, path: str, payload: str = ""):
    if DRY_RUN:
        print(_maybe(f"Would write {len(payload)} bytes → gs://{bucket}/{path}"))
        return
    gcs.bucket(bucket).blob(path).upload_from_string(payload or "")

# ---------------- BQ Helpers ----------------
def _run_sql(client: bigquery.Client, sql: str):
    job = client.query(sql)
    job.result()
    return job

def _export_query_to_parquet(client: bigquery.Client, uri_glob: str, select_sql: str, overwrite: bool = True):
    sql = f"""
    EXPORT DATA OPTIONS(
      uri='{uri_glob}',
      format='PARQUET',
      overwrite={str(overwrite).lower()}
    ) AS
    {select_sql}
    """
    return _run_sql(client, sql)

# ---------------- Manifest ----------------
def build_manifest(bucket: str, final_prefix: str, files: list[storage.Blob], run_meta: dict) -> dict:
    entries = []
    for b in files:
        if not b.name.startswith(final_prefix):
            continue
        entries.append({
            "uri": f"gs://{bucket}/{b.name}",
            "name": b.name.split("/")[-1],
            "size": b.size,
            "updated": b.updated.isoformat() if getattr(b, "updated", None) else None,
            "md5": b.md5_hash,
            "etag": b.etag,
            "crc32c": b.crc32c,
            "content_type": b.content_type,
        })
    return {
        "version": 1,
        "partition": f"gs://{bucket}/{final_prefix}",
        "run": run_meta,
        "files": entries,
        "file_count": len(entries),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

# ---------------- Great Expectations (Option B) ----------------
ctx = gx.get_context(context_root_dir=GX_DIR)

# 1) Ensure datasource exists (Fluent, programmatic; no hardcoding)
#    We build the connection string from env-driven PROJECT and BQ_LOCATION
ds_name = os.getenv("GX_DATASOURCE", DS_NAME)  # prefer env, fallback to DS_NAME
conn_str = f"bigquery://{PROJECT}/{DATASET}?location={BQ_LOCATION}"

# If not present, create; else reuse
try:
    fluent_names = [ds.name for ds in ctx.fluent_datasources]
except Exception:
    fluent_names = []
if ds_name in fluent_names:
    ds = ctx.get_datasource(ds_name)
else:
    print(f"[GX] Registering datasource '{ds_name}' → {conn_str}")
    ds = ctx.sources.add_or_update_sql(name=ds_name, connection_string=conn_str)

# After creating/updating datasource or assets
if hasattr(ctx, "save"):
    try:
        ctx.save()
    except Exception as e:
        print(f"[GX] Non-fatal: could not persist context ({e}). Continuing with in-memory config.")

# 2) Ensure asset exists based on MODE
#    For basic: demo_gx.orders_ext
#    For cross: demo_gx.orders_with_derived (view or table)
asset_name = ASSET_BASIC if MODE == "basic" else ASSET_CROSS
dataset = DATASET
table_name = TABLE if MODE == "basic" else ASSET_CROSS  # "orders_with_derived" by default

existing_assets = [a.name for a in ds.assets]
if asset_name not in existing_assets:
    print(f"[GX] Adding asset '{asset_name}' (schema={dataset}, table={table_name})")
    ds.add_table_asset(name=asset_name, table_name=table_name, schema_name=dataset)
    if hasattr(ctx, "save"):
        try:
            ctx.save()
        except Exception as e:
            print(f"[GX] Non-fatal: could not persist context ({e}). Continuing with in-memory config.")
asset = ds.get_asset(asset_name)

# 3) Load the suite (GE 1.7 legacy accessor)
suite_name = os.getenv("GX_SUITE_NAME", "orders_basic_suite" if MODE == "basic" else "orders_cross_suite")
try:
    #suite = ctx.suites.get(suite_name)
    suite = ctx.get_expectation_suite(suite_name)
except Exception as e:
    print(f"[GX] Suite '{suite_name}' not found: {e}")
    raise SystemExit(
        f"[GX] Expectation suite '{suite_name}' is missing. "
        f"Ensure /opt/airflow/gx/expectations/{suite_name}.json exists."
    )

# 4) Build batch & validator (non-parameterized asset)
batch_request = asset.build_batch_request()
validator = ctx.get_validator(batch_request=batch_request, expectation_suite=suite)
results = validator.validate()

# Debug: show failed expectation types
print("\n[GX] --- Failed Expectation Details ---")
for res in results.results:
    if not getattr(res, "success", False):
        cfg = getattr(res, "expectation_config", None)
        etype = None
        kwargs = {}
        if cfg is not None:
            etype = getattr(cfg, "expectation_type", None) or getattr(cfg, "type", None)
            kwargs = getattr(cfg, "kwargs", {}) or {}
        print(f"❌ {etype or 'unknown'} | kwargs={json.dumps(kwargs, indent=2)}")
print("---------------------------------------\n")

# Stats
total = len(results.results)
passed = sum(1 for r in results.results if getattr(r, "success", False))
failed = total - passed
success = bool(results.success)

# Row count (slice) — computed on the base external table by _FILE_NAME partition
row_count_sql = f"""
SELECT COUNT(*) AS c
FROM `{PROJECT}.{DATASET}.{TABLE}`
WHERE REGEXP_EXTRACT(_FILE_NAME, r'date=([0-9-]+)') = '{RUN_DT}'
"""
row_count = list(bq.query(row_count_sql).result())[0]["c"]

# Failure summary for payload
def _summarize_failures(res):
    acc = []
    for r in res.results:
        if getattr(r, "success", False):
            continue
        cfg = getattr(r, "expectation_config", None)
        etype = None
        col = None
        if cfg is not None:
            etype = getattr(cfg, "expectation_type", None) or getattr(cfg, "type", None)
            col = (getattr(cfg, "kwargs", {}) or {}).get("column")
        acc.append({"expectation_type": etype, "column": col})
    return acc

failed_details = _summarize_failures(results)

# Record result in BQ
payload = {
    "run_date": RUN_DT,
    "run_ts": datetime.now(timezone.utc).isoformat(),
    "success": success,
    "expectations_total": total,
    "expectations_passed": passed,
    "expectations_failed": failed,
    "slice_row_count": int(row_count),
    "suite_name": suite_name,
    "datasource": DS_NAME,
    "data_asset": asset.name,  # actual asset used
    "details": json.dumps({"failed_expectations": failed_details}),
}
table_id = f"{PROJECT}.{DATASET}.gx_validation_results"
errors = bq.insert_rows_json(table_id, [payload])
if errors:
    print("[GX] Warning: could not write summary to BigQuery:", errors)
else:
    print("[GX] Wrote summary to BigQuery:", table_id, payload["run_date"])

# Best-effort: persist validation result in GE stores
try:
    ctx.add_or_update_validation_result(result=results)
except Exception:
    pass

# ---------------- Export good/bad to GCS (stage) ----------------
# NOTE: export still reads from the BASE external table and filters run-date by _FILE_NAME.
date_filter = f"REGEXP_EXTRACT(_FILE_NAME, r'date=([0-9-]+)') = '{RUN_DT}'"

bad_predicate = f"""
(
  order_id IS NULL OR customer_id IS NULL OR order_date IS NULL OR
  order_timestamp IS NULL OR order_amount IS NULL OR net_amount IS NULL OR

  order_amount < 0 OR tax_amount < 0 OR discount_amount < 0 OR net_amount < 0 OR
  item_count < 1 OR

  order_status NOT IN ('NEW','SHIPPED','DELIVERED','CANCELLED') OR
  payment_method NOT IN ('Credit Card','Wallet','COD','UPI') OR
  region NOT IN ('IN-N','IN-S','IN-E','IN-W') OR
  (LENGTH(currency) < 3 OR LENGTH(currency) > 4) OR
  currency NOT IN ('INR') OR

  ABS(net_amount - (order_amount + tax_amount - discount_amount)) > 0.01 OR
  DATE(order_timestamp) != order_date
)
"""

base_from = f"`{PROJECT}.{DATASET}.{TABLE}`"
base_cte = f"""
WITH slice AS (
  SELECT *
  FROM {base_from}
  WHERE {date_filter}
)
"""
good_sql = f"""
{base_cte}
SELECT * FROM slice
WHERE NOT {bad_predicate}
"""
bad_sql = f"""
{base_cte}
SELECT * FROM slice
WHERE {bad_predicate}
"""

# Bad count
bad_count = list(bq.query(f"""
{base_cte}
SELECT COUNT(*) AS c FROM slice WHERE {bad_predicate}
""").result())[0]["c"]

# Export staged
if success or EXPORT_GOOD_ON_FAILURE:
    print(f"[GX] Exporting GOOD rows (staging) → {ready_staging_uri}")
    _export_query_to_parquet(bq, ready_staging_uri, good_sql, overwrite=True)
else:
    print("[GX] Validation failed; skipping GOOD export (export-good-on-failure disabled).")

if int(bad_count) > 0:
    print(f"[GX] Exporting BAD rows ({bad_count}) → {error_uri}")
    _export_query_to_parquet(bq, error_uri, bad_sql, overwrite=True)
else:
    print("[GX] No bad rows detected; skipping BAD export.")

# ---------------- Publish (promote staged → final) ----------------
# Determine if we should publish and write _SUCCESS:
# A) All expectations passed  OR
# B) export-good-on-failure is enabled AND we have at least MIN_FILES staged
staged_parquet = [b for b in gcs_list(READY_BUCKET, staging_prefix) if b.name.endswith(".parquet")]
have_enough_good = len(staged_parquet) >= MIN_FILES
should_publish = success or (EXPORT_GOOD_ON_FAILURE and have_enough_good)

if should_publish:
    final_success_marker = f"{final_prefix}_SUCCESS"

    # Overwrite protection
    if not ALLOW_REPUBLISH and gcs_exists(READY_BUCKET, final_success_marker):
        print(f"[GX] Final already published (found {final_success_marker}). "
              f"Use --allow-republish to overwrite. Aborting publish.")
    else:
        print(f"[GX] Promoting GOOD files for {RUN_DT} to final prefix and writing manifest "
              f"{'(dry-run)' if DRY_RUN else ''}")
        # 1) delete existing finals
        gcs_delete_prefix(READY_BUCKET, final_prefix + "orders-")
        # 2) copy staged → final
        copied_blobs = gcs_copy_prefix(READY_BUCKET, staging_prefix, READY_BUCKET, final_prefix)
        # 3) manifest + _SUCCESS
        run_meta = {
            "run_date": RUN_DT,
            "run_id": RUN_ID,
            "mode": MODE,
            "source_staging": f"gs://{READY_BUCKET}/{staging_prefix}",
            "published_at": datetime.now(timezone.utc).isoformat(),
            "validation_success": bool(success),
            "partial_publish": not bool(success),
            "staged_file_count": len(staged_parquet),
        }
        manifest = build_manifest(READY_BUCKET, final_prefix, copied_blobs, run_meta)
        gcs_write_text(READY_BUCKET, f"{final_prefix}_MANIFEST.json",
                       json.dumps(manifest, separators=(",", ":")))
        gcs_write_text(READY_BUCKET, final_success_marker, "")
        print(f"[GX] Published {manifest['file_count']} files. "
              f"Wrote _SUCCESS at gs://{READY_BUCKET}/{final_prefix}_SUCCESS")
else:
    # Mark staging failed (audit only; keep GOOD in staging)
    failed_meta = {
        "run_date": RUN_DT,
        "run_id": RUN_ID,
        "mode": MODE,
        "at": datetime.now(timezone.utc).isoformat(),
        "validation_success": bool(success),
        "partial_publish": False,
        "staged_file_count": len(staged_parquet),
    }
    gcs_write_text(READY_BUCKET, f"{staging_prefix}_FAILED", json.dumps(failed_meta))


# Exit code (after exports/markers)
if not success:
    raise SystemExit(1)
