# scripts/gx_bootstrap_cross.py
import os
from pathlib import Path
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from dotenv import load_dotenv
from google.cloud import bigquery

# ---------------- Env ----------------
current_dir = Path(__file__).resolve().parent
dotenv_path = current_dir.parent.parent / "keys" / ".env"
print(f"dotenv_path: {dotenv_path}")
load_dotenv(dotenv_path=dotenv_path)

GX_DIR  = os.getenv("GX_HOME", "/opt/airflow/gx")
PROJECT = os.getenv("GCP_PROJECT_ID", "alert-study-468915-n0")
DATASET = os.getenv("DATASET", "demo_gx")
TABLE   = os.getenv("TABLE_EXT", "orders_ext")
DS_NAME = os.getenv("GX_DATASOURCE", "bq_ds")
SUITE   = "orders_cross_suite"

# ---------------- Ensure BQ table exists ----------------
bq = bigquery.Client(project=PROJECT)
table_fqn = f"{PROJECT}.{DATASET}.{TABLE}"
bq.get_table(table_fqn)  # raises NotFound if missing
print(f"[GX] BigQuery table found: {table_fqn}")

# ---------------- GX Context + Datasource ----------------
Path(GX_DIR).mkdir(parents=True, exist_ok=True)
ctx = gx.get_context(context_root_dir=GX_DIR)

try:
    ds = ctx.data_sources.add_or_update_bigquery(
        name=DS_NAME, connection_string=f"bigquery://{PROJECT}"
    )
except Exception:
    ds = ctx.sources.add_or_update_sql(
        name=DS_NAME, connection_string=f"bigquery://{PROJECT}"
    )

# ---------------- Helpers ----------------
def get_or_create_table_asset(ds, name: str, schema: str, table: str):
    try:
        return ds.get_asset(name)
    except Exception:
        # create then fetch
        try:
            created = ds.add_table_asset(name=name, schema_name=schema, table_name=table)
            return created
        except Exception:
            # if another process created it in-between
            return ds.get_asset(name)

def add_or_update_query_asset(ds, name: str, query: str):
    if hasattr(ds, "add_or_update_query_asset"):
        return ds.add_or_update_query_asset(name=name, query=query)
    # Fallback: try add, else assume it exists and get it
    try:
        return ds.add_query_asset(name=name, query=query)
    except Exception:
        return ds.get_asset(name)

# ---------------- Assets ----------------
# 1) Table asset (pointer to the real external table)
table_asset = get_or_create_table_asset(ds, name=TABLE, schema=DATASET, table=TABLE)

# 2) Query asset with derived cross-check columns (fast + no SQL expectations needed)
CROSS_ASSET = "orders_cross_checks"
bootstrap_query = f"""
SELECT
  t.*,

  -- arithmetic check
  ABS(net_amount - (order_amount + tax_amount - discount_amount)) AS amt_diff,

  -- date/timestamp consistency as numeric flag (avoid boolean binding issues)
  CASE WHEN DATE(order_timestamp) != order_date THEN 1 ELSE 0 END AS ts_date_mismatch_flag,

  -- discount should never exceed (order_amount + tax_amount)
  GREATEST(discount_amount - (order_amount + tax_amount), 0) AS discount_excess,
  CASE WHEN discount_amount > (order_amount + tax_amount) THEN 1 ELSE 0 END AS discount_excess_flag

FROM `{PROJECT}.{DATASET}.{TABLE}` AS t
"""

cross_asset = add_or_update_query_asset(ds, name=CROSS_ASSET, query=bootstrap_query)

# ---------------- (Re)create the mini suite ----------------
try:
    ctx.suites.delete(SUITE)  # make it deterministic (always the same 3 checks)
    print(f"[GX] Deleted existing suite '{SUITE}'.")
except Exception:
    pass

suite = ExpectationSuite(name=SUITE)
ctx.suites.add(suite)

# Build validator off the cross-checks asset (so columns exist)
validator = ctx.get_validator(
    batch_request=cross_asset.build_batch_request(),
    expectation_suite_name=SUITE
)

# ---- 3 lightweight expectations (fast daily checks) ----
# 1) Arithmetic consistency: net ≈ order + tax - discount (within 0.01)
validator.expect_column_values_to_be_between(
    column="amt_diff", min_value=0, max_value=0.01, mostly=1.0
)

# 2) Timestamp/date should match (flag must be 0)
validator.expect_column_values_to_be_in_set(
    column="ts_date_mismatch_flag", value_set=[0], mostly=1.0
)

# 3) Reasonable volume (whole table)
validator.expect_table_row_count_to_be_between(min_value=1, max_value=50_000_000)

# 4) discount_excess must be 0 (no over-discounting)
validator.expect_column_values_to_be_between(
    column="discount_excess", min_value=0, max_value=0, mostly=1.0
)

# 5) flag must be 0
validator.expect_column_values_to_be_in_set(
    column="discount_excess_flag", value_set=[0], mostly=1.0
)

# Persist (GE 1.7.x: save = add-only → fallback delete+add)
try:
    validator.save_expectation_suite(discard_failed_expectations=False)
    print(f"[GX] Created mini suite '{SUITE}' with 3 expectations.")
except Exception:
    ctx.suites.delete(SUITE)
    ctx.suites.add(validator.expectation_suite)
    print(f"[GX] Replaced mini suite '{SUITE}' with 3 expectations.")
