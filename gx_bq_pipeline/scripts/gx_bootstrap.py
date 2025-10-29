# scripts/gx_bootstrap.py
import os
from pathlib import Path
import argparse
from datetime import date
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from dotenv import load_dotenv
from google.cloud import bigquery

# -------------------- env load --------------------
current_script_dir = Path(__file__).resolve().parent
dotenv_path = current_script_dir.parent.parent / "keys" / ".env"
print(f"dotenv_path: {dotenv_path}")
load_dotenv(dotenv_path=dotenv_path)

GX_DIR   = os.getenv("GX_HOME", "/opt/airflow/gx")
PROJECT  = os.getenv("GCP_PROJECT_ID", "alert-study-468915-n0")
DATASET  = os.getenv("DATASET", "demo_gx")
TABLE    = os.getenv("TABLE_EXT", "orders_ext")
#DS_NAME  = os.getenv("GX_DATASOURCE", "bq_ds")
SUITE    = os.getenv("GX_SUITE_BASIC", "orders_basic_suite")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

# Datasource/asset names to create inside GX
DS_NAME             = os.getenv("GX_DATASOURCE", "bq_orders")
ASSET_BASIC_NAME    = os.getenv("GX_ASSET_BASIC", "orders_ext")            # raw external table asset
ASSET_CROSS_NAME    = os.getenv("GX_ASSET_CROSS", "orders_with_derived")   # BQ view asset

# for building a concrete batch during bootstrap (metrics get computed at save)
RUN_DT   = os.getenv("RUN_DT", date.today().isoformat())  # e.g. 2025-10-23

ap = argparse.ArgumentParser()
ap.add_argument("--reset-suite", action="store_true", help="Drop existing suite before seeding")
args = ap.parse_args()
RESET = args.reset_suite

# -------------------- ensure target table exists --------------------
bq = bigquery.Client(project=PROJECT, location=BQ_LOCATION)
table_fqn = f"{PROJECT}.{DATASET}.{TABLE}"
try:
    bq.get_table(table_fqn)
except Exception as e:
    raise SystemExit(f"[GX] BigQuery table not found: {table_fqn}\n{e}")

for tbl in (ASSET_BASIC_NAME, ASSET_CROSS_NAME):
    fq = f"{PROJECT}.{DATASET}.{tbl}"
    bq.get_table(fq)  # raises if missing

# -------------------- GX context --------------------
Path(GX_DIR).mkdir(parents=True, exist_ok=True)
ctx = gx.get_context(context_root_dir=GX_DIR)

# -------------------- datasource --------------------
# Try explicit BigQuery helper if available; fall back to SQL conn
try:
    ds = ctx.data_sources.add_or_update_bigquery(
        name=DS_NAME, connection_string=f"bigquery://{PROJECT}?location={BQ_LOCATION}"
    )
except Exception:
    ds = ctx.sources.add_or_update_sql(
        name=DS_NAME, connection_string=f"bigquery://{PROJECT}?location={BQ_LOCATION}"
    )

# -------------------- assets --------------------
# register assets as table assets (views work)
def ensure_table_asset(name: str, dataset: str, table: str):
    try:
        ds.add_table_asset(name=name, schema_name=dataset, table_name=table)
        print(f"[GX] Added table asset '{name}' -> {dataset}.{table}")
    except Exception as e:
        # likely already exists; fetch to confirm
        _ = ds.get_asset(name)
        print(f"[GX] Table asset '{name}' already exists ({dataset}.{table})")
        
ensure_table_asset(ASSET_BASIC_NAME, DATASET, ASSET_BASIC_NAME)
ensure_table_asset(ASSET_CROSS_NAME, DATASET, ASSET_CROSS_NAME)

print("[GX] Asset bootstrap complete.")

# -------------------- suite --------------------
created = False
if RESET:
    print(f"[GX] --reset-suite specified. Deleting existing suite '{SUITE}'.")
    try:
        ctx.suites.delete(SUITE)
    except Exception:
        pass
    suite = ExpectationSuite(name=SUITE)
    ctx.suites.add(suite)
    created = True
else:
    try:
        suite = ctx.suites.get(SUITE)
        print(f"[GX] Suite '{SUITE}' already exists.")
    except Exception:
        suite = ExpectationSuite(name=SUITE)
        ctx.suites.add(suite)
        created = True
        print(f"[GX] Created suite '{SUITE}'.")

def _existing_keys(s):
    keys = set()
    try:
        for e in s.expectations:
            etype = getattr(e, "expectation_type", None)
            kwargs = getattr(e, "kwargs", {}) or {}
            col = kwargs.get("column")
            keys.add((etype, col))
    except Exception:
        pass
    return keys

def _desired_expectations():
    return [
        # Required fields
        ("expect_column_values_to_not_be_null", "order_id",        lambda v: v.expect_column_values_to_not_be_null("order_id")),
        ("expect_column_values_to_not_be_null", "customer_id",     lambda v: v.expect_column_values_to_not_be_null("customer_id")),
        ("expect_column_values_to_not_be_null", "order_date",      lambda v: v.expect_column_values_to_not_be_null("order_date")),
        ("expect_column_values_to_not_be_null", "order_timestamp", lambda v: v.expect_column_values_to_not_be_null("order_timestamp")),
        ("expect_column_values_to_not_be_null", "order_amount",    lambda v: v.expect_column_values_to_not_be_null("order_amount")),
        ("expect_column_values_to_not_be_null", "net_amount",      lambda v: v.expect_column_values_to_not_be_null("net_amount")),

        # Uniqueness
        ("expect_column_values_to_be_unique", "order_id",          lambda v: v.expect_column_values_to_be_unique("order_id")),

        # Ranges
        ("expect_column_values_to_be_between", "order_amount",     lambda v: v.expect_column_values_to_be_between("order_amount",    min_value=0, mostly=0.999)),
        ("expect_column_values_to_be_between", "tax_amount",       lambda v: v.expect_column_values_to_be_between("tax_amount",      min_value=0, mostly=0.999)),
        ("expect_column_values_to_be_between", "discount_amount",  lambda v: v.expect_column_values_to_be_between("discount_amount", min_value=0, mostly=0.999)),
        ("expect_column_values_to_be_between", "net_amount",       lambda v: v.expect_column_values_to_be_between("net_amount",      min_value=0, mostly=0.999)),
        ("expect_column_values_to_be_between", "item_count",       lambda v: v.expect_column_values_to_be_between("item_count",      min_value=1, mostly=0.999)),

        # Categoricals
        ("expect_column_values_to_be_in_set",  "order_status",     lambda v: v.expect_column_values_to_be_in_set("order_status",  ["NEW","SHIPPED","DELIVERED","CANCELLED"], mostly=0.99)),
        ("expect_column_values_to_be_in_set",  "payment_method",   lambda v: v.expect_column_values_to_be_in_set("payment_method",["Credit Card","Wallet","COD","UPI"],     mostly=0.99)),
        ("expect_column_values_to_be_in_set",  "region",           lambda v: v.expect_column_values_to_be_in_set("region",        ["IN-N","IN-S","IN-E","IN-W"],            mostly=0.99)),
        ("expect_column_value_lengths_to_be_between", "currency",  lambda v: v.expect_column_value_lengths_to_be_between("currency", min_value=3, max_value=4)),
        ("expect_column_values_to_be_in_set",  "currency",         lambda v: v.expect_column_values_to_be_in_set("currency", ["INR"], mostly=1.0)),

        # Volume guardrail (per-day slice)
        ("expect_table_row_count_to_be_between", None,             lambda v: v.expect_table_row_count_to_be_between(min_value=0, max_value=50_000_000)),

        # Cross-column checks via derived cols
        ("expect_column_values_to_be_between", "amt_diff",         lambda v: v.expect_column_values_to_be_between("amt_diff", min_value=0, max_value=0.05, mostly=0.999)),
        ("expect_column_values_to_be_in_set",  "ts_date_mismatch_flag",
         lambda v: v.expect_column_values_to_be_in_set("ts_date_mismatch_flag", value_set=[0], mostly=0.99)),
    ]

existing = _existing_keys(suite)
desired  = _desired_expectations()
missing  = [(etype, col, fn) for (etype, col, fn) in desired if (etype, col) not in existing]

if not missing and len(existing) > 0:
    print(f"[GX] Suite '{SUITE}' already has {len(existing)} expectations. No changes.")
else:
    print(f"[GX] Adding {len(missing)} new expectations to suite '{SUITE}'.")
    # IMPORTANT: build validator on the parameterized slice asset (ensures derived cols exist)
    derived_asset = ds.get_asset("orders_with_derived")
    validator = ctx.get_validator(
        batch_request=derived_asset.build_batch_request(),
        expectation_suite_name=SUITE
    )
    for _, _, fn in missing:
        fn(validator)

    try:
        validator.save_expectation_suite(discard_failed_expectations=False)
        print(f"[GX] Saved suite '{SUITE}' with {len(existing)+len(missing)} expectations.")
    except Exception:
        ctx.suites.delete(SUITE)
        ctx.suites.add(validator.expectation_suite)
        print(f"[GX] Replaced suite '{SUITE}' with {len(existing)+len(missing)} expectations.")
