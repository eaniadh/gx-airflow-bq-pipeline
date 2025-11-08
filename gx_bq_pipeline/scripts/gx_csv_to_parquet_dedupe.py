from airflow.decorators import dag, task
#from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime
from typing import List, Dict, Optional
from collections import defaultdict
import pandas as pd
import json, io, re, gzip, os, tempfile, pathlib

import fsspec
from google.cloud import storage
import great_expectations as ge
from great_expectations.core import ExpectationSuite
from great_expectations.data_context.types.base import DataContextConfig

# ========= CONFIG =========
PROJECT_ID       = "alert-study-468915-n0"     # <- change if needed
DATASET          = "ch04"                      # <- change if needed

RAW_BUCKET       = "raw-bucket"                # CSV lands here
RAW_BASE_PREFIX  = "orders/"                   # expects subfolders ingest_dt=YYYY-MM-DD/

PARQUET_BUCKET   = RAW_BUCKET                  # write Parquet here (can be a different bucket)
PARQUET_BASE     = "parquet/"                  # mirror same ingest_dt partition under this base

REPORT_BUCKET    = "my-gx-reports"             # GX Data Docs, summaries, etc.

FILE_REGEX       = r".*\.csv$"                 # we start from CSV only (conversion step writes Parquet)
CHUNK_ROWS       = 200_000                     # tune for your machine
SAMPLE_BAD_MAX   = 10_000                      # cap for bad-row sample (optional)

FILTER_BAD_ROWS = True
QUARANTINE_BUCKET = REPORT_BUCKET
QUARANTINE_PREFIX = "quarantine/"
TARGET_PART_ROWS = 300_000  # coalesce multiple CSV chunks into a single Parquet part

# ==========================

# ---- GX expectations you’re learning with ----
REQUIRED_COLUMNS = ["order_id", "amount", "views", "source"]
RANGES           = {"views": (0, 10_000), "amount": (0, 1_000_000)}
UNIQUE_WITHIN_CHUNK = ["order_id"]             # note: global de-dup done in BigQuery
SOURCE_ALLOWED   = ["Google", "Microsoft", "Facebook", "Apple", "Amazon"]

def _upload_bytes(bucket: str, key: str, data: bytes, content_type="application/octet-stream"):
    storage.Client().bucket(bucket).blob(key).upload_from_string(data, content_type=content_type)

def _list_gcs_files(bucket: str, prefix: str, regex: Optional[str]) -> List[str]:
    client = storage.Client()
    pat = re.compile(regex) if regex else None
    uris = []
    for b in client.list_blobs(bucket, prefix=prefix):
        if pat is None or pat.match(b.name):
            uris.append(f"gs://{bucket}/{b.name}")
    uris.sort()
    return uris

def _detect_latest_ingest_dt_prefix(bucket: str, base_prefix: str) -> Optional[str]:
    """Returns 'base_prefix/ingest_dt=YYYY-MM-DD/' with the max date, or None."""
    client = storage.Client()
    re_part = re.compile(rf"^{re.escape(base_prefix)}ingest_dt=(\d{{4}}-\d{{2}}-\d{{2}})/")
    latest = None
    for b in client.list_blobs(bucket, prefix=base_prefix):
        m = re_part.match(b.name)
        if m:
            d = m.group(1)
            if (latest is None) or (d > latest):  # ISO date string order works
                latest = d
    return f"{base_prefix}ingest_dt={latest}/" if latest else None

def _gx_context(tmpdir: str) -> ge.DataContext:
    cfg = DataContextConfig(
        store_backend_defaults=ge.data_context.store_utilities.InMemoryStoreBackendDefaults(),
        anonymous_usage_statistics={"enabled": False},
        data_docs_sites={
            "local_site": {
                "class_name": "SiteBuilder",
                "store_backend": {"class_name": "TupleFilesystemStoreBackend",
                                  "base_directory": os.path.join(tmpdir, "data_docs")},
                "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
            }
        },
        expectations_store_name="expectations_store",
        validations_store_name="validations_store",
        evaluation_parameter_store_name="evaluation_parameter_store",
        include_rendered_content={"globally": True, "expectation_validation_results": True},
    )
    return ge.get_context(project_config=cfg)

def _ensure_suite(ctx: ge.DataContext, name: str = "orders_suite") -> ExpectationSuite:
    suites = {s.expectation_suite_name for s in ctx.list_expectation_suites()}
    if name in suites:
        return ctx.get_expectation_suite(name)
    suite = ge.core.ExpectationSuite(expectation_suite_name=name)
    ctx.add_or_update_expectation_suite(expectation_suite=suite)
    return suite

def _apply_gx_expectations(v: ge.validator.validator.Validator):
    # 1) required columns
    for col in REQUIRED_COLUMNS:
        v.expect_column_to_exist(col)
    # 2) not-null
    if "order_id" in v.active_batch.data.columns:
        v.expect_column_values_to_not_be_null("order_id")
    # 3) within-chunk uniqueness (final global de-dup happens later in BQ)
    for col in UNIQUE_WITHIN_CHUNK:
        if col in v.active_batch.data.columns:
            v.expect_column_values_to_be_unique(col)
    # 4) ranges
    for col, (lo, hi) in RANGES.items():
        if col in v.active_batch.data.columns:
            v.expect_column_values_to_be_between(col, min_value=lo, max_value=hi,
                                                 strict_min=False, strict_max=False)
    # 5) allowed set
    if "source" in v.active_batch.data.columns:
        v.expect_column_values_to_be_in_set("source", SOURCE_ALLOWED)

def _delete_prefix(bucket: str, prefix: str):
    client = storage.Client()
    b = client.bucket(bucket)
    blobs = list(b.list_blobs(prefix=prefix))
    if blobs:
        b.delete_blobs(blobs)

@dag(start_date=datetime(2025,10,1), schedule=None, catchup=False,
     tags=["gx","csv2parquet","external-table","dedupe"])
def gx_csv_to_parquet_dedupe():

    # ---- 1) Detect latest partition ----
    @task
    def detect_latest_partition() -> str:
        latest = _detect_latest_ingest_dt_prefix(RAW_BUCKET, RAW_BASE_PREFIX)
        if not latest:
            print(f"No ingest_dt partition found under {RAW_BASE_PREFIX}")
            return ""
        print(f"Latest partition: {latest}")
        return latest

    # ---- 2) List CSV files in that partition ----
    @task
    def list_csv_files(latest_prefix: str) -> List[str]:
        if not latest_prefix:
            return []
        uris = _list_gcs_files(RAW_BUCKET, latest_prefix, FILE_REGEX)
        print(f"{len(uris)} CSV file(s) in {latest_prefix}")
        return uris

    # ---- 3) GX validate (chunked) + upload Data Docs to GCS; return URIs via XCom ----
    @task
    def gx_validate_csv(file_uri: str) -> Dict:
        rows_total = 0
        rows_bad   = 0
        rule_counts = defaultdict(int)
        sampled = 0
        bad_samples = []

        # choose reader (CSV)
        chunk_iter = pd.read_csv(
            file_uri, storage_options={"token": "google_default"},
            chunksize=CHUNK_ROWS, low_memory=True
        )

        with tempfile.TemporaryDirectory() as tmp:
            ctx   = _gx_context(tmp)
            suite = _ensure_suite(ctx, "orders_suite")

            for chunk in chunk_iter:
                n = len(chunk)
                rows_total += n
                if n == 0:
                    continue

                # simple row-level masks (for a small sample of bad rows)
                fail_masks = {}
                # missing required columns => whole chunk fails those rules
                missing = [c for c in REQUIRED_COLUMNS if c not in chunk.columns]
                for col in missing:
                    rule_counts[f"missing:{col}"] += n
                    fail_masks[f"missing:{col}"] = pd.Series(True, index=chunk.index)

                # not-null
                if "order_id" in chunk.columns:
                    m = chunk["order_id"].isna()
                    if m.any():
                        rule_counts["nn:order_id"] += int(m.sum())
                        fail_masks["nn:order_id"] = m

                # ranges
                for col, (lo, hi) in RANGES.items():
                    if col in chunk.columns:
                        m = ~chunk[col].between(lo, hi, inclusive="both")
                        if m.any():
                            key = f"range:{col}"
                            rule_counts[key] += int(m.sum())
                            fail_masks[key] = m if key not in fail_masks else (fail_masks[key] | m)

                # allowed set
                if "source" in chunk.columns:
                    m = ~chunk["source"].isin(SOURCE_ALLOWED)
                    if m.any():
                        rule_counts["isin:source"] += int(m.sum())
                        fail_masks["isin:source"] = m if "isin:source" not in fail_masks else (fail_masks["isin:source"] | m)

                # within-chunk duplicates (approx evidence only)
                for col in UNIQUE_WITHIN_CHUNK:
                    if col in chunk.columns:
                        m = chunk[col].duplicated(keep=False)
                        if m.any():
                            key = f"uniq:{col}"
                            rule_counts[key] += int(m.sum())
                            fail_masks[key] = m if key not in fail_masks else (fail_masks[key] | m)

                # bad rows
                if fail_masks:
                    any_fail = None
                    for fm in fail_masks.values():
                        any_fail = fm if any_fail is None else (any_fail | fm)
                    bad_chunk = chunk.loc[any_fail]
                else:
                    bad_chunk = chunk.iloc[0:0]

                rows_bad += len(bad_chunk)

                # sample & tag small bad-row set
                if not bad_chunk.empty and sampled < SAMPLE_BAD_MAX:
                    flags_df = pd.DataFrame({rule: mask.loc[bad_chunk.index]
                                             for rule, mask in fail_masks.items()})
                    failed_rules = flags_df.apply(lambda s: ",".join(s.index[s.values]), axis=1)
                    bad_tagged = bad_chunk.copy()
                    bad_tagged["_failed_rules"] = failed_rules.values

                    need = SAMPLE_BAD_MAX - sampled
                    take = bad_tagged.head(need)
                    bad_samples.append(take)
                    sampled += len(take)

                # GX per-chunk
                v = ge.from_pandas(chunk)
                v._initialize_expectations(suite)
                _apply_gx_expectations(v)
                _ = v.validate(result_format="SUMMARY")  # we only need docs summary later

            # write artifacts
            safe_name = file_uri.replace("gs://", "").replace("/", "_")

            # rule counts JSON.gz
            counts_path = f"dq_reports/{safe_name}.rule_counts.json.gz"
            _upload_bytes(REPORT_BUCKET, counts_path,
                          gzip.compress(json.dumps(rule_counts, indent=2).encode("utf-8")),
                          "application/gzip")
            counts_uri = f"gs://{REPORT_BUCKET}/{counts_path}"

            # bad sample CSV.gz
            bad_uri = None
            if bad_samples:
                sample_df = pd.concat(bad_samples, ignore_index=True)
                bad_path = f"dq_reports/{safe_name}.bad_sample.csv.gz"
                with io.BytesIO() as buf:
                    sample_df.to_csv(buf, index=False)
                    _upload_bytes(REPORT_BUCKET, bad_path, gzip.compress(buf.getvalue()), "application/gzip")
                bad_uri = f"gs://{REPORT_BUCKET}/{bad_path}"

            # Data Docs
            ctx.build_data_docs()
            local_docs = pathlib.Path(tmp) / "data_docs"
            docs_base_key = f"data_docs/site/{safe_name}/"
            bucket = storage.Client().bucket(REPORT_BUCKET)
            for p in local_docs.rglob("*"):
                if p.is_file():
                    rel = p.relative_to(local_docs).as_posix()
                    bucket.blob(docs_base_key + rel).upload_from_filename(str(p))
            docs_index_uri = f"gs://{REPORT_BUCKET}/{docs_base_key}index.html"

            # summary (small, goes via XCom — your GCS-backed XCom will keep it tiny anyway)
            summary = {
                "file_uri": file_uri,
                "rows_total": rows_total,
                "rows_bad": rows_bad,
                "sampled_bad": sampled,
                "rule_counts_uri": counts_uri,
                "bad_sample_uri": bad_uri,
                "docs_index_uri": docs_index_uri,
            }
            return summary

    # ---- 4) CSV → Parquet (chunked) ----
    @task
    def csv_partition_to_parquet(latest_prefix: str) -> str:
        if not latest_prefix:
            return ""

        src_prefix = latest_prefix                              # orders/ingest_dt=YYYY-MM-DD/
        dst_prefix = PARQUET_BASE + latest_prefix               # parquet/orders/ingest_dt=YYYY-MM-DD/
        _delete_prefix(PARQUET_BUCKET, dst_prefix)              # clean old output

        csv_uris = _list_gcs_files(RAW_BUCKET, src_prefix, FILE_REGEX)
        if not csv_uris:
            print("No CSVs to convert.")
            return dst_prefix

        part_idx = 0
        good_buf = []      # buffer of good-row chunks to coalesce
        good_rows = 0
        bad_part_idx = 0

        def write_good_part():
            nonlocal part_idx, good_buf, good_rows
            if not good_buf:
                return
            df = pd.concat(good_buf, ignore_index=True)
            out_uri = f"gs://{PARQUET_BUCKET}/{dst_prefix}part-{part_idx:05d}.parquet"
            df.to_parquet(out_uri, engine="pyarrow", compression="snappy", index=False,
                        storage_options={"token": "google_default"})
            part_idx += 1
            good_buf = []
            good_rows = 0

        for uri in csv_uris:
            for chunk in pd.read_csv(uri, storage_options={"token": "google_default"},
                                    chunksize=CHUNK_ROWS, low_memory=True):
                if not FILTER_BAD_ROWS:
                    # no filtering; just write chunks in TARGET_PART_ROWS groups
                    good_buf.append(chunk)
                    good_rows += len(chunk)
                    if good_rows >= TARGET_PART_ROWS:
                        write_good_part()
                    continue

                # --- apply the same rule logic used in GX step ---
                fail_masks = {}
                missing = [c for c in REQUIRED_COLUMNS if c not in chunk.columns]
                for col in missing:
                    fail_masks[f"missing:{col}"] = pd.Series(True, index=chunk.index)

                if "order_id" in chunk.columns:
                    m = chunk["order_id"].isna()
                    if m.any(): fail_masks["nn:order_id"] = m

                for col, (lo, hi) in RANGES.items():
                    if col in chunk.columns:
                        m = ~chunk[col].between(lo, hi, inclusive="both")
                        if m.any():
                            key = f"range:{col}"
                            fail_masks[key] = m if key not in fail_masks else (fail_masks[key] | m)

                if "source" in chunk.columns:
                    m = ~chunk["source"].isin(SOURCE_ALLOWED)
                    if m.any():
                        key = "isin:source"
                        fail_masks[key] = m if key not in fail_masks else (fail_masks[key] | m)

                # within-chunk duplicate evidence (optional to exclude)
                for col in UNIQUE_WITHIN_CHUNK:
                    if col in chunk.columns:
                        m = chunk[col].duplicated(keep=False)
                        if m.any():
                            key = f"uniq:{col}"
                            fail_masks[key] = m if key not in fail_masks else (fail_masks[key] | m)

                if fail_masks:
                    any_fail = None
                    for fm in fail_masks.values():
                        any_fail = fm if any_fail is None else (any_fail | fm)
                    bad_chunk = chunk.loc[any_fail]
                    good_chunk = chunk.loc[~any_fail]
                else:
                    bad_chunk = chunk.iloc[0:0]
                    good_chunk = chunk

                # Write quarantine sample/full (here we write full failing rows; switch to head(N) for sampling)
                if not bad_chunk.empty:
                    flags_df = pd.DataFrame({rule: mask.loc[bad_chunk.index]
                                            for rule, mask in fail_masks.items()})
                    failed_rules = flags_df.apply(lambda s: ",".join(s.index[s.values]), axis=1)
                    bad_tagged = bad_chunk.copy()
                    bad_tagged["_failed_rules"] = failed_rules.values

                    bad_key = (f"{QUARANTINE_PREFIX}"
                            f"{src_prefix}badrows-{bad_part_idx:05d}.csv.gz")
                    with io.BytesIO() as buf:
                        bad_tagged.to_csv(buf, index=False)
                        _upload_bytes(QUARANTINE_BUCKET, bad_key,
                                    gzip.compress(buf.getvalue()), "application/gzip")
                    bad_part_idx += 1

                # Accumulate good rows and roll Parquet parts by size
                if not good_chunk.empty:
                    good_buf.append(good_chunk)
                    good_rows += len(good_chunk)
                    if good_rows >= TARGET_PART_ROWS:
                        write_good_part()

        write_good_part()
        print(f"Wrote Parquet parts: gs://{PARQUET_BUCKET}/{dst_prefix} (count so far: {part_idx})")
        return dst_prefix

    
    # ---- 5) Create external table on Parquet ----
    create_ext = BigQueryInsertJobOperator(
        task_id="bq_create_external_on_parquet",
        location="US",
        configuration={
            "query": {
                "query": """
CREATE OR REPLACE EXTERNAL TABLE `{{ params.project }}.{{ params.dataset }}.orders_ext_parquet`
OPTIONS (
  format = 'PARQUET',
  uris   = ['gs://{{ params.parquet_bucket }}/{{ ti.xcom_pull(task_ids="csv_partition_to_parquet") }}*']
);
""",
                "useLegacySql": False,
            }
        },
        params={
            "project": PROJECT_ID,
            "dataset": DATASET,
            "parquet_bucket": PARQUET_BUCKET,
        },
    )

    # ---- 6) Final de-dup: load only unique rows into native BQ table ----
    # Order policy: keep earliest by _FILE_NAME (works for external Parquet); if you have ingest_ts TIMESTAMP, prefer that first.
    dedupe_to_native = BigQueryInsertJobOperator(
        task_id="bq_dedupe_to_native",
        location="US",
        configuration={
            "query": {
                "query": f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.orders_dedup` AS
WITH ranked AS (
  SELECT
    t.*,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY _FILE_NAME ASC  -- replace with 'ingest_ts ASC, _FILE_NAME ASC' if you have a TIMESTAMP column
    ) AS rn
  FROM `{PROJECT_ID}.{DATASET}.orders_ext_parquet` t
)
SELECT * EXCEPT(rn)
FROM ranked
WHERE rn = 1;
""",
                "useLegacySql": False,
            }
        },
    )

    # --- wiring ---
    latest_prefix = detect_latest_partition()

    csv_files = list_csv_files(latest_prefix)
    _ = gx_validate_csv.expand(file_uri=csv_files)     # get GX docs + small summaries via XCom (custom backend OK)

    parquet_prefix = csv_partition_to_parquet(latest_prefix)
    create_ext.set_upstream(parquet_prefix)
    dedupe_to_native.set_upstream(create_ext)

gx_csv_to_parquet_dedupe()
