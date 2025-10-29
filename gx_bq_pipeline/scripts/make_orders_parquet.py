# make_orders_parquet.py
import os
import json
import random
import argparse
from datetime import date, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
from faker import Faker

import pyarrow as pa
import pyarrow.parquet as pq
import gcsfs  # direct GCS write via fsspec

# ---------------- CONFIG (defaults; can be overridden by CLI/env) ----------------
HERE = Path(__file__).resolve().parent
SCHEMA_FILE = os.environ.get("SCHEMA_FILE", str(HERE / "orders_schema.json"))
BUCKET = os.environ.get("BUCKET", "gx_data_landing")
PROJECT = os.environ.get("PROJECT", "alert-study-468915-n0")
COMPRESSION = os.environ.get("COMPRESSION", "snappy")
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", "500000"))         # rows per chunk
TARGET_BYTES = int(os.environ.get("TARGET_BYTES", "1000000000")) # ~1 GB default
EST_ROW_BYTES = int(os.environ.get("EST_ROW_BYTES", "200"))      # heuristic for ~target size
# -------------------------------------------------------------------------------

# ---- env / paths ----
dotenv_path = HERE.parent.parent / "keys" / ".env"
load_dotenv(dotenv_path=dotenv_path)
GOOGLE_APPLICATION_CREDENTIALS = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    "/home/m78973096_gmail_com/airflow-local/keys/service-account.json",
)

# Faker / RNG
fake = Faker()
Faker.seed(42)
random.seed(42)

def parse_args():
    p = argparse.ArgumentParser(description="Generate orders parquet directly to GCS")
    p.add_argument("--date", help="Partition date YYYY-MM-DD (default: ENV RUN_DT or today)", default=None)
    p.add_argument("--dest", help="Full GCS URI (e.g., gs://bucket/date=YYYY-MM-DD/orders.parquet)", default=None)
    p.add_argument("--rows", type=int, help="Absolute number of rows to generate (overrides target-bytes heuristic)", default=None)
    p.add_argument("--target-bytes", type=int, help="Approx target parquet size in bytes (default from env TARGET_BYTES)", default=None)
    return p.parse_args()

def load_schema(schema_path: str) -> pa.Schema:
    with open('/opt/airflow/scripts/orders_schema.json') as f:
        schema_json = json.load(f)
    type_map = {
        "INT64": pa.int64(), "INTEGER": pa.int64(),
        "FLOAT64": pa.float64(), "FLOAT": pa.float64(),
        "STRING": pa.string(),
        "BOOL": pa.bool_(), "BOOLEAN": pa.bool_(),
        "DATE": pa.date32(),
        "TIMESTAMP": pa.timestamp("us"),
    }
    fields = [pa.field(col["name"], type_map[col["type"].upper()]) for col in schema_json]
    return pa.schema(fields)

def make_fake_record(run_dt: str, order_id: int):
    d = date.fromisoformat(run_dt)
    ts = datetime.combine(d, datetime.min.time()) + timedelta(seconds=random.randint(0, 86399))
    amt = round(random.uniform(100.0, 1000.0), 2)
    tax = round(amt * random.uniform(0.05, 0.18), 2)
    gross = amt + tax
    discount_rate = random.choice([0.0, 0.05, 0.1, 0.2, 0.3])
    disc = round(gross * discount_rate, 2)
    return {
        "order_id": order_id,
        "customer_id": random.randint(1000, 9999),
        "order_date": d,
        "order_timestamp": ts,
        "order_status": random.choice(["NEW", "SHIPPED", "DELIVERED", "CANCELLED"]),
        "payment_method": random.choice(["Credit Card", "Wallet", "COD", "UPI"]),
        "currency": "INR",
        "order_amount": amt,
        "tax_amount": tax,
        "discount_amount": disc,
        "net_amount": round(amt + tax - disc, 2),
        "is_first_order": random.choice([True, False]),
        "region": random.choice(["IN-N", "IN-S", "IN-E", "IN-W"]),
        "category": random.choice(["Electronics", "Books", "Apparel", "Home", "Toys"]),
        "item_count": random.randint(1, 5),
    }

def main():
    args = parse_args()

    # Resolve run date
    run_dt = args.date or os.getenv("RUN_DT") or date.today().isoformat()

    # Resolve destination URI
    if args.dest:
        dst_uri = args.dest
    else:
        # Keep your original convention
        dst_uri = f"gs://{BUCKET}/date={run_dt}/orders.parquet"

    # Resolve row target
    if args.rows is not None:
        target_rows = int(args.rows)
        approx_note = f"(explicit rows={target_rows:,})"
    else:
        target_bytes = args.target_bytes if args.target_bytes is not None else TARGET_BYTES
        target_rows = max(CHUNK_SIZE, int(target_bytes // EST_ROW_BYTES))
        approx_note = f"(approx rows≈{target_rows:,} from target_bytes={target_bytes:,} & est_row={EST_ROW_BYTES}B)"

    print(f"[INFO] Generating Snappy Parquet for {run_dt}")
    print(f"[INFO] Destination: {dst_uri}")
    print(f"[INFO] CHUNK_SIZE={CHUNK_SIZE:,}, TARGET={approx_note}")

    # Schema
    schema = load_schema(SCHEMA_FILE)

    # GCS FS - uses GOOGLE_APPLICATION_CREDENTIALS implicitly
    fs = gcsfs.GCSFileSystem(token="google_default")

    # Atomic write: write to tmp then move
    tmp_uri = dst_uri + ".tmp"

    # Open remote file for streaming write
    with fs.open(tmp_uri, "wb") as fout:
        writer = pq.ParquetWriter(fout, schema, compression=COMPRESSION, use_dictionary=True)
        try:
            rows_written = 0
            next_id = 1
            while rows_written < target_rows:
                # Last chunk sizing
                remaining = target_rows - rows_written
                this_chunk = CHUNK_SIZE if remaining >= CHUNK_SIZE else remaining

                records = [make_fake_record(run_dt, i) for i in range(next_id, next_id + this_chunk)]
                next_id += this_chunk

                table = pa.Table.from_pylist(records, schema=schema)
                writer.write_table(table)

                rows_written += this_chunk
                print(f"  → {rows_written:,} rows written")

        finally:
            writer.close()

    # Move tmp → final (GCS "move" is copy+delete; acceptable for landing)
    fs.move(tmp_uri, dst_uri)

    print(f"[SUCCESS] Wrote ~{rows_written:,} rows to {dst_uri}")

if __name__ == "__main__":
    main()
