#!/usr/bin/env python3
"""
Upsert a BigQuery table schema into Marquez by emitting a lineage event
with a SchemaDatasetFacet on the OUTPUT dataset.

Why lineage (vs dataset API)?
- Works consistently across Marquez versions without needing extra endpoints.
- We attach the schema to an existing pipeline job (you pass --attach-to-job)
  so we *don’t* introduce any "bootstrap.*" jobs.

Usage:
  python upsert_bq_schema_to_marquez.py \
    --project alert-study-468915-n0 \
    --dataset demo_gx \
    --table orders_ext \
    --marquez-url http://marquez:5000 \
    --ol-namespace gx-bq-pipeline \
    --dataset-namespace bigquery \
    --attach-to-job gx_pipeline__orders.gx_validate.basic
"""

import os
import argparse
import uuid
from datetime import datetime, timezone

import requests
from google.cloud import bigquery
from google.oauth2 import service_account


SCHEMA_FACET_URL = "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet"


def _bq_client(project: str) -> bigquery.Client:
    key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if key_path and os.path.exists(key_path):
        creds = service_account.Credentials.from_service_account_file(
            key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        return bigquery.Client(project=project, credentials=creds)
    return bigquery.Client(project=project)


def _fetch_bq_schema(client: bigquery.Client, fqn: str):
    """
    Return list of dicts: [{"name": "...", "type": "...", "description": "... or None"}, ...]
    Handles nested RECORD fields by flattening with dot notation.
    """
    tbl = client.get_table(fqn)

    def walk(fields, prefix=""):
        out = []
        for f in fields:
            name = prefix + f.name
            out.append({
                "name": name,
                "type": f.field_type,                # e.g. STRING, INTEGER, RECORD
                "description": f.description or None
            })
            if f.field_type.upper() == "RECORD" and f.fields:
                out.extend(walk(f.fields, prefix=name + "."))
        return out

    return walk(tbl.schema)


def _emit_schema_facet_via_lineage(
    *,
    marquez_url: str,
    ol_namespace: str,
    dataset_namespace: str,
    attach_job_name: str,
    dataset_fqn: str,
    fields: list[dict]
) -> None:
    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()

    payload = {
        "eventType": "COMPLETE",
        "eventTime": now,
        "run": {"runId": run_id},
        "job": {"namespace": ol_namespace, "name": attach_job_name},
        "inputs": [],
        "outputs": [
            {
                "namespace": dataset_namespace,
                "name": dataset_fqn,
                "facets": {
                    "schema": {
                        "_producer": "cli://schema-upsert",
                        "_schemaURL": SCHEMA_FACET_URL,
                        "fields": fields
                    }
                }
            }
        ],
        "producer": "cli://schema-upsert"
    }

    url = f"{marquez_url.rstrip('/')}/api/v1/lineage"
    resp = requests.post(url, json=payload, timeout=30)
    if resp.status_code not in (200, 201, 202):
        raise RuntimeError(
            f"Marquez lineage POST failed ({resp.status_code}): {resp.text[:500]}"
        )


def main():
    ap = argparse.ArgumentParser(description="Upsert BigQuery schema into Marquez (via lineage schema facet).")
    ap.add_argument("--project", required=True, help="GCP project id")
    ap.add_argument("--dataset", required=True, help="BigQuery dataset id")
    ap.add_argument("--table", required=True, help="BigQuery table id")
    ap.add_argument("--marquez-url", default=os.getenv("MARQUEZ_URL", "http://marquez:5000"))
    ap.add_argument("--ol-namespace", default=os.getenv("OPENLINEAGE_NAMESPACE", "gx-bq-pipeline"),
                    help="OpenLineage job namespace (Marquez logical namespace for jobs)")
    ap.add_argument("--dataset-namespace", default="bigquery",
                    help="Dataset namespace in Marquez (usually 'bigquery')")
    ap.add_argument(
        "--attach-to-job",
        required=True,
        help="Attach schema facet to this existing job name (e.g. gx_pipeline__orders.gx_validate.basic)"
    )
    args = ap.parse_args()

    dataset_fqn = f"{args.project}.{args.dataset}.{args.table}"
    bq = _bq_client(args.project)
    fields = _fetch_bq_schema(bq, dataset_fqn)

    if not fields:
        raise RuntimeError(f"No fields found for {dataset_fqn}")

    _emit_schema_facet_via_lineage(
        marquez_url=args.marquez_url,
        ol_namespace=args.ol_namespace,
        dataset_namespace=args.dataset_namespace,
        attach_job_name=args.attach_to_job,
        dataset_fqn=dataset_fqn,
        fields=fields
    )
    print(f"[SchemaSync] Upserted schema for {dataset_fqn} into Marquez via job '{args.attach_to_job}'.")


if __name__ == "__main__":
    main()
