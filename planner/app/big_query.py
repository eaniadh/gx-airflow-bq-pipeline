""" BigQuery helpers for DDL, config loads, and partition checks."""
import os
from google.cloud import bigquery

DDL = """
CREATE SCHEMA IF NOT EXISTS `{p}.ops`;
CREATE TABLE IF NOT EXISTS `{p}.ops.config_job_cadence` (
  job_name STRING, cadence STRING, expected_partition_field STRING, owner STRING, active BOOL DEFAULT TRUE
);
CREATE TABLE IF NOT EXISTS `{p}.ops.task_map` (
  job_name STRING, dag_id STRING, task_id STRING, notes STRING
);
CREATE TABLE IF NOT EXISTS `{p}.ops.backfill_plan` (
  plan_id STRING, requested_date DATE, root_bronze_table STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, created_by STRING, status STRING
);
CREATE TABLE IF NOT EXISTS `{p}.ops.backfill_plan_items` (
  plan_id STRING, downstream_table STRING, producing_job STRING, missing_partitions ARRAY<DATE>,
  recommended_cmd STRING, reason STRING, priority INT64, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

def _normalize_fqn(fqn: str) -> str:
    # "bigquery:proj.ds.tbl" → "proj.ds.tbl"
    return fqn.split(":", 1)[-1] if ":" in fqn else fqn

class BQ:
    def __init__(self, project: str):
        self.project = project
        location = os.environ.get("BQ_LOCATION", "US")
        self.client = bigquery.Client(project=project, location=location)

    def ensure_ops_tables(self):
        for stmt in [s for s in DDL.format(p=self.project).split(";") if s.strip()]:
            self.client.query(stmt).result()

    def load_cadence(self):
        q = f"""
        SELECT job_name, cadence, expected_partition_field, owner, active
        FROM `{self.project}.ops.config_job_cadence`
        WHERE active = TRUE
        """
        return {
            r["job_name"]: {
                "cadence": r["cadence"],
                # treat NULL/empty as no native partition
                "partition_field": (r["expected_partition_field"] or "").strip(),
                "owner": r["owner"],
            }
            for r in self.client.query(q).result()
        }

    def load_taskmap(self):
        q = f"SELECT job_name, dag_id, task_id FROM `{self.project}.ops.task_map`"
        return {r["job_name"]: {"dag_id": r["dag_id"], "task_id": r["task_id"]}
                for r in self.client.query(q).result()}

    def insert_plan_header(self, plan_id, requested_date, root_bronze_table, created_by):
        root_bronze_table = _normalize_fqn(root_bronze_table)
        q = f"""
        INSERT INTO `{self.project}.ops.backfill_plan`
          (plan_id, requested_date, root_bronze_table, created_by, status)
        VALUES (@id, @d, @root, @by, 'PLANNED')
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("id", "STRING", plan_id),
            bigquery.ScalarQueryParameter("d", "DATE", requested_date),
            bigquery.ScalarQueryParameter("root", "STRING", root_bronze_table),
            bigquery.ScalarQueryParameter("by", "STRING", created_by),
        ])
        self.client.query(q, job_config=cfg).result()

    def insert_plan_item(self, plan_id, downstream_table, producing_job, missing_parts, recommended_cmd, reason, priority):
        downstream_table = _normalize_fqn(downstream_table)
        q = f"""
        INSERT INTO `{self.project}.ops.backfill_plan_items`
          (plan_id, downstream_table, producing_job, missing_partitions, recommended_cmd, reason, priority)
        VALUES (@id, @tbl, @job, @parts, @cmd, @why, @pri)
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("id", "STRING", plan_id),
            bigquery.ScalarQueryParameter("tbl", "STRING", downstream_table),
            bigquery.ScalarQueryParameter("job", "STRING", producing_job),
            bigquery.ArrayQueryParameter("parts", "DATE", missing_parts or []),
            bigquery.ScalarQueryParameter("cmd", "STRING", recommended_cmd),
            bigquery.ScalarQueryParameter("why", "STRING", reason),
            bigquery.ScalarQueryParameter("pri", "INT64", priority),
        ])
        self.client.query(q, job_config=cfg).result()
    
    def insert_taskmap_stub(self, job_name: str, dag_id: str | None, task_id: str | None, note: str | None = "stubbed by planner"):
        """
        Upsert a stub row in ops.task_map keyed by job_name.
        Only uses columns that exist in your current DDL: job_name, dag_id, task_id, notes.
        """
        sql = f"""
        MERGE `{self.project}.ops.task_map` T
        USING (SELECT @job_name AS job_name) S
        ON T.job_name = S.job_name
        WHEN NOT MATCHED THEN
        INSERT(job_name, dag_id, task_id, notes)
        VALUES(@job_name, @dag_id, @task_id, @notes)
        WHEN MATCHED THEN
        UPDATE SET
            -- keep existing dag_id/task_id if present; otherwise set from stub
            dag_id = COALESCE(T.dag_id, @dag_id),
            task_id = COALESCE(T.task_id, @task_id),
            notes = COALESCE(T.notes, @notes)
        """
        self.client.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("job_name", "STRING", job_name),
                    bigquery.ScalarQueryParameter("dag_id", "STRING", dag_id),
                    bigquery.ScalarQueryParameter("task_id", "STRING", task_id),
                    bigquery.ScalarQueryParameter("notes", "STRING", note),
                ]
            )
        ).result()

    # ---------- Partition existence helpers ----------

    def _table_exists(self, fqn: str) -> bool:
        try:
            self.client.get_table(fqn)
            return True
        except Exception:
            return False

    def _table_type(self, fqn: str) -> str | None:
        try:
            return self.client.get_table(fqn).table_type  # "EXTERNAL", "TABLE", etc.
        except Exception:
            return None

    def _has_hive_partition_by_filename(self, fqn: str, date_str: str) -> bool:
        # For external hive-style partitions: gs://.../date=YYYY-MM-DD/...
        sql = f"""
        SELECT 1
        FROM `{fqn}`
        WHERE REGEXP_EXTRACT(_FILE_NAME, r'date=([0-9-]+)') = @d
        LIMIT 1
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("d", "STRING", date_str)
        ])
        rows = list(self.client.query(sql, job_config=cfg).result())
        return len(rows) > 0

    def _has_partition_in_information_schema(self, fqn: str, yyyymmdd: str) -> bool:
        proj, ds, tbl = fqn.split(".")
        sql = f"""
        SELECT 1
        FROM `{proj}.{ds}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE table_name = @tbl
          AND partition_id = @pid
        LIMIT 1
        """
        cfg = bigquery.QueryJobConfig(query_parameters=[
            bigquery.ScalarQueryParameter("tbl", "STRING", tbl),
            bigquery.ScalarQueryParameter("pid", "STRING", yyyymmdd),
        ])
        rows = list(self.client.query(sql, job_config=cfg).result())
        return len(rows) > 0

    def partition_exists(self, fqn: str, partition_field: str | None, run_date: str) -> bool:
        """
        Returns True if data for run_date exists for:
          - EXTERNAL hive-style table (by _FILE_NAME date=YYYY-MM-DD),
          - native partitioned table (by INFORMATION_SCHEMA.PARTITIONS),
          - non-partitioned table (always True).
        """
        fqn = _normalize_fqn(fqn)
        if not self._table_exists(fqn):
            return False

        ttype = (self._table_type(fqn) or "").upper()

        # EXTERNAL: check by file path date
        if ttype == "EXTERNAL":
            try:
                return self._has_hive_partition_by_filename(fqn, run_date)
            except Exception:
                # Fail closed for external tables: if probe errors, assume partition is missing
                return False

        # Native partitions via INFORMATION_SCHEMA (regardless of partition_field)
        try:
            if self._has_partition_in_information_schema(fqn, run_date.replace("-", "")):
                return True
        except Exception:
            # Some table types won't expose PARTITIONS; ignore
            pass

        # Non-partitioned: treat as available
        return True
