-- Create ops schema
CREATE SCHEMA IF NOT EXISTS `alert-study-468915-n0.ops` OPTIONS(location="US");

-- Cadence and ownership per OL job
CREATE TABLE IF NOT EXISTS `alert-study-468915-n0.ops.config_job_cadence` (
  job_name STRING,
  cadence STRING,                  -- 'hourly'|'daily'|'custom'
  expected_partition_field STRING, -- e.g. '_ingestion_date'
  owner STRING,
  active BOOL DEFAULT TRUE
);

-- Map OL job -> Airflow DAG/task for precise commands
CREATE TABLE IF NOT EXISTS `alert-study-468915-n0.ops.task_map` (
  job_name STRING,
  dag_id STRING,
  task_id STRING,
  notes STRING
);

-- Plan header
CREATE TABLE IF NOT EXISTS `alert-study-468915-n0.ops.backfill_plan` (
  plan_id STRING,
  requested_date DATE,
  root_bronze_table STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  created_by STRING,
  status STRING                    -- 'PLANNED'|'APPROVED'|'EXECUTED'|'CANCELLED'
);

-- Plan items
CREATE TABLE IF NOT EXISTS `alert-study-468915-n0.ops.backfill_plan_items` (
  plan_id STRING,
  downstream_table STRING,
  producing_job STRING,
  missing_partitions ARRAY<DATE>,
  recommended_cmd STRING,
  reason STRING,
  priority INT64,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
