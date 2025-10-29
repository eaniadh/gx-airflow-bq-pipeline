# 🧠 GX + Airflow + BigQuery Pipeline (SCD-II Dim Customers)

End-to-end data-quality and SCD-II pipeline built on **Google Cloud Platform** using  
**Great Expectations**, **Apache Airflow**, and **BigQuery** — orchestrated via Docker Compose.

---

## 🚀 Architecture

**GCS → BigQuery (External Table → Validated Slice) → Airflow → SCD-II Merge to Silver**

1. **Great Expectations Validation**
   - Runs inside Airflow worker (`worker-gx-bq-pipeline`)
   - Validates Bronze external table partitions
   - Writes results to `gx_validation_results`
   - Publishes `_MANIFEST.json` + `_SUCCESS` to GCS

2. **Bronze → Silver Promotion**
   - Triggered by manifest success
   - Executes parameterized Jinja2 SQL merge (`silver_scd2_merge.sql.j2`)
   - Maintains historical records via `eff_from`, `eff_to`, `is_current`

3. **Orchestration**
   - Airflow DAG Factory for GX pipelines  
   - Automatic downstream trigger to Silver DAG  
   - Configurable via `.env` variables

---

## 🧱 Project Structure
```text
dags/
├─ gx_pipeline_factory.py
├─ gx_pipeline__orders.py
├─ silver_scd2_dim_customers.py
├─ sql/silver_scd2_merge.sql.j2
docker/
├─ Dockerfile.gx_bq_pipeline
├─ Dockerfile.worker_a
gx_bq_pipeline/
├─ configs/pipelines.yml
├─ gx/expectations/...
├─ scripts/gx_validate.py
