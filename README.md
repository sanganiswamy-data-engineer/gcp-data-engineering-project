# gcp-data-engineering-project
Gooogle Cloud Data engineering modules 

# BankEva — mandatory Dataproc Serverless PySpark, Bronze → Silver SCD2

**Project:** `retail-omkar-cli-org-dev`  
**Region:** `us-central1` (change if Bronze is elsewhere)  
**Source:** `bankeva_bronze` (10 tables)  
**Target:** `bankeva_silver` (`*_clean` SCD2 tables)

## Architecture
GCS CSV → existing ingestion → BigQuery Bronze → **Dataproc Serverless PySpark** → BigQuery `_stg_*_scd2` → BigQuery `MERGE` → Silver `*_clean` → Dataform Gold.

This satisfies the mandatory Dataproc + PySpark processing requirement. Dataform can own Gold models. No `DECLARE`, `${ref()}`, or `${vars}` is used in the Silver merge.

## Cloud Shell copy/paste

1. Upload/unzip this package into Cloud Shell, then `cd bankeva_dataproc_scd2`.
2. Choose your existing GCS bucket and **real** Day 0 date:

```bash
export PROJECT=retail-omkar-cli-org-dev
export REGION=us-central1
export PIPELINE_BUCKET=YOUR_EXISTING_GCS_BUCKET
export DAY_ZERO_DATE=2026-10-09   # CHANGE to the real source_day=0 date
bash orchestration/setup.sh
bash orchestration/run_all.sh
```

Do not run the old Dataform `type: "table"` `*_clean.sqlx` models after this job; they overwrite SCD2 history. Remove/disable those old definitions or rename them. Gold Dataform models should read the Silver `*_clean` tables and filter `cur_row_indicator='Y'` for current-only analytics.

## What the PySpark job does
- Reads each Bronze BigQuery table via the Dataproc Spark BigQuery connector.
- Trims whitespace from business STRING fields.
- Deduplicates by **business key + source_day**.
- Computes SHA256 over non-metadata business attributes.
- Removes consecutive unchanged snapshots using `LAG`.
- Calculates `row_effective_date`, `row_expiry_date` (exclusive), `cur_row_indicator`.
- Writes staged historical versions to BigQuery.
- `merge_scd2.py` generates **explicit BigQuery MERGE** UPDATE SET / INSERT VALUES lists based on actual BigQuery schema; it does not use BigQuery `DECLARE`.

## Known prerequisites and limitations
- **Historical snapshots must be retained in Bronze**. If Bronze is WRITE_TRUNCATE to only the latest day, this cannot reconstruct prior versions. Existing ingestion must preserve Day 0..N records for this approach.
- The Day 0 date is mandatory; `source_day` 0, 1, 2 is mapped to actual dates from that value. Do not arbitrarily change it between reruns.
- Business keys are assumed: `AccountID`, `AccountTypeID`, `BranchID`, `CardID`, `CustomerID`, `EmployeeID`, `PaymentID`, `LoanID`, `TransactionID`, `TransactionTypeID`. Confirm against the actual nine schemas (only accounts schema was supplied).
- This is **full-history version reconciliation**, rerun-safe for unchanged historical snapshots, not optimized to scan only newly landed rows. It preserves old history and updates matching versions. Corrections removing previously known versions are not automatically deleted.
- Existing Silver `*_clean` tables created by `SELECT *` lack SCD columns. **Back them up and migrate/drop/recreate in a safe environment before running**. The MERGE script detects a mismatched schema and stops rather than silently corrupting data.
- If business keys are nullable, null/empty keys are skipped. If duplicate records have equal tie-breakers but different business attributes, choose a source-specific deterministic tie-breaker.
- The PySpark job currently preserves business column STRING types (except trimming); SCD2 and normalization are real transformations. Extend column-specific casts once remaining schemas are confirmed.
- Requires Google Cloud billing, Dataproc API, permission to create a service account / IAM bindings, BigQuery dataset access, GCS access, and the ability to submit Dataproc Serverless batches. Service account needs network access to Google APIs and Dataproc runtime prerequisites.

## Validate
Run `sql/validation.sql` in BigQuery. Expected duplicate-current query: zero rows.
