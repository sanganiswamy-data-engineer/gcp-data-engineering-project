"""Direct Python GCS-to-BigQuery Bronze ingestion, callable by Airflow PythonOperator.

This script:
- Reads interleaved source files from GCS.
- Parses HDR/DR sections and FTR record counts.
- Validates records and quarantines rejected rows.
- Rebuilds Bronze tables and audit tables in BigQuery.
- Prints file-level diagnostics to Cloud Composer task logs.

IMPORTANT:
1. Confirm the exact GCS object names in config.yaml before running.
2. main(2) processes Day 0, Day 1, and Day 2.
3. WRITE_TRUNCATE replaces the contents of each target table on each run.
"""

import csv
import hashlib
import io
import json
import re
from collections import defaultdict
from datetime import datetime
import argparse
import logging
import os
import yaml

from google.cloud import bigquery, storage


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Deployment configuration is loaded at runtime from YAML.
DEFAULT_CONFIG_PATH = os.environ.get("BANKEVA_CONFIG", "config.yaml")

KEYS = {
    "BRANCHES": "BranchID",
    "EMPLOYEES": "EmployeeID",
    "CUSTOMERS": "CustomerID",
    "ACCOUNTTYPES": "AccountTypeID",
    "ACCOUNTS": "AccountID",
    "CARDS": "CardID",
    "TRANSACTIONTYPES": "TransactionTypeID",
    "TRANSACTIONS": "TransactionID",
    "LOANS": "LoanID",
    "LOANPAYMENTS": "PaymentID",
}

DATES = {
    "DateOfBirth", "CreatedDate", "OpenDate", "CloseDate", "HireDate",
    "IssueDate", "ExpiryDate", "TransactionDateTime", "StartDate",
    "DueDate", "PaymentDate",
}

NUMBERS = {
    "CurrentBalance", "Amount", "BalanceAfter", "InterestRate",
    "MinBalance", "OverdraftLimit", "PrincipalAmount", "AmountDue",
    "AmountPaid",
}


# ---------------------------------------------------------------------------
# Parsing and validation
# ---------------------------------------------------------------------------

def parse(text):
    """Parse HDR/DR sections, FTR row counts, and physical multiline rows."""
    sections = {}
    entity = None
    columns = []
    pending = []

    def bad_pending():
        nonlocal pending
        if pending and entity:
            sections[entity]["bad"].append(("INCOMPLETE_ROW", pending[:]))
        pending = []

    for line in text.splitlines():
        if not line.strip():
            continue

        if line.startswith(("HDR|", "DR|")):
            bad_pending()
            entity = line.split("|", 1)[1].strip().upper()
            columns = []

            if entity in sections:
                raise ValueError(f"Repeated section {entity}")

            sections[entity] = {
                "columns": [],
                "rows": [],
                "bad": [],
                "footer": None,
            }
            continue

        if line.startswith("FTR|"):
            bad_pending()
            if entity:
                parts = line.split("|")
                if len(parts) == 3 and parts[1] == "RECORDCOUNT":
                    try:
                        sections[entity]["footer"] = int(parts[2])
                    except ValueError as exc:
                        raise ValueError(
                            f"Invalid RECORDCOUNT footer for {entity}: {parts[2]}"
                        ) from exc
            entity = None
            columns = []
            continue

        if not entity:
            continue

        if not columns:
            columns = line.split("|")
            sections[entity]["columns"] = columns
            continue

        parts = line.split("|")

        if not pending and len(parts) == len(columns):
            sections[entity]["rows"].append(parts)
            continue

        if not pending and len(parts) < len(columns):
            pending = parts
            continue

        if pending:
            combined = pending[:-1] + [pending[-1] + " " + parts[0]] + parts[1:]

            if len(combined) == len(columns):
                sections[entity]["rows"].append(combined)
                pending = []
            elif len(combined) < len(columns):
                pending = combined
            else:
                sections[entity]["bad"].append(("COLUMN_COUNT", combined))
                pending = []
        else:
            sections[entity]["bad"].append(("COLUMN_COUNT", parts))

    bad_pending()
    return sections


def validate(entity, record):
    errors = []
    key = KEYS.get(entity)

    if not key:
        errors.append("UNKNOWN_ENTITY")
    elif not record.get(key, "").strip():
        errors.append("MISSING_BUSINESS_KEY")

    for column in DATES.intersection(record):
        value = record[column].strip()
        if value:
            try:
                datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                errors.append("INVALID_DATE:" + column)

    for column in NUMBERS.intersection(record):
        value = record[column].strip()
        if value:
            try:
                float(value)
            except ValueError:
                errors.append("INVALID_NUMBER:" + column)

    email = record.get("Email", "").strip()
    if entity == "CUSTOMERS" and email:
        if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email):
            errors.append("INVALID_EMAIL")

    open_date = record.get("OpenDate", "").strip()
    close_date = record.get("CloseDate", "").strip()
    if entity == "ACCOUNTS" and open_date and close_date and close_date < open_date:
        errors.append("CLOSE_BEFORE_OPEN")

    return errors


# ---------------------------------------------------------------------------
# Main ingestion
# ---------------------------------------------------------------------------


# Same input source paths and 10 business keys as the existing pipeline.
# For Day 3+, provide an additional file path using --file or BANK_EVA_DAY3_FILE.
def load_config(config_path):
    config_path = str(config_path)
    """Read YAML from Cloud Shell/local filesystem or gs:// for Composer."""
    if config_path.startswith("gs://"):
        location = config_path[5:]
        bucket_name, sep, object_name = location.partition("/")
        if not sep or not object_name:
            raise ValueError("Invalid GCS config URI: " + config_path)
        content = storage.Client().bucket(bucket_name).blob(object_name).download_as_text()
    else:
        with open(config_path, "r", encoding="utf-8") as handle:
            content = handle.read()
    config = yaml.safe_load(content)
    if not isinstance(config, dict):
        raise ValueError("config.yaml must contain a YAML mapping")
    required = {
        "gcp": ("project_id", "region"),
        "storage": ("source_bucket", "quarantine_bucket"),
        "bigquery": ("bronze_dataset", "control_dataset", "silver_dataset", "gold_dataset"),
        "pipeline": ("input_files",),
    }
    for section, keys in required.items():
        if not isinstance(config.get(section), dict):
            raise ValueError(f"Missing YAML section: {section}")
        for key in keys:
            if not config[section].get(key):
                raise ValueError(f"Missing config value: {section}.{key}")
    files = config["pipeline"]["input_files"]
    if not isinstance(files, list) or not all(
        isinstance(item, str) and item and not item.startswith("gs://") for item in files
    ):
        raise ValueError("pipeline.input_files must be a list of relative GCS paths")
    return config


def _file_paths(day, files, extra_files=None):
    paths = list(files)
    if extra_files:
        paths.extend(extra_files)
    if day < 0 or day >= len(paths):
        raise ValueError(f"Day {day} not configured; configured days: 0..{len(paths)-1}")
    return paths[:day + 1]


def _reject(day, entity, key, reason, uri, row_no, data):
    return dict(
        batch_day=day, entity_name=entity, business_key=str(key),
        reason=reason, source_uri=uri, source_row=row_no,
        rejected_json=json.dumps(data, sort_keys=True, default=str)
    )


def _as_of(items, key, day):
    # Deterministic latest record available on or before a source day.
    return {
        row[key]: row
        for row in sorted(
            (r for r in items if r["source_day"] <= day),
            key=lambda r: (r["source_day"], r["source_file"])
        )
    }


def _load_rows(bq, table_id, rows, schema, region):
    # WRITE_TRUNCATE gives a cumulative snapshot, just like the original Dataflow load.
    config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )
    if rows:
        bq.load_table_from_json(rows, table_id, job_config=config).result()
    else:
        # Create/replace an empty table without relying on an empty JSON load.
        bq.query(
            "CREATE OR REPLACE TABLE `" + table_id + "` ("
            + ", ".join(f"`{field.name}` {field.field_type}" for field in schema)
            + ")",
            location=region
        ).result()
    logging.info("Loaded %s: %d records", table_id, len(rows))


def run_ingestion(day, config_path=None, extra_files=None, **kwargs):
    """Cloud Shell: python3 ingestion.py 2; Airflow: python_callable=run_ingestion."""
    config_path = config_path or DEFAULT_CONFIG_PATH
    config = load_config(config_path)
    day = int(day)
    selected = _file_paths(day, config["pipeline"]["input_files"], extra_files=extra_files)
    project = kwargs.get("project") or config["gcp"]["project_id"]
    bucket = kwargs.get("bucket") or config["storage"]["source_bucket"]
    bronze = kwargs.get("bronze_dataset") or config["bigquery"]["bronze_dataset"]
    control = kwargs.get("control_dataset") or config["bigquery"]["control_dataset"]
    quarantine_bucket = kwargs.get("quarantine_bucket") or config["storage"]["quarantine_bucket"]
    region = kwargs.get("region") or config["gcp"]["region"]
    logging.info("Day %d: GCS -> BigQuery using Python (not Dataflow)", day)

    gcs = storage.Client(project=project)
    bq = bigquery.Client(project=project, location=region)
    rows = defaultdict(list)
    schemas = {}
    rejects = []
    warnings = []
    audits = []
    file_metadata = []

    # Read and validate ALL source files before modifying Bronze.
    for source_day, path in enumerate(selected):
        uri = f"gs://{bucket}/{path}"
        blob = gcs.bucket(bucket).blob(path)
        if not blob.exists():
            raise FileNotFoundError(uri)
        raw = blob.download_as_bytes()
        sha = hashlib.sha256(raw).hexdigest()
        sections = parse(raw.decode("utf-8-sig"))
        if not sections:
            raise ValueError(f"No entity sections in {uri}")
        for entity, section in sections.items():
            if entity not in KEYS:
                raise ValueError(f"Unknown entity {entity} in {uri}")
            columns = section["columns"]
            if not columns or KEYS[entity] not in columns:
                raise ValueError(f"Missing header/key for {entity} in {uri}")
            if entity in schemas and schemas[entity] != columns:
                raise ValueError(f"Schema changed for {entity} in {uri}")
            schemas[entity] = columns
            actual = len(section["rows"]) + len(section["bad"])
            if section["footer"] is not None and section["footer"] != actual:
                raise ValueError(f"Footer mismatch for {entity} in {uri}: "
                                 f"{section['footer']} != {actual}")
            seen = set()
            for row_number, values in enumerate(section["rows"], 1):
                record = dict(zip(columns, values))
                key = record.get(KEYS[entity], "")
                errors = validate(entity, record)
                if key in seen:
                    errors.append("DUPLICATE_KEY_IN_FILE")
                seen.add(key)
                if errors:
                    rejects.append(_reject(source_day, entity, key, ";".join(errors),
                                           uri, row_number, record))
                else:
                    record["source_day"] = source_day
                    record["source_file"] = uri
                    rows[entity].append(record)
            for reason, values in section["bad"]:
                rejects.append(_reject(source_day, entity, "", reason, uri, None, values))
        file_metadata.append(dict(batch_day=source_day, source_uri=uri, source_sha256=sha))
        logging.info("Parsed %s (%d bytes)", uri, len(raw))

    # Match the established Dataflow rules: validate references as of each source day.
    valid_accounts = []
    for row in rows["ACCOUNTS"]:
        source_day = row["source_day"]
        branch = _as_of(rows["BRANCHES"], "BranchID", source_day).get(row.get("BranchID", ""))
        types = _as_of(rows["ACCOUNTTYPES"], "AccountTypeID", source_day)
        errors = []
        if branch is None:
            errors.append("INVALID_BRANCH_ID")
        else:
            prefix = branch.get("SortCodePrefix", "")
            if prefix and not row.get("SortCode", "").startswith(prefix):
                warnings.append(_reject(source_day, "ACCOUNTS", row.get("AccountID", ""),
                                        "BRANCH_SORT_CODE_PREFIX", row["source_file"], None, row))
        if row.get("AccountTypeID", "") not in types:
            errors.append("INVALID_ACCOUNT_TYPE")
        if errors:
            rejects.append(_reject(source_day, "ACCOUNTS", row.get("AccountID", ""),
                                   ";".join(errors), row["source_file"], None, row))
        else:
            valid_accounts.append(row)
    rows["ACCOUNTS"] = valid_accounts

    valid_transactions = []
    for row in rows["TRANSACTIONS"]:
        source_day = row["source_day"]
        account = _as_of(valid_accounts, "AccountID", source_day).get(row.get("AccountID", ""))
        types = _as_of(rows["TRANSACTIONTYPES"], "TransactionTypeID", source_day)
        errors = []
        if account is None:
            errors.append("INVALID_ACCOUNT_ID")
        elif row.get("Currency") != account.get("Currency"):
            errors.append("CURRENCY_MISMATCH")
        if row.get("TransactionTypeID", "") not in types:
            errors.append("INVALID_TRANSACTION_TYPE")
        if errors:
            rejects.append(_reject(source_day, "TRANSACTIONS", row.get("TransactionID", ""),
                                   ";".join(errors), row["source_file"], None, row))
        else:
            valid_transactions.append(row)
    rows["TRANSACTIONS"] = valid_transactions

    for meta in file_metadata:
        uri = meta["source_uri"]
        meta["accepted"] = sum(
            1 for entity_rows in rows.values() for r in entity_rows
            if r["source_file"] == uri
        )
        meta["rejected"] = sum(1 for r in rejects if r["source_uri"] == uri)
        meta["status"] = "SUCCESS"
        audits.append(meta)
        logging.info("AUDIT day=%d accepted=%d rejected=%d", meta["batch_day"],
                     meta["accepted"], meta["rejected"])

    for dataset in (bronze, control):
        ds = bigquery.Dataset(f"{project}.{dataset}")
        ds.location = region
        bq.create_dataset(ds, exists_ok=True)

    # Preserve source_day/source_file; do not add hash columns to business schema.
    for entity, columns in schemas.items():
        schema = ([bigquery.SchemaField(c, "STRING") for c in columns]
                  + [bigquery.SchemaField("source_day", "INTEGER"),
                     bigquery.SchemaField("source_file", "STRING")])
        _load_rows(bq, f"{project}.{bronze}.{entity.lower()}", rows[entity], schema, region)

    reject_schema = [
        bigquery.SchemaField("batch_day", "INTEGER"),
        bigquery.SchemaField("entity_name", "STRING"),
        bigquery.SchemaField("business_key", "STRING"),
        bigquery.SchemaField("reason", "STRING"),
        bigquery.SchemaField("source_uri", "STRING"),
        bigquery.SchemaField("source_row", "INTEGER"),
        bigquery.SchemaField("rejected_json", "STRING"),
    ]
    audit_schema = [
        bigquery.SchemaField("batch_day", "INTEGER"),
        bigquery.SchemaField("source_uri", "STRING"),
        bigquery.SchemaField("source_sha256", "STRING"),
        bigquery.SchemaField("accepted", "INTEGER"),
        bigquery.SchemaField("rejected", "INTEGER"),
        bigquery.SchemaField("status", "STRING"),
    ]
    _load_rows(bq, f"{project}.{control}.quarantine_audit", rejects, reject_schema, region)
    _load_rows(bq, f"{project}.{control}.warning_audit", warnings, reject_schema, region)
    _load_rows(bq, f"{project}.{control}.file_audit", audits, audit_schema, region)

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=[f.name for f in reject_schema])
    writer.writeheader()
    writer.writerows(rejects)
    gcs.bucket(quarantine_bucket).blob(
        f"rejected_through_day{day}.csv"
    ).upload_from_string(output.getvalue(), content_type="text/csv")
    logging.info("SUCCESS Day %d; total accepted=%d rejected=%d warnings=%d",
                 day, sum(a["accepted"] for a in audits), len(rejects), len(warnings))
    return {"day": day, "accepted": sum(a["accepted"] for a in audits),
            "rejected": len(rejects), "warnings": len(warnings)}


def main(day, **kwargs):
    return run_ingestion(day, **kwargs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BankEva direct Python Bronze ingestion")
    parser.add_argument("day", type=int, help="0, 1, 2, or another day listed in YAML")
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH, help="Local path or gs:// URI")
    parser.add_argument("--file", action="append", dest="extra_files", default=None,
                        help="Additional GCS object path relative to source bucket")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main(args.day, config_path=args.config, extra_files=args.extra_files)
