#!/usr/bin/env python3
"""Dataproc PySpark: Silver -> Gold SCD2 for 10 banking tables.

Runtime configuration via command-line arguments. Uses a per-table staging table
and a BigQuery copy job (WRITE_TRUNCATE), not SQL MERGE. Run only one writer at a time.
"""
import argparse
import logging
from datetime import datetime, timezone

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from pyspark.sql import SparkSession, functions as F

MAX_EXPIRY_DATE = "9999-12-31 23:59:59"

TABLES = {
    "accounts": "AccountID",
    "accounttypes": "AccountTypeID",
    "branches": "BranchID",
    "cards": "CardID",
    "customers": "CustomerID",
    "employees": "EmployeeID",
    "loanpayments": "PaymentID",
    "loans": "LoanID",
    "transactions": "TransactionID",
    "transactiontypes": "TransactionTypeID",
}

SCD_COLS = {
    "record_hash", "version_number", "row_effective_date",
    "row_expiry_date", "current_row_indicator", "lst_updt_ts",
}

# These are ingestion/processing metadata, not business attributes.
# Excluding them prevents false SCD2 versions on subsequent day loads.
TECHNICAL_COLS = SCD_COLS | {
    "source_day", "source_file", "source_sha256", "record_sha256",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOG = logging.getLogger("all_tables_scd2")


def process_table(spark, client, table, pk, effective_ts, project,
                  silver_dataset, gold_dataset, region, temp_bucket):
    source = f"{project}.{silver_dataset}.{table}"
    target = f"{project}.{gold_dataset}.{table}"
    staging = f"{project}.{gold_dataset}.{table}_gold_staging"
    LOG.info("Starting %s -> %s", source, target)

    silver = spark.read.format("bigquery").option("table", source).load()
    if pk not in silver.columns:
        raise ValueError(f"{source}: missing primary key {pk}")

    # Gold owns SCD audit columns; source metadata must not affect the hash.
    silver = silver.drop(*[c for c in TECHNICAL_COLS if c in silver.columns])
    business_cols = sorted(c for c in silver.columns if c != pk)
    if not business_cols:
        raise ValueError(f"{source}: no business attributes available")

    if silver.filter(F.col(pk).isNull()).limit(1).count():
        raise ValueError(f"{source}: NULL {pk}")
    if silver.groupBy(pk).count().filter(F.col("count") > 1).limit(1).count():
        raise ValueError(f"{source}: duplicate {pk}")

    hash_input = F.to_json(
        F.struct(*[F.col(c).alias(c) for c in business_cols]),
        options={"ignoreNullFields": "false"},
    )
    silver = silver.withColumn("record_hash", F.sha2(hash_input, 256))
    max_expiry = F.lit(MAX_EXPIRY_DATE).cast("timestamp")

    try:
        client.get_table(target)
        gold_exists = True
    except NotFound:
        gold_exists = False

    if not gold_exists:
        result = (
            silver.withColumn("version_number", F.lit(1))
            .withColumn("row_effective_date", effective_ts)
            .withColumn("row_expiry_date", max_expiry)
            .withColumn("current_row_indicator", F.lit("Y"))
            .withColumn("lst_updt_ts", effective_ts)
        )
    else:
        gold = spark.read.format("bigquery").option("table", target).load()
        expected = set(silver.columns) | {
            "version_number", "row_effective_date", "row_expiry_date",
            "current_row_indicator", "lst_updt_ts",
        }
        if set(gold.columns) != expected:
            raise ValueError(
                f"{target}: schema mismatch. "
                f"Missing in Gold: {sorted(expected - set(gold.columns))}; "
                f"extra in Gold: {sorted(set(gold.columns) - expected)}"
            )

        current = gold.filter(F.col("current_row_indicator") == "Y")
        history = gold.filter(F.col("current_row_indicator") == "N")
        invalid_flags = gold.filter(
            F.col("current_row_indicator").isNull()
            | ~F.col("current_row_indicator").isin("Y", "N")
        )
        if invalid_flags.limit(1).count():
            raise ValueError(f"{target}: invalid current_row_indicator")
        if current.groupBy(pk).count().filter(F.col("count") > 1).limit(1).count():
            raise ValueError(f"{target}: multiple current rows for a primary key")
        if gold.filter(F.col(pk).isNull()).limit(1).count():
            raise ValueError(f"{target}: NULL primary key")

        comparison = silver.alias("s").join(
            current.select(pk, "record_hash", "version_number").alias("g"),
            on=pk, how="left",
        )
        changed = comparison.filter(
            F.col("g.version_number").isNull()
            | ~F.col("s.record_hash").eqNullSafe(F.col("g.record_hash"))
        ).select(
            "s.*",
            F.coalesce(F.col("g.version_number"), F.lit(0)).alias("previous_version"),
        )
        changed_ids = changed.select(pk).distinct()
        expired = (
            current.join(changed_ids, pk, "inner")
            .withColumn("current_row_indicator", F.lit("N"))
            .withColumn("row_expiry_date", effective_ts)
            .withColumn("lst_updt_ts", effective_ts)
        )
        unchanged = current.join(changed_ids, pk, "left_anti")
        new_versions = (
            changed.withColumn("version_number", F.col("previous_version") + 1)
            .drop("previous_version")
            .withColumn("row_effective_date", effective_ts)
            .withColumn("row_expiry_date", max_expiry)
            .withColumn("current_row_indicator", F.lit("Y"))
            .withColumn("lst_updt_ts", effective_ts)
        )
        result = (
            history.unionByName(expired)
            .unionByName(unchanged)
            .unionByName(new_versions)
        )

    # Write and copy exactly once; always attempt staging cleanup.
    try:
        (
            result.write.format("bigquery")
            .option("table", staging)
            .option("writeMethod", "indirect")
            .option("temporaryGcsBucket", temp_bucket)
            .mode("overwrite")
            .save()
        )
        copy_config = bigquery.CopyJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        )
        client.copy_table(
            staging, target, job_config=copy_config, location=region
        ).result()
        LOG.info("SCD2 load completed: %s", target)
    finally:
        client.delete_table(staging, not_found_ok=True)
        LOG.info("Deleted staging table: %s", staging)


def main():
    parser = argparse.ArgumentParser(description="BankEva Silver to Gold SCD2")
    parser.add_argument("--project", required=True)
    parser.add_argument("--silver_dataset", required=True)
    parser.add_argument("--gold_dataset", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--temp_bucket", required=True)
    args = parser.parse_args()

    client = bigquery.Client(project=args.project)
    dataset = bigquery.Dataset(f"{args.project}.{args.gold_dataset}")
    dataset.location = args.region
    client.create_dataset(dataset, exists_ok=True)
    actual = client.get_dataset(f"{args.project}.{args.gold_dataset}")
    if actual.location.lower() != args.region.lower():
        raise ValueError(f"Gold dataset is in {actual.location}, expected {args.region}")

    spark = (
        SparkSession.builder.appName("BankingSilverToGoldAllTablesSCD2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    effective_ts = F.lit(datetime.now(timezone.utc)).cast("timestamp")
    try:
        for table, pk in TABLES.items():
            process_table(
                spark, client, table, pk, effective_ts, args.project,
                args.silver_dataset, args.gold_dataset, args.region, args.temp_bucket,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
