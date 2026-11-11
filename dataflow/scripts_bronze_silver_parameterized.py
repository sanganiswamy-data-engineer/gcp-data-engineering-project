#!/usr/bin/env python3
"""Dataproc PySpark: BigQuery Bronze -> Silver for 10 banking tables.

Requires Spark BigQuery connector and google-cloud-bigquery.
Silver is a full-refresh snapshot: latest valid record per business key, ordered
by Dataflow's source_day (Day 2 > Day 1 > Day 0).
Run Gold after each day's Silver refresh to capture SCD2 history.
"""
import argparse
import logging

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from pyspark.sql import SparkSession, Window, functions as F

LOG = logging.getLogger("bronze_to_silver")

TABLES = {
    "accounts": {"pk": "AccountID", "dates": ["OpenDate", "CloseDate"], "numeric": ["CurrentBalance"]},
    "accounttypes": {"pk": "AccountTypeID", "dates": [], "numeric": ["InterestRate", "MinBalance", "OverdraftLimit"]},
    "branches": {"pk": "BranchID", "dates": ["OpenDate"], "numeric": []},
    "cards": {"pk": "CardID", "dates": ["IssueDate", "ExpiryDate"], "numeric": []},
    "customers": {"pk": "CustomerID", "dates": ["DateOfBirth", "CreatedDate"], "numeric": []},
    "employees": {"pk": "EmployeeID", "dates": ["HireDate"], "numeric": []},
    "loanpayments": {"pk": "PaymentID", "dates": ["DueDate", "PaymentDate"], "numeric": ["AmountDue", "AmountPaid"]},
    "loans": {"pk": "LoanID", "dates": ["StartDate"], "numeric": ["PrincipalAmount", "InterestRate", "TermMonths"]},
    "transactions": {"pk": "TransactionID", "dates": ["TransactionDateTime"], "numeric": ["Amount", "BalanceAfter"]},
    "transactiontypes": {"pk": "TransactionTypeID", "dates": [], "numeric": []},
}
INTEGER_COLUMNS = {"TermMonths"}
TIMESTAMP_COLUMNS = {"TransactionDateTime"}


def ensure_dataset(project, dataset_id, location):
    client = bigquery.Client(project=project)
    dataset_ref = f"{project}.{dataset_id}"
    try:
        existing = client.get_dataset(dataset_ref)
        LOG.info("Dataset already exists: %s (location=%s)", dataset_ref, existing.location)
        if existing.location.lower() != location.lower():
            raise ValueError(
                f"Dataset {dataset_ref} exists in {existing.location}, not {location}. "
                "Choose the matching location; datasets cannot be relocated in place."
            )
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        created = client.create_dataset(dataset, exists_ok=True)
        LOG.info("Dataset ready: %s (location=%s)", dataset_ref, created.location)


def parsed_date(column_name):
    """Preserve calendar date from date-only and ISO-8601 timestamp strings."""
    source = F.col(column_name)
    token = F.regexp_extract(source, r"^(\d{4}-\d{2}-\d{2})(?:$|[Tt ][0-9])", 1)
    return F.to_date(F.when(token != "", token))


def parsed_timestamp(column_name):
    return F.expr(f"try_cast(`{column_name}` as timestamp)")


def transform(spark, project, bronze, silver, table, temp_bucket):
    spec = TABLES[table]
    src = f"{project}.{bronze}.{table}"
    dst = f"{project}.{silver}.{table}"
    LOG.info("Reading %s", src)
    df = spark.read.format("bigquery").option("table", src).load()

    required = [spec["pk"], *spec["dates"], *spec["numeric"], "source_day", "source_file"]
    missing = [name for name in required if name not in df.columns]
    if missing:
        raise ValueError(
            f"{src} missing required columns: {missing}. "
            "Run the updated Dataflow ingestion first so Bronze contains source_day/source_file."
        )

    # Normalize all source fields; empty strings become NULL.
    for name in df.columns:
        cleaned = F.trim(F.col(name).cast("string"))
        df = df.withColumn(name, F.when(cleaned == "", F.lit(None)).otherwise(cleaned))

    # Validate source metadata before selecting latest records.
    # Invalid metadata must not silently fall back to a random older record.
    day_as_int = F.expr("try_cast(`source_day` as int)")
    df = df.withColumn("__source_day_int", day_as_int)
    df = df.withColumn(
        "__metadata_valid",
        F.col("__source_day_int").between(0, 2) & F.col("source_file").isNotNull(),
    )

    # Remove exact duplicate rows only; retain distinct source-day versions.
    df = df.dropDuplicates()
    pk = spec["pk"]

    for name in spec["dates"]:
        parsed = parsed_timestamp(name) if name in TIMESTAMP_COLUMNS else parsed_date(name)
        df = df.withColumn("__valid_" + name, F.col(name).isNull() | parsed.isNotNull())
        df = df.withColumn(name, parsed)

    for name in spec["numeric"]:
        target = "bigint" if name in INTEGER_COLUMNS else "decimal(20,4)"
        parsed = F.expr(f"try_cast(`{name}` as {target})")
        df = df.withColumn("__valid_" + name, F.col(name).isNull() | parsed.isNotNull())
        df = df.withColumn(name, parsed)

    checks = [name for name in df.columns if name.startswith("__valid_")]
    validity = F.col(pk).isNotNull() & F.col("__metadata_valid")
    for check in checks:
        validity = validity & F.col(check)
    df = df.withColumn("__is_valid", validity)
    rejected = df.filter(~F.col("__is_valid")).count()
    df = df.filter(F.col("__is_valid")).drop("__is_valid", "__metadata_valid", *checks)

    # A deterministic hash breaks ties for conflicting rows within the same day.
    # If source records have a real intra-file sequence, prefer that instead.
    business_cols = sorted(c for c in df.columns if not c.startswith("__"))
    stable_hash = F.sha2(F.to_json(F.struct(*[F.col(c) for c in business_cols])), 256)
    window_spec = Window.partitionBy(pk).orderBy(
        F.col("__source_day_int").desc(),
        F.col("source_file").desc(),
        stable_hash.desc(),
    )
    df = (
        df.withColumn("__rn", F.row_number().over(window_spec))
          .filter(F.col("__rn") == 1)
          .drop("__rn", "__source_day_int")
    )

    # Keep source provenance in Bronze, not in Silver/Gold business comparison.
    df = df.drop("source_day", "source_file")
    df = df.withColumn("lst_updt_ts", F.current_timestamp())
    df = df.withColumn("current_row_indicator", F.lit("Y"))

    LOG.info("Writing %s (rejected invalid rows=%s)", dst, rejected)
    (
        df.write.format("bigquery")
          .option("table", dst)
          .option("writeMethod", "indirect")
          .mode("overwrite")
          .option("temporaryGcsBucket", temp_bucket)
          .save()
    )
    LOG.info("Completed %s", table)


def main():
    parser = argparse.ArgumentParser(description="BankEva Bronze to Silver")
    parser.add_argument("--project", required=True)
    parser.add_argument("--bronze_dataset", required=True)
    parser.add_argument("--silver_dataset", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--temp_bucket", required=True)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ensure_dataset(args.project, args.silver_dataset, args.region)
    spark = (
        SparkSession.builder
        .appName("BankingBronzeToSilver")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    try:
        for table in TABLES:
            LOG.info("Processing table: %s", table)
            transform(spark, args.project, args.bronze_dataset, args.silver_dataset, table, args.temp_bucket)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
