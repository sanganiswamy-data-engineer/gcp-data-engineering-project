#!/usr/bin/env python3
"""Dataproc Serverless PySpark: Bronze snapshots -> deterministic SCD2 staging."""
import argparse
import re
from pyspark.sql import SparkSession, functions as F, Window

KEYS = {
    'accounts': 'AccountID', 'accounttypes': 'AccountTypeID',
    'branches': 'BranchID', 'cards': 'CardID',
    'customers': 'CustomerID', 'employees': 'EmployeeID',
    'loanpayments': 'PaymentID', 'loans': 'LoanID',
    'transactions': 'TransactionID', 'transactiontypes': 'TransactionTypeID'
}
META = {'source_day', 'source_file', 'source_sha256', 'record_sha256'}

def qident(s):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', s):
        raise ValueError('Invalid identifier: ' + s)
    return s

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--project', required=True)
    ap.add_argument('--bronze-dataset', default='bankeva_bronze')
    ap.add_argument('--silver-dataset', default='bankeva_silver')
    ap.add_argument('--table', choices=sorted(KEYS), required=True)
    ap.add_argument('--day-zero-date', required=True, help='YYYY-MM-DD: date represented by source_day=0')
    args = ap.parse_args()
    key = KEYS[args.table]
    spark = SparkSession.builder.appName('bankeva-scd2-' + args.table).getOrCreate()
    bronze = f'{args.project}.{args.bronze_dataset}.{args.table}'
    staging = f'{args.project}.{args.silver_dataset}._stg_{args.table}_scd2'
    df = spark.read.format('bigquery').option('table', bronze).load()
    if key not in df.columns:
        raise RuntimeError(f'Missing business key {key} in {bronze}. Actual columns: {df.columns}')
    if 'source_day' not in df.columns:
        raise RuntimeError(f'Missing source_day in {bronze}')
    for c in df.columns:
        if c not in META and dict(df.dtypes)[c] == 'string':
            df = df.withColumn(c, F.trim(F.col('`' + c + '`')))
    df = df.withColumn('source_day', F.col('source_day').cast('long'))
    df = df.filter(F.col(key).isNotNull() & (F.length(F.col(key)) > 0) & F.col('source_day').isNotNull())
    # Stable dedup: at most one snapshot per business key per source day.
    order_cols = [F.coalesce(F.col(c).cast('string'), F.lit('')).desc() for c in ('source_file', 'record_sha256', 'source_sha256') if c in df.columns]
    if not order_cols:
        order_cols = [F.col(key).desc()]
    w = Window.partitionBy(key, 'source_day').orderBy(*order_cols)
    df = df.withColumn('_rn', F.row_number().over(w)).filter('_rn = 1').drop('_rn')
    business_cols = sorted(c for c in df.columns if c not in META and c != key)
    if not business_cols:
        raise RuntimeError(f'No business attributes found in {bronze}')
    # Null-safe deterministic hash, independent of ingestion metadata.
    df = df.withColumn('scd_hash', F.sha2(F.to_json(F.struct(*[F.col(c).alias(c) for c in business_cols]), options={'ignoreNullFields': 'false'}), 256))
    hist_w = Window.partitionBy(key).orderBy('source_day')
    df = df.withColumn('_previous_hash', F.lag('scd_hash').over(hist_w))
    df = df.filter(F.col('_previous_hash').isNull() | (F.col('_previous_hash') != F.col('scd_hash'))).drop('_previous_hash')
    df = df.withColumn('row_effective_date', F.date_add(F.to_date(F.lit(args.day_zero_date)), F.col('source_day').cast('int')))
    changed_w = Window.partitionBy(key).orderBy('source_day')
    df = df.withColumn('_next_date', F.lead('row_effective_date').over(changed_w))
    df = df.withColumn('row_expiry_date', F.coalesce(F.col('_next_date'), F.lit('9999-12-31').cast('date')))
    df = df.withColumn('cur_row_indicator', F.when(F.col('_next_date').isNull(), F.lit('Y')).otherwise(F.lit('N'))).drop('_next_date')
    # Real Silver transformations: canonical trim + SCD2 change detection and versioning.
    # Preserve Bronze column types/names for safe schema-compatible MERGE.
    print(f'Staging {bronze} -> {staging}; business key={key}')
    df.write.format('bigquery').option('table', staging).mode('overwrite').save()
    spark.stop()

if __name__ == '__main__':
    main()
