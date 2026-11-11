"""Apache Beam / Google Cloud Dataflow: cumulative, replay-safe Bronze rebuild.
Run with --day 0|1|2. Dataflow runner requires a staging/temp bucket.
File parsing is parallel by file; cross-entity referential validation is globally grouped.
"""
import argparse
import csv
import hashlib
import io
import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
from google.cloud import bigquery, storage

DEFAULT_PROJECT = 'retail-omkar-cli-org-dev'
DEFAULT_BUCKET = 'hackathon_hyderabad_swara'
DEFAULT_QUARANTINE_BUCKET = 'bankeva-quarantine-retail-omkar-cli-org-dev'
DEFAULT_REGION = 'us-central1'
DEFAULT_BRONZE = 'hack_bronze'
DEFAULT_CONTROL = 'bankeva_control'
FILES = [
 'day0/hack149_ws01_Source_data_EvalDB_Day0_Interleaved.txt',
 'day1/hack149_ws01_Source_data_EvalDB_Day1_Interleaved.txt',
 'day2/hack149_ws01_Source_data_EvalDB_Day2_Interleaved.txt',
]

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



def inspect_inputs(project, bucket, paths):
    """Driver-side preflight: validate file existence, sections, schemas and footers."""
    client = storage.Client(project=project)
    schemas = {}
    for name in paths:
        blob = client.bucket(bucket).blob(name)
        if not blob.exists():
            raise FileNotFoundError(f'gs://{bucket}/{name}')
        sections = parse(blob.download_as_bytes().decode('utf-8-sig'))
        if not sections:
            raise ValueError(f'No sections in {name}')
        for entity, section in sections.items():
            if entity not in KEYS:
                raise ValueError(f'Unknown entity: {entity}')
            cols = section['columns']
            if not cols or KEYS[entity] not in cols:
                raise ValueError(f'Missing header or business key: {entity} in {name}')
            if entity in schemas and schemas[entity] != cols:
                raise ValueError(f'Schema changed: {entity} in {name}')
            schemas[entity] = cols
            count = len(section['rows']) + len(section['bad'])
            if section['footer'] is not None and section['footer'] != count:
                raise ValueError(f'Footer mismatch: {entity} in {name}')
    return schemas


def reject(day, entity, key, reason, uri, row_no, data):
    return dict(batch_day=day, entity_name=entity, business_key=key,
                reason=reason, source_uri=uri, source_row=row_no,
                rejected_json=json.dumps(data, sort_keys=True))


class ParseFile(beam.DoFn):
    def process(self, item):
        day, uri = item
        with FileSystems.open(uri) as handle:
            raw = handle.read()
        digest = hashlib.sha256(raw).hexdigest()
        sections = parse(raw.decode('utf-8-sig'))
        if not sections:
            raise ValueError(f'No sections in {uri}')
        accepted = defaultdict(list)
        rejects = []
        for entity, section in sections.items():
            if entity not in KEYS:
                raise ValueError(f'Unknown entity {entity} in {uri}')
            columns = section['columns']
            count = len(section['rows']) + len(section['bad'])
            if section['footer'] is not None and section['footer'] != count:
                raise ValueError(f'Footer mismatch: {entity} in {uri}')
            seen = set()
            for row_no, values in enumerate(section['rows'], 1):
                record = dict(zip(columns, values))
                key = record.get(KEYS[entity], '')
                errors = validate(entity, record)
                if key in seen:
                    errors.append('DUPLICATE_KEY_IN_FILE')
                seen.add(key)
                if errors:
                    rejects.append(reject(day, entity, key, ';'.join(errors), uri, row_no, record))
                else:
                    # Keep provenance only inside the pipeline for ordering and audits.
                    record.update(source_day=day, source_file=uri)
                    accepted[entity].append(record)
            for reason, values in section['bad']:
                rejects.append(reject(day, entity, '', reason, uri, None, values))
        yield ('all', dict(rows=dict(accepted), rejects=rejects,
                           day=day, uri=uri, sha=digest))


class ValidateReferences(beam.DoFn):
    """Global reconciliation to match original cross-entity checks and audit counts."""
    def process(self, grouped):
        _, files = grouped
        rows = defaultdict(list)
        rejects = []
        file_meta = []
        for f in files:
            file_meta.append(f)
            rejects.extend(f['rejects'])
            for entity, items in f['rows'].items():
                rows[entity].extend(items)
        latest = {}
        for entity, items in rows.items():
            key = KEYS[entity]
            latest[entity] = {r[key]: r for r in sorted(items, key=lambda r: (r['source_day'], r['source_file']))}
        for entity in ('ACCOUNTS', 'TRANSACTIONS'):
            remaining = []
            for row in rows[entity]:
                errors = []
                if entity == 'ACCOUNTS':
                    branch = latest.get('BRANCHES', {}).get(row.get('BranchID', ''))
                    if not branch:
                        errors.append('INVALID_BRANCH_ID')
                    elif not row.get('SortCode', '').startswith(branch.get('SortCodePrefix', '')):
                        errors.append('BRANCH_SORT_CODE_PREFIX')
                    if row.get('AccountTypeID', '') not in latest.get('ACCOUNTTYPES', {}):
                        errors.append('INVALID_ACCOUNT_TYPE')
                else:
                    account = latest.get('ACCOUNTS', {}).get(row.get('AccountID', ''))
                    if not account:
                        errors.append('INVALID_ACCOUNT_ID')
                    elif row.get('Currency') != account.get('Currency'):
                        errors.append('CURRENCY_MISMATCH')
                    if row.get('TransactionTypeID', '') not in latest.get('TRANSACTIONTYPES', {}):
                        errors.append('INVALID_TRANSACTION_TYPE')
                if errors:
                    rejects.append(reject(row['source_day'], entity, row.get(KEYS[entity], ''),
                                          ';'.join(errors), row['source_file'], None, row))
                else:
                    remaining.append(row)
            rows[entity] = remaining
        for entity, items in rows.items():
            for item in items:
                yield beam.pvalue.TaggedOutput(entity, item)
        for r in rejects:
            yield beam.pvalue.TaggedOutput('rejects', r)
        for f in file_meta:
            day = f['day']
            yield beam.pvalue.TaggedOutput('audit', dict(
                batch_day=day, source_uri=f['uri'], source_sha256=f['sha'],
                accepted=sum(1 for rs in rows.values() for r in rs if r['source_file'] == f['uri']),
                rejected=sum(1 for r in rejects if r['source_uri'] == f['uri']),
                status='SUCCESS'))


def to_bronze_row(row):
    """Write source business fields only; keep metadata for internal checks."""
    excluded = {
        'source_day', 'source_file', 'source_sha256', 'record_sha256',
        'cur_row_in', 'last_upd',
    }
    return {key: value for key, value in row.items() if key not in excluded}


REJECT_SCHEMA = ('batch_day:INTEGER,entity_name:STRING,business_key:STRING,'
                 'reason:STRING,source_uri:STRING,source_row:INTEGER,rejected_json:STRING')
AUDIT_SCHEMA = ('batch_day:INTEGER,source_uri:STRING,source_sha256:STRING,'
                'accepted:INTEGER,rejected:INTEGER,status:STRING')
CSV_FIELDS = ['batch_day','entity_name','business_key','reason','source_uri','source_row','rejected_json']


def csv_line(row):
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=CSV_FIELDS, lineterminator='')
    writer.writerow(row)
    return stream.getvalue()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--day', type=int, choices=[0, 1, 2], default=2)
    parser.add_argument('--project', default=DEFAULT_PROJECT)
    parser.add_argument('--bucket', default=DEFAULT_BUCKET)
    parser.add_argument('--quarantine_bucket', default=DEFAULT_QUARANTINE_BUCKET)
    parser.add_argument('--region', default=DEFAULT_REGION)
    parser.add_argument('--bronze_dataset', default=DEFAULT_BRONZE)
    parser.add_argument('--control_dataset', default=DEFAULT_CONTROL)
    parser.add_argument('--temp_location', required=True)
    parser.add_argument('--staging_location', required=True)
    parser.add_argument('--runner', default='DataflowRunner')
    args, beam_args = parser.parse_known_args()
    selected = FILES[:args.day + 1]
    schemas = inspect_inputs(args.project, args.bucket, selected)
    bq = bigquery.Client(project=args.project, location=args.region)
    for dataset_name in (args.bronze_dataset, args.control_dataset):
        ds = bigquery.Dataset(f'{args.project}.{dataset_name}')
        ds.location = args.region
        bq.create_dataset(ds, exists_ok=True)
    options = PipelineOptions(beam_args, runner=args.runner, project=args.project,
                              region=args.region, temp_location=args.temp_location,
                              staging_location=args.staging_location,
                              save_main_session=True)
    options.view_as(SetupOptions).save_main_session = True
    tags = list(KEYS) + ['rejects', 'audit']
    with beam.Pipeline(options=options) as p:
        files = p | 'SourceFiles' >> beam.Create([
            (i, f'gs://{args.bucket}/{name}') for i, name in enumerate(selected)])
        parsed = files | 'ParseFiles' >> beam.ParDo(ParseFile())
        output = (parsed | 'GlobalKey' >> beam.Map(lambda x: x)
                  | 'GroupAllFiles' >> beam.GroupByKey()
                  | 'ValidateReferences' >> beam.ParDo(ValidateReferences()).with_outputs(*tags))
        for entity, cols in schemas.items():
            schema = {'fields': [{'name': c, 'type': 'STRING'} for c in cols]}
            (output[entity]
             | f'PrepareBronze_{entity}' >> beam.Map(to_bronze_row)
             | f'Write_{entity}' >> beam.io.WriteToBigQuery(
                table=f'{args.project}:{args.bronze_dataset}.{entity.lower()}',
                schema=schema, method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                custom_gcs_temp_location=args.temp_location,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
        (output['rejects'] | 'WriteRejectAudit' >> beam.io.WriteToBigQuery(
            table=f'{args.project}:{args.control_dataset}.quarantine_audit',
            schema=REJECT_SCHEMA, method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
            custom_gcs_temp_location=args.temp_location,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
        (output['audit'] | 'WriteFileAudit' >> beam.io.WriteToBigQuery(
            table=f'{args.project}:{args.control_dataset}.file_audit',
            schema=AUDIT_SCHEMA, method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
            custom_gcs_temp_location=args.temp_location,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
        (output['rejects'] | 'RejectToCSV' >> beam.Map(csv_line)
         | 'QuarantineCSV' >> beam.io.WriteToText(
             f'gs://{args.quarantine_bucket}/rejected_through_day{args.day}',
             file_name_suffix='.csv', num_shards=1,
             header=','.join(CSV_FIELDS)))
    logging.info('Dataflow Bronze rebuild completed through Day %s', args.day)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
