#!/usr/bin/env python3
"""
DRAFTED INITIALLY BY CHATGPT

Download, extract, and convert the NC voter data archive to a single CSV.

Output: nc_vf_partial.csv
"""

import csv
import io
import os
import sys
import zipfile
from urllib.request import urlopen
from google.cloud import bigquery

bq_creds = "../vr-mail-generator-8e97a63564fe.json"
bq_client = bigquery.Client.from_service_account_json(bq_creds)


def county_id_from_num(num: int) -> str:
    if num == 100:
        return "00"
    else:
        return str(num)


url_list = [
    f"https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter{county_id_from_num(num)}.zip"
    for num in range(0, 100)
]


OUTPUT_CSV = "nc_vf_partial.csv"

# Candidate inner-file extensions we consider as tabular
CANDIDATE_EXTS = {".txt", ".csv", ".tsv"}


def sniff_dialect(sample_bytes: bytes):
    """Try to detect CSV dialect from a bytes sample; prefer tab/pipe/comma."""
    text = sample_bytes.decode("utf-8", errors="replace")
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(text, delimiters="\t,|")
        return dialect
    except Exception:
        # Manual heuristic fallback: prefer tab, then pipe, then comma
        lines = [ln for ln in text.splitlines() if ln.strip()]
        if not lines:
            # default to comma if nothing to go on
            class D(csv.excel):
                delimiter = ","

            return D
        first = lines[0]
        counts = {
            "\t": first.count("\t"),
            "|": first.count("|"),
            ",": first.count(","),
        }
        delim = max(counts, key=counts.get)

        class D(csv.excel):
            delimiter = delim

        return D


def is_tabular_member(name: str) -> bool:
    lower = name.lower()
    return any(lower.endswith(ext) for ext in CANDIDATE_EXTS)


def stream_zip_bytes(url: str) -> bytes:
    """Download the ZIP as bytes (streamed)."""
    with urlopen(url) as resp:
        # Read in chunks without progress bar to keep deps minimal
        chunks = []
        while True:
            chunk = resp.read(1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)


def collect_upload_countyvf(url, create_table=False):
    # 1) Download the ZIP file into memory (you can switch to disk if desired)
    print(f"Downloading: {url}")
    try:
        zip_bytes = stream_zip_bytes(url)
    except Exception as e:
        print(f"ERROR: failed to download: {e}", file=sys.stderr)
        sys.exit(1)

    # 2) Open ZIP and list candidate members
    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except zipfile.BadZipFile as e:
        print(f"ERROR: not a valid ZIP: {e}", file=sys.stderr)
        sys.exit(1)

    members = [m for m in zf.infolist() if is_tabular_member(m.filename)]
    if not members:
        # If nothing matched extensions, try everything (some archives use no ext)
        members = [m for m in zf.infolist() if not m.is_dir()]

    if not members:
        print("ERROR: No files found inside the ZIP archive.", file=sys.stderr)
        sys.exit(1)

    # Sort by file size (largest first) so we get the main statewide file first
    members.sort(key=lambda m: m.file_size, reverse=True)

    print("Found inside ZIP:")
    for m in members:
        print(f"  - {m.filename}  ({m.file_size:,} bytes)")

    if len(members) > 1:
        print("ERROR: Found multiple files in zip, should only be one", file=sys.stderr)
        sys.exit(1)

    table_id = "vr-mail-generator.voterfile.vf_nc_full"

    for mem in members:
        if mem.is_dir():
            continue

        print(f"\nProcessing: {mem.filename}")
        with zf.open(mem, "r") as raw:
            # Peek some bytes to sniff delimiter reliably
            peek = raw.read(1024 * 64)
            raw_stream = io.BytesIO(peek + raw.read())  # recompose

            # Wrap as text stream
            text_stream = io.TextIOWrapper(
                raw_stream, encoding="utf-8", errors="replace", newline=""
            )

            # Detect dialect and reset reader to start
            dialect = sniff_dialect(peek)
            text_stream.seek(0)

            reader = csv.reader(text_stream, dialect=dialect)

            # Read header
            try:
                header = next(reader)
            except StopIteration:
                print("  (empty file, skipping)")
                continue

            # Remove BOM if present
            if header and header[0].startswith("\ufeff"):
                header[0] = header[0].lstrip("\ufeff")

            # Create table if not exists
            if create_table:
                # Infer schema: all columns as STRING
                schema = [bigquery.SchemaField(col.strip(), "STRING") for col in header]
                try:
                    bq_client.get_table(table_id)
                    print(f"BigQuery table {table_id} already exists.")
                except Exception:
                    table = bigquery.Table(table_id, schema=schema)
                    table = bq_client.create_table(table)
                    print(f"Created BigQuery table {table_id}.")

            # Prepare rows for BigQuery
            rows_to_insert = []
            row_count = 0
            for row in reader:
                # Ensure row length matches header by padding/truncating
                if len(row) < len(header):
                    row = row + [""] * (len(header) - len(row))
                elif len(row) > len(header):
                    row = row[: len(header)]
                rows_to_insert.append(dict(zip(header, row)))
                row_count += 1

                # Insert in batches of 10,000 for efficiency
                if len(rows_to_insert) >= 10000:
                    errors = bq_client.insert_rows_json(table_id, rows_to_insert)
                    if errors:
                        print(f"BigQuery insert errors: {errors}", file=sys.stderr)
                    rows_to_insert = []

            # Insert any remaining rows
            if rows_to_insert:
                errors = bq_client.insert_rows_json(table_id, rows_to_insert)
                if errors:
                    print(f"BigQuery insert errors: {errors}", file=sys.stderr)

            print(
                f"  Inserted {row_count:,} data rows from {mem.filename} into BigQuery"
            )

    print(f"\nDone. Data loaded to BigQuery table: {table_id}")


if __name__ == "__main__":
    create_table = True
    for url in url_list:
        if create_table:
            collect_upload_countyvf(url, create_table=True)
        else:
            collect_upload_countyvf(url, create_table=False)
