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

URL = "https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter99.zip"
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


def main():
    # 1) Download the ZIP file into memory (you can switch to disk if desired)
    print(f"Downloading: {URL}")
    try:
        zip_bytes = stream_zip_bytes(URL)
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

    # 3) Open output CSV
    out_path = os.path.abspath(OUTPUT_CSV)
    wrote_header = False
    total_rows = 0

    with open(out_path, "w", newline="", encoding="utf-8") as fout:
        writer = None

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

                # Initialize writer with normalized CSV excel dialect
                if writer is None:
                    writer = csv.writer(
                        fout, lineterminator="\n", quoting=csv.QUOTE_MINIMAL
                    )
                # Write header once (normalized to strings stripped of BOM)
                if not wrote_header:
                    if header and header[0].startswith("\ufeff"):
                        header[0] = header[0].lstrip("\ufeff")
                    writer.writerow(header)
                    wrote_header = True
                else:
                    # If subsequent files have a header, skip writing it
                    # Optionally, you could verify the header matches.
                    pass

                # Stream rows
                row_count = 0
                for row in reader:
                    # Ensure row length matches header by padding/truncating
                    if len(row) < len(header):
                        row = row + [""] * (len(header) - len(row))
                    elif len(row) > len(header):
                        row = row[: len(header)]
                    writer.writerow(row)
                    row_count += 1

                total_rows += row_count
                print(f"  Wrote {row_count:,} data rows from {mem.filename}")

    print(f"\nDone. Output: {out_path}")
    print(f"Total data rows written (excluding header): {total_rows:,}")


if __name__ == "__main__":
    main()
