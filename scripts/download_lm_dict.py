"""Download the Loughran-McDonald Master Dictionary CSV.

Run once at install time. The canonical source is Bill McDonald's
research page at Notre Dame:
https://sraf.nd.edu/loughranmcdonald-master-dictionary/

The CSV is freely available for academic and research use.

Usage::

    python scripts/download_lm_dict.py
    # writes to collectors/nlp/data/lm_master_dict.csv

The URL has changed between LM dict revisions historically; the
operator may need to update ``LM_DICT_URL`` if Notre Dame relocates
the file. Validate the download by sampling: the CSV should have
column "Word" and rows like "good" with Positive flag = a year
(non-zero) and Negative flag = 0.

Idempotent: the script writes to a fixed path; re-running overwrites
with the latest version. Pin via vcs commit if reproducibility is a
concern.
"""

from __future__ import annotations

import argparse
import sys
import urllib.request
from pathlib import Path


# Update this URL when Bill McDonald rotates the dict version. As of
# 2026-06-09 the latest stable release is the 1993-2025 Master Dictionary
# (updated March 2026). NOTE: this upstream Google-Drive id rotates and has
# 404'd before (L4575) — production hosts self-heal from our own S3 mirror
# (s3://alpha-engine-research/reference/nlp/lm_master_dict.csv) via
# collectors.nlp.loughran_mcdonald.ensure_lm_master_dict, NOT this script.
# This script is the operator path for (re)seeding that S3 mirror.
LM_DICT_URL = (
    "https://drive.google.com/uc?export=download&id="
    "1iq2RUf8qGFEAk1g8wQntP3habOnR3fXF"
    # ^ 1993-2025 Master Dictionary; if this 404s, fetch the latest URL from
    #   https://sraf.nd.edu/loughranmcdonald-master-dictionary/
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out", type=Path,
        default=Path(__file__).resolve().parent.parent
                / "collectors" / "nlp" / "data" / "lm_master_dict.csv",
        help="Destination CSV path (default: bundled location).",
    )
    parser.add_argument(
        "--url", type=str, default=LM_DICT_URL,
        help="Override the source URL if Notre Dame relocates the dict.",
    )
    args = parser.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)

    print(f"Fetching from {args.url}")
    print(f"Writing to {args.out}")
    try:
        urllib.request.urlretrieve(args.url, args.out)
    except Exception as e:
        print(f"ERROR: download failed: {e}", file=sys.stderr)
        print(
            "If the URL has rotated, fetch the latest from "
            "https://sraf.nd.edu/loughranmcdonald-master-dictionary/ "
            "and pass --url.",
            file=sys.stderr,
        )
        return 1

    size = args.out.stat().st_size
    print(f"Wrote {size:,} bytes to {args.out}")
    if size < 1_000_000:
        print(
            "WARNING: file is suspiciously small — the canonical LM "
            "Master Dictionary is ~10MB. Verify the source URL.",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
