"""Regenerate the synthetic CUR parquet fixtures ``test_cur_parquet.py`` reads.

The tests import :func:`rows` from here as the expected values, so this file
must stay importable without pyarrow (the Lambda's test preflight has none,
which is the whole reason ``cur_parquet.py`` exists). Run by hand when a new encoding needs
a fixture:  python3 fixtures/make_cur_fixtures.py   (needs pyarrow)

The data is SYNTHETIC: invented line items with the export's column names and
types. No real billing data is committed to this public repository.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

HERE = Path(__file__).parent
PRODUCTS = [("AmazonEC2", "USE1-BoxUsage:t3.small"), ("AmazonEC2", "USE1-EBS:VolumeUsage.gp3"),
            ("AmazonS3", "Requests-Tier1"), ("AmazonStates", "StateTransition"),
            ("AmazonCloudWatch", "CW:Requests"), ("AWSCostExplorer", "USE1-APIRequest")]
SYSTEMS = [None, "crucible-v2", "fleet-audit", None, "data-collection", None]


def rows(n: int = 400) -> list[dict]:
    out = []
    start = datetime(2026, 10, 1)
    for i in range(n):
        product, usage = PRODUCTS[i % len(PRODUCTS)]
        line_type = "Usage"
        if i % 97 == 0:
            product, usage, line_type = "ComputeSavingsPlans", "ComputeSP:1yrNoUpfront", "SavingsPlanRecurringFee"
        if i == 5:
            product, usage, line_type = "AmazonEC2", "", "Tax"
        out.append({
            "bill_billing_period_start_date": start,
            "line_item_usage_start_date": start + timedelta(hours=(i * 7) % (24 * 9)),
            "line_item_unblended_cost": None if i % 151 == 0 else round(0.0013 * (i % 41) + i * 1e-5, 8),
            "line_item_product_code": product,
            "line_item_usage_type": usage,
            "line_item_line_item_type": line_type,
            "resource_tags_user_component": None if i % 3 else f"comp-{i % 5}",
            "resource_tags_user_system": SYSTEMS[i % len(SYSTEMS)],
        })
    return out


VARIANTS = {
    # The shape the live export writes (parquet-mr via Spark): v1 pages, dictionary, snappy, INT96.
    "spark_like": dict(version="1.0", data_page_version="1.0", use_dictionary=True,
                       compression="snappy", use_deprecated_int96_timestamps=True),
    # parquet-mr's fallback when a dictionary overflows: PLAIN values.
    "plain_gzip": dict(version="1.0", data_page_version="1.0", use_dictionary=False,
                       compression="gzip", use_deprecated_int96_timestamps=True),
    # Newer writers: v2 data pages, RLE_DICTIONARY, INT64 timestamps, no compression,
    # several row groups and small pages.
    "v2_int64_multi_rowgroup": dict(version="2.6", data_page_version="2.0", use_dictionary=True,
                                    compression="none", row_group_size=130, data_page_size=512),
}


def main() -> None:
    import pyarrow as pa  # only the generator needs it; the tests import rows()
    import pyarrow.parquet as pq

    data = rows()
    table = pa.Table.from_pylist(data, schema=pa.schema([
        ("bill_billing_period_start_date", pa.timestamp("ns")),
        ("line_item_usage_start_date", pa.timestamp("ns")),
        ("line_item_unblended_cost", pa.float64()),
        ("line_item_product_code", pa.string()),
        ("line_item_usage_type", pa.string()),
        ("line_item_line_item_type", pa.string()),
        ("resource_tags_user_component", pa.string()),
        ("resource_tags_user_system", pa.string()),
    ]))
    for name, kw in VARIANTS.items():
        kw = dict(kw)
        rg = kw.pop("row_group_size", None)
        pq.write_table(table, HERE / f"cur_{name}.parquet", row_group_size=rg, **kw)


if __name__ == "__main__":
    main()
