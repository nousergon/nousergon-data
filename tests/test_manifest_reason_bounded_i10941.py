"""A failing mode's manifest `reason` names the failing collector and stays
bounded, even when the result it failed with carries a large array.

`alpha-engine-config-I10941`. The 2026-09-16 shadow run's D17 manifest
recorded `reason = "_CollectorError: morning_enrich: morning_enrich returned
status='failed': {...}"` with `constituents_preflight`'s 903-ticker `tickers`
array f-strung in whole — the field ended mid-array, and the sub-collector
that actually failed never appeared. `run_units.describe_mode_failure` is the
fix: every call site that used to build this string with a raw f-string of
the result dict (`weekly_collector.py::_run_whole_mode_unit`,
`collectors/daily_news.py`, `collectors/metron_market_data.py`) now goes
through it. This test constructs the exact reported shape — a large
`tickers` array on an unrelated ok sub-collector, and the real failure on a
different one — and asserts the built reason both names the failing
collector up front and stays under the cap.
"""

from __future__ import annotations

import run_units


def _morning_enrich_result_like_2026_09_16() -> dict:
    """Reconstructs the reported shape: `constituents_preflight` is fine and
    huge, some other sub-collector is the one that actually failed."""
    return {
        "mode": "morning_enrich",
        "date": "2026-09-16",
        "status": "failed",
        "collectors": {
            "constituents_preflight": {
                "status": "ok",
                "count": 903,
                "tickers": [f"TICK{i:04d}" for i in range(903)],
            },
            "daily_closes_fetch": {
                "status": "error",
                "error": "vendor returned 502 for polygon T+1 aggregate window",
            },
        },
    }


def test_reason_names_failing_collector_in_first_200_chars():
    result = _morning_enrich_result_like_2026_09_16()
    reason = run_units.describe_mode_failure("morning_enrich", result)
    head = reason[:200]
    assert "daily_closes_fetch" in head
    assert "502" in head


def test_reason_stays_under_the_cap_with_a_900_element_array():
    result = _morning_enrich_result_like_2026_09_16()
    reason = run_units.describe_mode_failure("morning_enrich", result)
    assert len(reason) <= run_units.REASON_MAX_LEN


def test_reason_never_inlines_the_bulk_array_raw():
    result = _morning_enrich_result_like_2026_09_16()
    reason = run_units.describe_mode_failure("morning_enrich", result)
    # The raw per-ticker values must not appear — only the elided placeholder.
    assert "TICK0000" not in reason
    assert "TICK0902" not in reason
    assert "903 items elided" in reason


def test_reason_marks_truncation_explicitly_when_even_elided_form_overflows():
    # A pathologically wide result (many sub-collectors, each with a modest
    # dict) can still overflow the cap even after elision; the marker must
    # say so rather than silently stopping mid-sentence.
    huge = {
        "mode": "morning_enrich",
        "status": "failed",
        "collectors": {
            f"collector_{i}": {"status": "ok", "detail": "x" * 40} for i in range(200)
        },
    }
    reason = run_units.describe_mode_failure("morning_enrich", huge, max_len=500)
    assert len(reason) <= 500
    assert "reason_truncated: true" in reason


def test_elide_bulk_replaces_long_lists_and_preserves_short_ones():
    payload = {"tickers": list(range(50)), "small": [1, 2, 3], "note": "ok"}
    out = run_units.elide_bulk(payload, max_items=10)
    assert out["tickers"] == "<list: 50 items elided>"
    assert out["small"] == [1, 2, 3]
    assert out["note"] == "ok"


def test_no_sub_collector_case_still_bounded_and_labeled():
    # daily_news / collect_intraday: a flat result, no `collectors` sub-dict.
    result = {
        "status": "error",
        "error": "podcast digest build failed: TTS provider timeout",
        "articles": [{"title": f"Article {i}", "body": "x" * 500} for i in range(300)],
    }
    reason = run_units.describe_mode_failure("daily_news", result)
    assert "daily_news" in reason[:200]
    assert len(reason) <= run_units.REASON_MAX_LEN
    assert "Article 299" not in reason
