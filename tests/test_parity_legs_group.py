"""Two dispatches write ONE parity report, and neither erases the other.

`alpha-engine-config-I11352` deliverable 4. `shadow-sameday` runs the two
post-market legs at 18:30 ET on day D; `shadow-morning` runs v1's two morning
legs at 07:45 ET on D+1 for the PREVIOUS session — because Polygon's grouped
daily bar for session D is only final the next morning, which is why v1
schedules them there too.

Both publish `parity/{D}.json`. The comparison itself is rewritten by the
second run, deliberately: the morning legs' keys do not exist at 18:30 ET. The
`legs` block must NOT be, or the report would say the post-market legs never
ran — the exact silence `alpha-engine-config-I11200` created that block to end.

The sibling morning dispatch calls, verbatim:

    python -m shadow parity --trading-day $TD --legs-file $LEGS \\
      --legs-group morning --store s3://alpha-engine-research/data_collection
"""

from __future__ import annotations

import datetime as dt
import json

import pytest

from shadow import parity
from shadow.__main__ import _parser


def test_the_flag_exists_with_the_two_groups_and_defaults_to_sameday():
    args = _parser().parse_args(
        ["parity", "--trading-day", "2026-09-21", "--store", "/tmp/x"]
    )
    assert args.legs_group == "sameday"
    args = _parser().parse_args(
        [
            "parity",
            "--trading-day",
            "2026-09-21",
            "--legs-file",
            "/tmp/legs.tsv",
            "--legs-group",
            "morning",
            "--store",
            "s3://alpha-engine-research/data_collection",
        ]
    )
    assert args.legs_group == "morning"
    assert args.legs_file == "/tmp/legs.tsv"


def test_an_unknown_group_is_refused_at_the_cli():
    with pytest.raises(SystemExit):
        _parser().parse_args(
            ["parity", "--trading-day", "2026-09-21", "--store", "/tmp/x",
             "--legs-group", "afternoon"]
        )


def test_a_second_group_rewrites_its_own_legs_and_keeps_the_others():
    sameday = [
        {"name": "post-market-data", "exit_code": 1, "ok": False, "dispatch": "sameday"},
        {"name": "post-market-arctic-append", "exit_code": 0, "ok": True, "dispatch": "sameday"},
    ]
    morning = [
        {"name": "morning-enrich", "exit_code": 0, "ok": True},
        {"name": "morning-arctic-append", "exit_code": 0, "ok": True},
    ]
    merged, known = parity.merge_legs(sameday, morning, group="morning")
    assert [leg["name"] for leg in merged] == [
        "post-market-data",
        "post-market-arctic-append",
        "morning-enrich",
        "morning-arctic-append",
    ]
    assert known == {"sameday": True, "morning": True}
    assert all(leg["dispatch"] == "morning" for leg in merged[2:])

    # And the morning dispatch re-running replaces only its own entries.
    again, known = parity.merge_legs(
        merged, [{"name": "morning-enrich", "exit_code": 1, "ok": False}], group="morning"
    )
    assert [leg["name"] for leg in again] == [
        "post-market-data",
        "post-market-arctic-append",
        "morning-enrich",
    ]
    assert known == {"sameday": True, "morning": True}


def test_a_pre_i11352_entry_is_attributed_to_the_default_group():
    """An entry written before the split carries no `dispatch`. Dropping it
    would lose a measurement; leaving it unlabelled would make it
    un-replaceable forever. It belongs to the only dispatch that existed."""
    merged, known = parity.merge_legs(
        [{"name": "post-market-data", "exit_code": 0, "ok": True}], [], group="morning"
    )
    assert merged[0]["dispatch"] == "sameday"
    assert known == {"sameday": True, "morning": False}


def test_a_missing_morning_dispatch_renders_as_false_never_as_silence():
    merged, known = parity.merge_legs(
        [], [{"name": "post-market-data", "exit_code": 0, "ok": True}], group="sameday"
    )
    assert known == {"sameday": True, "morning": False}
    body = parity.ParityReport(
        trading_day=dt.date(2026, 9, 21),
        bucket="b",
        shadow_prefix="staging/shadow/2026-09-21/",
        code_sha="abc",
        rows=[],
        excluded=[],
        rel_tolerance=1e-6,
        absolute_tolerance=1e-9,
        generated_at="2026-09-21T23:43:04Z",
        legs=merged,
        legs_known=known,
    ).as_dict()
    assert body["legs_known"] == {"sameday": True, "morning": False}


def test_an_unknown_group_is_refused_by_the_merge():
    with pytest.raises(ValueError, match="legs group"):
        parity.merge_legs([], [], group="afternoon")


def test_the_cli_merges_into_an_existing_report_on_disk(tmp_path, monkeypatch):
    """End to end over the real store backend: a same-day publish followed by a
    morning publish leaves ONE report carrying both groups."""
    from shadow import __main__ as cli

    store_dir = tmp_path / "data_collection"
    key = parity.parity_key(dt.date(2026, 9, 21))

    def _fake_run_parity(**kwargs):
        return parity.ParityReport(
            trading_day=kwargs["trading_day"],
            bucket=kwargs["bucket"],
            shadow_prefix="staging/shadow/2026-09-21/",
            code_sha="sha",
            rows=[],
            excluded=[],
            rel_tolerance=1e-6,
            absolute_tolerance=1e-9,
            generated_at="2026-09-21T23:43:04Z",
            legs=kwargs["legs"],
            legs_known=kwargs["legs_known"],
        )

    monkeypatch.setattr(parity, "run_parity", _fake_run_parity)

    sameday_file = tmp_path / "sameday.tsv"
    sameday_file.write_text("post-market-data\t0\npost-market-arctic-append\t0\n")
    cli.main([
        "parity", "--trading-day", "2026-09-21", "--store", str(store_dir),
        "--legs-file", str(sameday_file),
    ])
    first = json.loads((store_dir / key).read_text(encoding="utf-8"))
    assert first["legs_known"] == {"sameday": True, "morning": False}

    morning_file = tmp_path / "morning.tsv"
    morning_file.write_text("morning-enrich\t0\nmorning-arctic-append\t0\n")
    cli.main([
        "parity", "--trading-day", "2026-09-21", "--store", str(store_dir),
        "--legs-file", str(morning_file), "--legs-group", "morning",
    ])
    second = json.loads((store_dir / key).read_text(encoding="utf-8"))
    assert second["legs_known"] == {"sameday": True, "morning": True}
    assert [leg["name"] for leg in second["legs"]] == [
        "post-market-data",
        "post-market-arctic-append",
        "morning-enrich",
        "morning-arctic-append",
    ]
    assert {leg["dispatch"] for leg in second["legs"]} == {"sameday", "morning"}
