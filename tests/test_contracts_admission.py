"""Semantic admission at the publish boundary (alpha-engine-config-I12082).

Seeded counterexamples, each schema-VALID so the advisory validator alone
admits it: semantically wrong units, an incomplete or stray manifest, stale
lineage and an incompatible version. Plus the valid case, shadow vs enforce,
and the fail-closed mode parse.
"""

from __future__ import annotations

import copy
import dataclasses
import json
from datetime import date

import pytest

from contracts import validate_technicals
from contracts.admission import (
    TECHNICALS,
    AdmissionRefused,
    AdmissionSpec,
    admit,
    mode,
    publish_admitted,
)

RUN = "2026-10-06"
TODAY = date(2026, 10, 6)
ROW = {
    "rsi_14": 55.2,
    "macd_hist": 0.12,
    "ma_50": 101.0,
    "ma_200": 98.5,
    "pct_to_ma_50": 0.0198,
    "pct_to_ma_200": 0.0457,
    "high_52w": 120.0,
    "low_52w": 80.0,
    "pct_in_52w_range": 0.575,
    "pct_from_52wk_high": -0.1417,
    "mom_20d": 0.031,
    "mom_60d": -0.02,
}
VALID = {
    "schema_version": 4,
    "as_of": RUN,
    "source": "computed",
    "technicals": {"AAA": ROW, "BBB": dict(ROW)},
}
POP = ["AAA", "BBB", "CCC"]


def _admit(doc, pop=POP):
    return admit(doc, TECHNICALS, run_date=RUN, population=pop, today=TODAY)


def test_valid_document_is_admitted():
    assert validate_technicals(VALID) == []
    assert _admit(VALID) == []


@pytest.mark.parametrize(
    "field,value",
    [
        ("pct_in_52w_range", 57.5),
        ("pct_from_52wk_high", -14.17),
        ("rsi_14", 0.552 * 1000),
        ("mom_20d", -3.1),
    ],
)
def test_percent_where_a_fraction_belongs_is_refused_though_schema_valid(field, value):
    doc = copy.deepcopy(VALID)
    doc["technicals"]["AAA"][field] = value
    assert validate_technicals(doc) == []  # schema cannot see it
    assert any(p.startswith("units:") and field in p for p in _admit(doc))


def test_moving_average_outside_its_own_window_is_refused():
    doc = copy.deepcopy(VALID)
    doc["technicals"]["AAA"]["ma_200"] = 150.0
    assert any("inside [low_52w, high_52w]" in p for p in _admit(doc))


def test_stray_member_and_empty_manifest_are_refused():
    doc = copy.deepcopy(VALID)
    doc["technicals"]["ZZZ"] = dict(ROW)
    assert any(p.startswith("manifest:") and "ZZZ" in p for p in _admit(doc))
    empty = dict(VALID, technicals={})
    assert any("empty technicals" in p for p in _admit(empty))


def test_stale_lineage_is_refused():
    doc = dict(VALID, as_of="2026-09-20")
    problems = admit(
        doc, TECHNICALS, run_date="2026-09-20", population=POP, today=TODAY
    )
    assert any("days old" in p for p in problems)
    assert any("is not this run's date" in p for p in _admit(doc))


def test_incompatible_version_is_refused():
    doc = dict(VALID, schema_version=3)
    assert any(p.startswith("version:") for p in _admit(doc))


SHADOW_SPEC = dataclasses.replace(TECHNICALS, mode="shadow")


def test_shadow_writes_and_reports(monkeypatch):
    monkeypatch.delenv("NOUSERGON_ADMISSION_MODE", raising=False)
    writes = []
    bad = dict(VALID, schema_version=3)
    d = publish_admitted(
        lambda: writes.append(1),
        bad,
        SHADOW_SPEC,
        run_date=RUN,
        population=POP,
        today=TODAY,
    )
    assert writes == [1] and d["mode"] == "shadow" and not d["admitted"]


def test_enforce_refuses_before_writing(monkeypatch):
    monkeypatch.setenv("NOUSERGON_ADMISSION_MODE", "enforce")
    writes = []
    with pytest.raises(AdmissionRefused):
        publish_admitted(
            lambda: writes.append(1),
            dict(VALID, schema_version=3),
            TECHNICALS,
            run_date=RUN,
            population=POP,
            today=TODAY,
        )
    assert writes == []
    d = publish_admitted(
        lambda: writes.append(1),
        VALID,
        TECHNICALS,
        run_date=RUN,
        population=POP,
        today=TODAY,
    )
    assert writes == [1] and d["admitted"]


def test_unknown_mode_fails_closed(monkeypatch):
    monkeypatch.setenv("NOUSERGON_ADMISSION_MODE", "enfroce")
    assert mode() == "enforce"


# ── Per-artifact mode (I12082 ruling, Brian 2026-10-23) ──────────────────────


def test_technicals_defaults_to_enforce(monkeypatch):
    monkeypatch.delenv("NOUSERGON_ADMISSION_MODE", raising=False)
    assert TECHNICALS.mode == "enforce"
    assert mode(TECHNICALS) == "enforce"


def test_spec_default_is_shadow_and_env_unset_follows_the_spec(monkeypatch):
    monkeypatch.delenv("NOUSERGON_ADMISSION_MODE", raising=False)
    plain = AdmissionSpec("x", "technicals", 4, "technicals")
    assert plain.mode == "shadow" and mode(plain) == "shadow"
    assert mode() == "shadow"  # no spec: an undeclared artifact is shadow


def test_env_overrides_in_either_direction(monkeypatch):
    monkeypatch.setenv("NOUSERGON_ADMISSION_MODE", "shadow")
    assert mode(TECHNICALS) == "shadow"
    monkeypatch.setenv("NOUSERGON_ADMISSION_MODE", " Enforce ")
    assert mode(SHADOW_SPEC) == "enforce"
    monkeypatch.setenv("NOUSERGON_ADMISSION_MODE", "")
    assert mode(SHADOW_SPEC) == "enforce"  # set-but-blank fails closed


def test_invalid_spec_mode_is_rejected_at_declaration():
    with pytest.raises(ValueError):
        AdmissionSpec("x", "technicals", 4, "technicals", mode="enfroce")


def test_default_enforce_refuses_before_writing(monkeypatch):
    monkeypatch.delenv("NOUSERGON_ADMISSION_MODE", raising=False)
    writes = []
    with pytest.raises(AdmissionRefused) as exc:
        publish_admitted(
            lambda: writes.append(1),
            dict(VALID, schema_version=3),
            TECHNICALS,
            run_date=RUN,
            population=POP,
            today=TODAY,
        )
    assert writes == [] and exc.value.artifact == TECHNICALS.artifact


def test_env_shadow_override_writes_a_refused_technicals(monkeypatch):
    monkeypatch.setenv("NOUSERGON_ADMISSION_MODE", "shadow")
    writes = []
    d = publish_admitted(
        lambda: writes.append(1),
        dict(VALID, schema_version=3),
        TECHNICALS,
        run_date=RUN,
        population=POP,
        today=TODAY,
    )
    assert writes == [1] and d["mode"] == "shadow" and not d["admitted"]


class _S3:
    """Minimal S3 double: one object store keyed by Key."""

    def __init__(self, objects):
        self.objects = dict(objects)
        self.puts = []

    def get_object(self, Bucket, Key):  # noqa: N803 — boto3 signature
        if Key not in self.objects:
            raise KeyError(Key)

        class _Body:
            def __init__(self, raw):
                self._raw = raw

            def read(self):
                return self._raw

        return {"Body": _Body(self.objects[Key])}

    def put_object(self, Bucket, Key, Body, **kw):  # noqa: N803
        self.puts.append(Key)
        self.objects[Key] = Body if isinstance(Body, bytes) else Body.encode()
        return {}


def test_collector_refusal_reports_error_and_keeps_yesterdays_latest(monkeypatch):
    """The ruling's operational half: under the default (enforce), an
    inadmissible technicals document is never written. The collector's
    existing ``except`` path returns ``status: error`` and the previous
    ``latest.json`` is untouched."""
    from collectors import metron_market_data as mmd

    monkeypatch.delenv("NOUSERGON_ADMISSION_MODE", raising=False)
    latest = f"{mmd.TECHNICALS_PREFIX}latest.json"
    yesterday = json.dumps({"as_of": "2026-10-05", "marker": "yesterday"}).encode()
    s3 = _S3({latest: yesterday})
    monkeypatch.setattr(
        mmd,
        "load_price_derived_universe",
        lambda bucket, client: ([{"yf_symbol": "AAA"}], None),
    )
    monkeypatch.setattr(
        mmd, "_read_json", lambda client, bucket, key: {"closes": [[RUN, 100.0]]}
    )
    # A percent where a fraction belongs: schema-valid, semantically wrong.
    monkeypatch.setattr(
        mmd, "_compute_technicals", lambda closes: dict(ROW, pct_in_52w_range=57.5)
    )
    out = mmd.collect_technicals(
        bucket="b", s3_client=s3, run_date=str(date.today())
    )
    assert out["status"] == "error" and "refused" in out["error"]
    assert s3.puts == [] and s3.objects[latest] == yesterday
