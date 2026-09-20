"""A contract a unit declares must be resolvable from the keys that unit writes.

`alpha-engine-config-I11203`. Measured 2026-09-20: **33 of 35 contracts in
`contracts/` declared no `x-key-pattern`**, so `shadow.parity.resolve_contract`
returned ``None`` for every key they document. Only `constituents` and
`staging_daily_closes` were wired.

The consequence was silent and total. `resolve_contract` returning ``None``
means an empty `provenance_fields` set, which means `_split_json_diffs` routes
EVERY diff into `breaches` — including run-timestamp fields whose whole purpose
is to differ between two productions of the same artifact. The provenance
mechanism existed, was tested, and was **inert for 33 of 35 contracts**.

The evidence is in the 2026-09-18 parity report: of 959 keys, the two with a
resolvable contract behaved correctly, and `market_data/sentiment/latest.json`
reported `$.sentiment.AAPL.as_of: '2026-09-18' live vs '2026-09-20' shadow` as a
DATA breach — a timestamp, graded as if the sentiment had changed.

**What this test grades is the property, not the 33 instances.** A contract
that a descriptor declares, for a unit that publishes S3 keys, must be
reachable from at least one of those keys. Anything else is a contract nobody
can consult — which is indistinguishable, from the comparator's side, from
having no contract at all.
"""

from __future__ import annotations

import pathlib

import pytest
import yaml

from shadow.parity import _CONTRACTS_DIR, resolve_contract

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
_UNITS_DIR = _REPO_ROOT / "registry.d" / "units"


def _concrete_keys(writes) -> list[str]:
    """The declared writes that are real S3 key templates.

    Excluded, all of them deliberately and not as a convenience:
    ArcticDB libraries (`arcticdb/universe (library)`) address a store, not a
    key; `lib::symbol` forms name a symbol inside one; a bare token with no
    `/` is not a key.
    """
    out = []
    for w in writes or []:
        if not isinstance(w, str) or "/" not in w:
            continue
        if "(library)" in w or "::" in w:
            continue
        out.append(w)
    return out


def _units_declaring_a_schema():
    for path in sorted(_UNITS_DIR.glob("*.yaml")):
        doc = yaml.safe_load(path.read_text())
        schema = (doc.get("contract") or {}).get("schema")
        if not schema:
            continue
        schemas = [schema] if isinstance(schema, str) else list(schema)
        schemas = [s for s in schemas if str(s).endswith(".schema.json")]
        if not schemas:
            continue
        keys = _concrete_keys(doc.get("writes"))
        if not keys:
            continue
        yield doc["unit_id"], schemas, keys


def test_there_are_units_declaring_schemas_over_s3_keys():
    """Non-vacuity guard — without it every assertion below could iterate
    nothing and report green over an empty set."""
    units = list(_units_declaring_a_schema())
    assert len(units) >= 20, f"expected the bulk of the registry, found {len(units)}"


@pytest.mark.parametrize("unit_id,schemas,keys", list(_units_declaring_a_schema()), ids=lambda v: v if isinstance(v, str) else "")
def test_every_declared_contract_is_reachable_from_a_key_its_unit_writes(unit_id, schemas, keys):
    resolved = {}
    for key in keys:
        contract = resolve_contract(key)
        if contract is not None:
            resolved.setdefault(contract.path.name, []).append(key)

    for schema_name in schemas:
        name = pathlib.Path(schema_name).name
        if not (_CONTRACTS_DIR / name).exists():
            pytest.skip(f"{unit_id}: declared contract {name} is not in contracts/ — a different finding")
        assert name in resolved, (
            f"{unit_id} declares contract {name}, but none of the keys it writes resolves to it:\n"
            + "\n".join(f"    {k} -> {(resolve_contract(k).path.name if resolve_contract(k) else 'NONE')}" for k in keys)
            + f"\n\nAdd an `x-key-pattern` to contracts/{name} naming the key(s) it documents "
            "(a string, or a list when one contract covers several). Without it "
            "shadow.parity.resolve_contract cannot reach the contract, every provenance field "
            "grades as a data breach, and the contract is unconsultable "
            "(alpha-engine-config-I11203)."
        )


def test_run_timestamp_fields_are_declared_provenance_where_a_contract_declares_them():
    """A top-level run-timestamp that grades as data makes every re-production
    of the same artifact a mismatch. This asserts the class, over whatever
    contracts declare such a field."""
    import json

    TIMESTAMPS = {"as_of", "generated_at", "generated_utc", "fetched_at", "produced_at"}
    offenders = []
    for path in sorted(_CONTRACTS_DIR.glob("*.schema.json")):
        doc = json.loads(path.read_text())
        if not doc.get("x-key-pattern"):
            continue  # not resolvable from a key; provenance is not consulted
        props = doc.get("properties") or {}
        for field in sorted(TIMESTAMPS & set(props)):
            prop = props[field]
            if isinstance(prop, dict) and prop.get("x-provenance") is not True:
                offenders.append(f"{path.name}:{field}")
    assert not offenders, (
        "these key-resolvable contracts declare a run-timestamp field that is NOT marked "
        f"`x-provenance: true`, so it grades as a data breach on every re-production: {offenders}"
    )
