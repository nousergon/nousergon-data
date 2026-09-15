"""The four base-column readers that replaced "no reader is built for this column yet".

`alpha-engine-config-I10823`: ``observability_row``, ``artifact_registry``,
``consumers`` and ``identity``. Each is a REAL read with the same three-way
honesty as every other reader in this package:

* **MET** only when the evidence was read and it satisfies the requirement.
* **UNMET** when we looked and the thing is not there — the missing artifact is
  named in ``detail``.
* **UNMEASURABLE** when we could not look — a denied read, no token, a
  repository the token cannot see, or a descriptor that declares nothing this
  column can grade — and the missing grant, credential or declaration is named.

Every reader has a withholding test in `tests/test_unit_readers.py`: remove the
evidence and the clause is never MET.
"""

from __future__ import annotations

import functools
import pathlib
import re
from typing import Any

import yaml

from data_gate.descriptors import REPO_ROOT, Unit
from data_gate.evidence import GateStore, Reading
from data_gate.sources import GATE_ROLE, GITHUB_TOKEN_ENV, SourceUnavailable

__all__ = [
    "OBSERVABILITY_ROWS_DIR",
    "WRITER_IDENTITIES_PATH",
    "read_artifact_registry",
    "read_consumers",
    "read_identity",
    "read_observability_row",
    "s3_write_targets",
]

OBSERVABILITY_ROWS_DIR = REPO_ROOT / "registry.d"
WRITER_IDENTITIES_PATH = REPO_ROOT / "data_gate" / "config" / "writer_identities.yaml"

# ---------------------------------------------------------------------------
# Interpreting a descriptor's `writes:` entries.
# ---------------------------------------------------------------------------

#: A published S3 key template: path characters, `{placeholders}` and `*`, and
#: nothing else. Anything with a space, `::` or `store:` is prose, an ArcticDB
#: symbol, a SQLite table or another store — named as ungradable, never guessed.
_S3_KEY_RE = re.compile(r"^[A-Za-z0-9_.{}*\-/]+$")
_ARCTIC_RE = re.compile(r"^arcticdb/([A-Za-z0-9_\-]+)\s+\(")
_PLACEHOLDER_RE = re.compile(r"\{[^}]*\}")


def s3_write_targets(unit: Unit) -> tuple[list[str], list[str], list[str]]:
    """``(s3_keys, arcticdb_libraries, ungradable)`` from the descriptor's `writes`."""
    keys: list[str] = []
    libraries: list[str] = []
    ungradable: list[str] = []
    for entry in unit.writes:
        text = str(entry).strip()
        arctic = _ARCTIC_RE.match(text)
        if arctic:
            libraries.append(arctic.group(1))
        elif _S3_KEY_RE.match(text) and "::" not in text:
            keys.append(text)
        else:
            ungradable.append(text)
    return keys, libraries, ungradable


def _normalized(template: str) -> str:
    return _PLACEHOLDER_RE.sub("{}", template)


def _literal_prefix(template: str) -> str:
    """Everything before the first placeholder or wildcard."""
    cut = len(template)
    for marker in ("{", "*"):
        index = template.find(marker)
        if index != -1:
            cut = min(cut, index)
    return template[:cut]


def _concrete(template: str) -> str:
    """A concrete key the template could produce, for IAM simulation."""
    key = _PLACEHOLDER_RE.sub("gate-probe", template).replace("*", "gate-probe")
    return f"{key}gate-probe" if key.endswith("/") else key


def _declared_row_id(entry: Any) -> str:
    """`daily_closes_parquet (indirect)` -> `daily_closes_parquet`."""
    return str(entry).strip().split()[0].rstrip(",") if str(entry).strip() else ""


def _descriptor_ref(unit: Unit) -> str:
    return f"registry.d/units/{unit.path.name}"


# ---------------------------------------------------------------------------
# observability_row — this repository's own generated registry.d rows.
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=8)
def _observability_index(directory: str) -> tuple[dict[str, list[tuple[str, dict]]], tuple[str, ...]]:
    """``component_id -> [(file, row)]`` over ``registry.d/*.yaml``, plus parse failures."""
    index: dict[str, list[tuple[str, dict]]] = {}
    failures: list[str] = []
    for path in sorted(pathlib.Path(directory).glob("*.yaml")):
        try:
            document = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError) as exc:
            failures.append(f"{path.name}: {type(exc).__name__}")
            continue
        if isinstance(document, dict) and document.get("component_id"):
            index.setdefault(str(document["component_id"]), []).append((path.name, document))
    return index, tuple(failures)


def read_observability_row(unit: Unit, *, rows_dir: pathlib.Path | None = None) -> Reading:
    """Does this unit have a registry row of its OWN, agreeing with its descriptor?

    Keyed on the unit's `component_id`, so a stage or dispatcher umbrella row
    can never satisfy it — the aggregate-hides-member defect the requirement
    names. The rows are the generated `registry.d/data-collector-*.yaml` files
    (`scripts/gen_observability_rows.py`, nousergon-data-PR1713) that
    nous-ergon-ops' `gather_repo_descriptors.py` publishes to the console; the
    gate job checks out `main`, so this reads exactly what is published.
    """
    directory = rows_dir or OBSERVABILITY_ROWS_DIR
    expected = f"registry.d/{unit.component_id}.yaml"
    index, failures = _observability_index(str(directory))
    claims = index.get(unit.component_id, [])
    source = "nousergon-data registry.d"
    if not claims:
        if failures:
            return Reading(
                met=False,
                detail=(
                    f"no registry row carries component_id {unit.component_id!r}, and "
                    f"{len(failures)} row file(s) would not parse ({list(failures)[:5]}) — the "
                    "row may be one of them, so absence cannot be concluded"
                ),
                evidence=(expected,),
                unmeasurable=True,
                source=source,
            )
        return Reading(
            met=False,
            detail=(
                f"no registry row carries component_id {unit.component_id!r}; expected {expected}, "
                "generated from the descriptor by scripts/gen_observability_rows.py. Without it the "
                "console cannot see this unit, and a unit it cannot see renders as nothing at all."
            ),
            evidence=(expected, _descriptor_ref(unit)),
            source=source,
        )
    files = tuple(f"registry.d/{name}" for name, _ in claims)
    if len(claims) > 1:
        return Reading(
            met=False,
            detail=f"{len(claims)} registry rows claim component_id {unit.component_id!r}: {list(files)}",
            evidence=files,
            source=source,
        )
    name, row = claims[0]
    problems: list[str] = []
    if str(row.get("owning_repo") or "") != str(unit.raw.get("owning_repo") or ""):
        problems.append(
            f"owning_repo {row.get('owning_repo')!r} != descriptor {unit.raw.get('owning_repo')!r}"
        )
    if str(row.get("lifecycle") or "") != unit.lifecycle:
        problems.append(f"lifecycle {row.get('lifecycle')!r} != descriptor {unit.lifecycle!r}")
    if problems:
        return Reading(
            met=False,
            detail=(
                f"registry.d/{name} disagrees with {_descriptor_ref(unit)}: {'; '.join(problems)}. "
                "A generated row that no longer matches its descriptor is stale — re-run the generator."
            ),
            evidence=files + (_descriptor_ref(unit),),
            source=source,
        )
    return Reading(
        met=True,
        detail=f"registry.d/{name} carries component_id {unit.component_id!r}, lifecycle {unit.lifecycle!r}",
        evidence=files,
        source=source,
    )


# ---------------------------------------------------------------------------
# artifact_registry — the enforced ARTIFACT_REGISTRY copy.
# ---------------------------------------------------------------------------


def read_artifact_registry(store: GateStore, unit: Unit) -> Reading:
    """Every published S3 key has a registry row or a grandfathered prefix, and
    every row the descriptor names exists and is not parked.

    Read from the copy the freshness monitor enforces
    (`data_gate.sources.PUBLISHED_REGISTRY_URI`), through
    ``store.artifact_registry_source``. A store with no source configured —
    tests, a local run without ``--artifact-registry`` — is UNMEASURABLE by
    construction.
    """
    keys, libraries, ungradable = s3_write_targets(unit)
    declared_rows = [r for r in (_declared_row_id(e) for e in unit.raw.get("registry_rows") or []) if r]
    not_graded = ungradable + [f"arcticdb/{lib} (ArcticDB library; graded by the in-region probe)" for lib in libraries]
    source_obj = getattr(store, "artifact_registry_source", None)
    evidence_base = (_descriptor_ref(unit),)
    if not keys and not declared_rows:
        return Reading(
            met=False,
            detail=(
                "the descriptor declares no S3 key template and no registry_rows this column can "
                f"grade (writes not graded: {not_graded})"
            ),
            evidence=evidence_base,
            unmeasurable=True,
            source="registry.d/units",
        )
    if source_obj is None:
        return Reading(
            met=False,
            detail=(
                "no ARTIFACT_REGISTRY source is configured for this store (a live S3 store reads the "
                "published copy; a local store needs --artifact-registry <path>)"
            ),
            evidence=evidence_base,
            unmeasurable=True,
            source="ARTIFACT_REGISTRY",
        )
    try:
        registry = source_obj.load()
    except SourceUnavailable as exc:
        return Reading(
            met=False,
            detail=f"could not read the artifact registry: {exc}",
            evidence=(source_obj.uri,),
            unmeasurable=True,
            source="ARTIFACT_REGISTRY",
        )

    by_template: dict[str, list[dict]] = {}
    for row in registry.artifacts:
        if str(row.get("s3_bucket") or "alpha-engine-research") != "alpha-engine-research":
            continue
        by_template.setdefault(_normalized(str(row.get("s3_key_template") or "")), []).append(row)
    prefixes = [str(g.get("path_prefix") or "") for g in registry.grandfathered if g.get("path_prefix")]

    registered: list[str] = []
    grandfathered: list[str] = []
    missing: list[str] = []
    parked: list[str] = []
    for key in keys:
        rows = by_template.get(_normalized(key), [])
        if rows:
            registered.append(f"{key} -> {rows[0].get('artifact_id')}")
            parked.extend(_parked(rows[0], unit))
            continue
        literal = _literal_prefix(key)
        cover = next((p for p in prefixes if literal.startswith(p)), None)
        if cover is not None:
            grandfathered.append(f"{key} (grandfathered {cover})")
        else:
            missing.append(key)
    unresolved: list[str] = []
    for artifact_id in declared_rows:
        row = registry.artifact(artifact_id)
        if row is None:
            unresolved.append(artifact_id)
        else:
            parked.extend(_parked(row, unit))
    parked = sorted(set(parked))

    problems: list[str] = []
    if missing:
        problems.append(f"published key(s) with NO registry row and no grandfathered prefix: {missing}")
    if unresolved:
        problems.append(f"registry_rows naming artifact_id(s) absent from the registry: {unresolved}")
    if parked:
        problems.append(f"row(s) parked while this unit is {unit.lifecycle!r}: {parked}")
    summary = f"registered {registered}; grandfathered {grandfathered}"
    if not_graded:
        summary += f"; not graded here: {not_graded}"
    evidence = (registry.uri, _descriptor_ref(unit))
    if problems:
        return Reading(
            met=False,
            detail=f"{'; '.join(problems)}. {summary}",
            evidence=evidence,
            source="ARTIFACT_REGISTRY",
        )
    return Reading(met=True, detail=summary, evidence=evidence, source="ARTIFACT_REGISTRY")


def _parked(row: dict, unit: Unit) -> list[str]:
    """A declared-off or non-in-service row covering an in-service unit's key."""
    if unit.lifecycle != "in-service":
        return []
    out: list[str] = []
    if row.get("declared_off"):
        out.append(f"{row.get('artifact_id')} (declared_off)")
    lifecycle = row.get("lifecycle")
    if lifecycle and str(lifecycle) != "in-service":
        out.append(f"{row.get('artifact_id')} (lifecycle {lifecycle})")
    return out


# ---------------------------------------------------------------------------
# consumers — each declared reader, resolved on its repository's default branch.
# ---------------------------------------------------------------------------

_CONSUMER_RE = re.compile(r"^([a-z0-9][a-z0-9-]*):(\S+)")


def _consumer_path(raw_path: str) -> str:
    path = raw_path.split("::", 1)[0]
    return re.sub(r":\d+$", "", path)


def read_consumers(store: GateStore, unit: Unit) -> Reading:
    """Every consumer the descriptor declares resolves to a reader file that exists.

    `repo:path` entries are checked on that repository's default branch via the
    GitHub contents API (``store.github_contents``, built from the workflow's own
    ``GITHUB_TOKEN``); this repository's own entries are checked in the tree.
    Existence of the reader file is what this column grades — whether that file
    still reads THIS key is the consumer pin `schema_contract` grades.
    """
    declared = [str(c).strip() for c in unit.raw.get("consumers") or []]
    ref = _descriptor_ref(unit)
    if not declared:
        reason = str(unit.raw.get("consumers_reason") or "").strip()
        return Reading(
            met=False,
            detail=(
                f"declares `consumers: []` — reason: {reason!r}. A key with no surviving consumer "
                "renders as a finding until it gets a retirement decision (plan §3)."
            ),
            evidence=(ref,),
            source="registry.d/units",
        )
    github = getattr(store, "github_contents", None)
    present: list[str] = []
    missing: list[str] = []
    not_a_file: list[str] = []
    unparseable: list[str] = []
    unmeasurable: list[str] = []
    for entry in declared:
        match = _CONSUMER_RE.match(entry)
        if not match:
            unparseable.append(entry)
            continue
        repo, path = match.group(1), _consumer_path(match.group(2))
        label = f"{repo}:{path}"
        if repo == "nousergon-data":
            target = REPO_ROOT / path
            if target.is_file():
                present.append(label)
            elif target.exists():
                not_a_file.append(label)
            else:
                missing.append(label)
            continue
        if github is None:
            unmeasurable.append(f"{label} (no GitHub reader configured; set {GITHUB_TOKEN_ENV})")
            continue
        try:
            kind = github.path_kind(repo, path)
        except SourceUnavailable as exc:
            unmeasurable.append(f"{label} ({exc})")
            continue
        if kind is None:
            missing.append(label)
        elif kind == "file":
            present.append(label)
        else:
            not_a_file.append(f"{label} ({kind})")

    evidence = tuple(sorted({e for e in declared})) + (ref,)
    parts: list[str] = []
    if missing:
        parts.append(f"declared reader(s) ABSENT from their repository's default branch: {missing}")
    if not_a_file:
        parts.append(f"declared consumer(s) that resolve to something other than a reader file: {not_a_file}")
    if unparseable:
        parts.append(f"consumer entr(y/ies) that are not a `repo:path` reference: {unparseable}")
    if unmeasurable:
        parts.append(f"could not resolve: {unmeasurable}")
    parts.append(f"resolved {len(present)}/{len(declared)}: {present}")
    source = "GitHub contents API (default branch)"
    if unmeasurable:
        return Reading(met=False, detail="; ".join(parts), evidence=evidence, unmeasurable=True, source=source)
    return Reading(
        met=not (missing or not_a_file or unparseable),
        detail="; ".join(parts),
        evidence=evidence,
        source=source,
    )


# ---------------------------------------------------------------------------
# identity — the declared writer role, graded by IAM policy simulation.
# ---------------------------------------------------------------------------


def _load_identities(path: pathlib.Path) -> dict[str, Any]:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or not isinstance(document.get("by_runs_on"), dict):
        raise ValueError(f"{path} carries no `by_runs_on` mapping")
    return document


def _simulate(client, role_arn: str, action: str, resources: list[str]) -> dict[str, str]:
    decisions: dict[str, str] = {}
    marker = None
    while True:
        kwargs: dict[str, Any] = {"PolicySourceArn": role_arn, "ActionNames": [action], "ResourceArns": resources}
        if marker:
            kwargs["Marker"] = marker
        response = client.simulate_principal_policy(**kwargs)
        for result in response.get("EvaluationResults") or []:
            decisions[str(result.get("EvalResourceName"))] = str(result.get("EvalDecision"))
        if not response.get("IsTruncated"):
            return decisions
        marker = response.get("Marker")


def read_identity(store: GateStore, unit: Unit, *, identities_path: pathlib.Path | None = None) -> Reading:
    """The unit's declared writer role may write every declared prefix, and nothing else.

    `alpha-engine-config-I10756`'s requirement: a workload identity scoped to the
    prefixes the unit declares, no whole-bucket wildcard, no bucket-wide Delete.
    Graded with ``iam:SimulatePrincipalPolicy`` against the LIVE role — the
    effective decision over inline, managed and boundary policies, which is what
    AWS enforces — rather than by text-matching a codified policy JSON the gate
    cannot check out (nous-ergon-ops is private) and which can drift from live
    (iam-drift-check.yml owns that comparison). Three questions:

    1. ``s3:PutObject`` on a concrete key of every declared template → allowed.
    2. ``s3:PutObject`` on an undeclared probe key → NOT allowed (catches a
       whole-bucket wildcard).
    3. ``s3:DeleteObject`` on the same probe → NOT allowed (catches bucket-wide
       Delete).
    """
    path = identities_path or WRITER_IDENTITIES_PATH
    config = _load_identities(path)
    runs_on = str((unit.raw.get("trigger") or {}).get("runs_on") or "")
    config_ref = path.relative_to(REPO_ROOT).as_posix() if path.is_relative_to(REPO_ROOT) else str(path)
    role = (config["by_runs_on"] or {}).get(runs_on)
    ref = _descriptor_ref(unit)
    if not role:
        return Reading(
            met=False,
            detail=(
                f"no workload identity is declared for runs_on={runs_on!r} in {config_ref} "
                f"(declared: {sorted(config['by_runs_on'])}). A unit whose writer identity is not "
                "declared cannot be shown to be scoped."
            ),
            evidence=(config_ref, ref),
            source=config_ref,
        )
    bucket = str(config["bucket"])
    keys, libraries, ungradable = s3_write_targets(unit)
    targets = sorted({_concrete(k) for k in keys} | {f"arcticdb/{lib}/gate-probe" for lib in libraries})
    role_arn = f"arn:aws:iam::{config['account_id']}:role/{role}"
    evidence = (f"iam:{role}", config_ref, ref)
    if not targets:
        return Reading(
            met=False,
            detail=f"the descriptor declares no S3 key or ArcticDB library to simulate (writes: {ungradable})",
            evidence=evidence,
            unmeasurable=True,
            source="iam:SimulatePrincipalPolicy",
        )
    client = getattr(store, "iam_client", None)
    if client is None:
        return Reading(
            met=False,
            detail=f"this store backend supplies no IAM client (only a live S3Store does); role {role}",
            evidence=evidence,
            unmeasurable=True,
            source="iam:SimulatePrincipalPolicy",
        )
    probe = f"arn:aws:s3:::{bucket}/{config['undeclared_probe_key']}"
    declared_arns = [f"arn:aws:s3:::{bucket}/{t}" for t in targets]
    try:
        writes = _simulate(client, role_arn, "s3:PutObject", declared_arns)
        probe_put = _simulate(client, role_arn, "s3:PutObject", [probe]).get(probe)
        probe_delete = _simulate(client, role_arn, "s3:DeleteObject", [probe]).get(probe)
    except Exception as exc:  # noqa: BLE001 - classified by AWS error code below
        # Deliberate: the failure mode is "this role could not be simulated";
        # every other clause survives; the recording surface is this row.
        code = ""
        response = getattr(exc, "response", None)
        if isinstance(response, dict):
            code = str((response.get("Error") or {}).get("Code") or "")
        if code == "NoSuchEntity":
            return Reading(
                met=False,
                detail=f"the declared writer role {role} does not exist (NoSuchEntity)",
                evidence=evidence,
                source="iam:SimulatePrincipalPolicy",
            )
        grant = (
            f"nous-ergon-ops infrastructure/iam/{GATE_ROLE}/data-gate-read.json needs "
            '{"Sid": "DataGateSimulateWriterIdentities", "Effect": "Allow", '
            f'"Action": "iam:SimulatePrincipalPolicy", "Resource": "{role_arn}"}}'
        )
        detail = f"could not simulate {role}: {type(exc).__name__}: {exc}"
        if code in {"AccessDenied", "AccessDeniedException"}:
            detail = f"AccessDenied simulating {role} as {GATE_ROLE}; {grant}"
        return Reading(
            met=False,
            detail=detail,
            evidence=evidence,
            unmeasurable=True,
            source="iam:SimulatePrincipalPolicy",
        )
    denied = sorted(arn.split(":::", 1)[1] for arn in declared_arns if writes.get(arn) != "allowed")
    problems: list[str] = []
    if denied:
        problems.append(f"{role} may NOT PutObject on declared key(s) {denied}")
    if probe_put == "allowed":
        problems.append(f"{role} may PutObject on the undeclared probe {config['undeclared_probe_key']} (a whole-bucket wildcard)")
    if probe_delete == "allowed":
        problems.append(f"{role} may DeleteObject on the undeclared probe (bucket-wide Delete)")
    summary = (
        f"simulated {role} for s3:PutObject on {len(targets)} declared target(s); undeclared probe "
        f"PutObject={probe_put}, DeleteObject={probe_delete}"
    )
    if ungradable:
        summary += f"; not simulated (not an S3 key in {bucket}): {ungradable}"
    if problems:
        return Reading(
            met=False, detail=f"{'; '.join(problems)}. {summary}", evidence=evidence, source="iam:SimulatePrincipalPolicy"
        )
    return Reading(met=True, detail=summary, evidence=evidence, source="iam:SimulatePrincipalPolicy")
