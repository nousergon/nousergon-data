"""Daily panel publisher — first slice (alpha-engine-config-I10791, plan P-25).

Compiles the day's long price panel ONCE from the ArcticDB ``universe``
library and publishes it as the data collector's product, under the contract
in ``contracts/daily_panel.py``:

1. ``data_collection/panel/{trading_day}/panel.parquet`` — the panel;
2. ``data_collection/panel/{trading_day}/manifest.json`` — written LAST,
   naming the parquet's sha256, so a manifest is never there for a parquet
   that is not.

And, for the reader switch, one same-trading-day parity receipt:
``data_collection/panel/{trading_day}/parity.json``, comparing the published
panel with a panel the consumer compiled directly from ArcticDB for that
session (``contracts.daily_panel.compare_panels``).

**Refuse, never degrade.** A per-ticker empty frame, a panel with no rows on
its own trading day, a null or non-positive value, or a non-session date
raises :class:`PanelCompileError` before anything is written. The published
panel is either the contract or absent, and an absent panel is a red leg on
``data.phase3.daily_panel_adopted`` rather than a thin artifact every
consumer reads as fine.

**Entry points.** ``python -m builders.daily_panel publish|parity`` for an
operator, and :func:`run` for the collector's whole-mode unit D51
(``python weekly_collector.py --daily-panel``, ``weekly_collector.py::_run_daily_panel``),
which records a run manifest around it. The collector entry runs on
``ne-data-collection-panel``, started when ``ne-data-collection-eod`` SUCCEEDS
(alpha-engine-config-I10791), never inside the EOD machine whose terminal
event starts the eod_pnl reconcile. The box's ``data_collection/panel/*``
write grant is nous-ergon-ops-PR1668. The operator CLI below stays for
replays and for the parity receipt.

    python -m builders.daily_panel publish --date 2026-10-02 [--dry-run]
    python -m builders.daily_panel parity --date 2026-10-02 --consumer-panel s3://.../panel.parquet
    python -m builders.daily_panel history --name m-slot-2016-2022q1 --start 2000-01-01 --end 2022-03-31
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import json
import logging
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from contracts import daily_panel as dp

logger = logging.getLogger(__name__)

DEFAULT_BUCKET = "alpha-engine-research"
MODULE = "builders.daily_panel"

#: The calendar window the compile requests. Sized to cover the deepest
#: consumer: crucible's feature catalogue needs 313 sessions
#: (`crucible.features.min_panel_trading_days`), which its own sizing factor
#: puts at 486 calendar days; 600 is ~410 sessions. The manifest records the
#: depth actually published (`session_count`), and the consumer refuses a
#: panel too shallow for it — this default is never the check.
DEFAULT_LOOKBACK_CALENDAR_DAYS = 600

_OHLCV_RENAME = {
    "Open": "open_raw",
    "High": "high_raw",
    "Low": "low_raw",
    "Close": "close_raw",
    "Volume": "volume_raw",
}

Loader = Callable[..., dict[str, Any]]


class PanelCompileError(RuntimeError):
    """The panel could not be compiled to contract. Nothing is published."""


def _repo_code_sha() -> str:
    try:
        return subprocess.run(  # noqa: S607 - git resolved from PATH
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parent.parent,
            capture_output=True,
            text=True,
            check=True,
            timeout=10,
        ).stdout.strip()
    except Exception:  # noqa: BLE001 - provenance is recorded as missing, never invented
        return "unknown (git rev-parse failed)"


def frames_to_panel(frames: dict[str, Any], *, end: dt.date, lookback_days: int) -> pd.DataFrame:
    """``{ticker: ArcticDB OHLCV frame}`` -> the contract's long panel.

    Mirrors crucible's ``crucible/data/sources.py::normalize_panel`` step for
    step — tz-stripped index, window ``(end - lookback_days, end]``, duplicate
    (trading_day, ticker) collapsed ``keep="last"``, sorted — because parity
    is graded against that function's output, and a second normalization
    that differed would be the divergence the receipt exists to catch.
    """
    empty = sorted(t for t, f in frames.items() if f is None or len(f) == 0)
    if empty:
        raise PanelCompileError(
            f"ArcticDB returned empty frame(s) for {empty[:20]}: a per-ticker read failure is a "
            "partial outage, and dropping it would publish a panel thinner than the library"
        )
    start = end - dt.timedelta(days=lookback_days)
    blocks: list[pd.DataFrame] = []
    for ticker, frame in sorted(frames.items()):
        missing = [c for c in _OHLCV_RENAME if c not in frame.columns]
        if missing:
            raise PanelCompileError(f"{ticker}: ArcticDB frame lacks {missing}")
        block = frame[list(_OHLCV_RENAME)].rename(columns=_OHLCV_RENAME)
        index = pd.to_datetime(block.index)
        try:
            index = index.tz_localize(None)
        except TypeError:
            index = index.tz_convert(None)
        block = block.assign(trading_day=index.date, ticker=ticker).reset_index(drop=True)
        blocks.append(block[list(dp.PANEL_COLUMNS)])
    if not blocks:
        raise PanelCompileError("the universe library returned no symbols — an outage, not an empty market")
    panel = pd.concat(blocks, ignore_index=True)
    panel = panel[(panel["trading_day"] > start) & (panel["trading_day"] <= end)]
    panel = panel.drop_duplicates(subset=["trading_day", "ticker"], keep="last")
    panel = panel.sort_values(["trading_day", "ticker"]).reset_index(drop=True)
    for column in dp.VALUE_COLUMNS:
        panel[column] = panel[column].astype("float64")
    return panel


def compile_panel(
    bucket: str,
    *,
    trading_day: dt.date,
    lookback_days: int = DEFAULT_LOOKBACK_CALENDAR_DAYS,
    region: str | None = None,
    loader: Loader | None = None,
) -> pd.DataFrame:
    """Read every ``universe`` symbol over the window and return the contract panel."""
    from nousergon_lib.trading_calendar import is_trading_day  # pyright: ignore[reportAttributeAccessIssue]

    if not is_trading_day(trading_day):
        raise PanelCompileError(f"{trading_day} is not an NYSE session; there is no panel to publish for it")
    if loader is None:
        from nousergon_lib.arcticdb import load_universe_ohlcv as loader
    frames = loader(bucket, symbols=None, lookback_days=lookback_days, end=str(trading_day), region=region)
    if not frames:
        raise PanelCompileError(f"the universe library on {bucket!r} returned zero symbols for {trading_day}")
    panel = frames_to_panel(frames, end=trading_day, lookback_days=lookback_days)
    problems = dp.validate_panel_frame(panel, trading_day=trading_day)
    if problems:
        raise PanelCompileError(f"the compiled panel for {trading_day} breaks daily_panel.v1: {problems}")
    for record in dp.panel_row_records(panel, limit=50):
        row_problems = dp.schema_problems(record, "row")
        if row_problems:
            raise PanelCompileError(f"row {record} breaks daily_panel.schema.json: {row_problems}")
    return panel


def serialize(panel: pd.DataFrame) -> bytes:
    """Parquet with a fixed codec and no index, so one panel is one sha256."""
    buffer = io.BytesIO()
    panel.to_parquet(buffer, index=False, compression="snappy")
    return buffer.getvalue()


def read_panel(payload: bytes) -> pd.DataFrame:
    """A published (or consumer) panel parquet, ``trading_day`` back as ``date``."""
    panel = pd.read_parquet(io.BytesIO(payload))
    panel["trading_day"] = pd.to_datetime(panel["trading_day"]).dt.date
    return panel


def publish(
    panel: pd.DataFrame,
    *,
    trading_day: dt.date,
    lookback_days: int,
    put: Callable[[str, bytes, str], None],
    code_sha: str | None = None,
    now: dt.datetime | None = None,
) -> dict:
    """Write the parquet, then the manifest; return the manifest.

    ``put(key, body, content_type)`` is the one write seam (S3 in production,
    a dict in tests, a directory under ``--dry-run``). The manifest is
    validated against its schema BEFORE either write, so a sidecar the gate
    would refuse is never published beside a good parquet.
    """
    payload = serialize(panel)
    manifest = dp.build_manifest(
        panel,
        payload,
        trading_day=trading_day,
        lookback_calendar_days=lookback_days,
        module=MODULE,
        code_sha=code_sha or _repo_code_sha(),
        generated_at=now or dt.datetime.now(dt.timezone.utc),
    )
    problems = dp.schema_problems(manifest, "manifest")
    if problems:
        raise PanelCompileError(f"refusing to publish: manifest breaks its schema: {problems}")
    put(dp.panel_key(trading_day), payload, "application/vnd.apache.parquet")
    put(dp.manifest_key(trading_day), json.dumps(manifest, indent=2).encode(), "application/json")
    return manifest


def parity(
    *,
    trading_day: dt.date,
    get: Callable[[str], bytes],
    consumer_payload: bytes,
    consumer_key: str,
    put: Callable[[str, bytes, str], None],
    now: dt.datetime | None = None,
) -> dict:
    """Compare the published panel for ``trading_day`` with a consumer's; write the receipt.

    The producer side is read back from the published key and checked against
    its own manifest's sha256 first — a receipt about bytes that are not the
    published panel would grade the wrong artifact.
    """
    manifest = json.loads(get(dp.manifest_key(trading_day)))
    payload = get(dp.panel_key(trading_day))
    if dp.sha256_hex(payload) != manifest["panel_sha256"]:
        raise PanelCompileError(
            f"{dp.panel_key(trading_day)} does not match its manifest's sha256 — the publish is "
            "incomplete or was overwritten; republish before comparing"
        )
    receipt = dp.compare_panels(
        read_panel(payload),
        read_panel(consumer_payload),
        trading_day=trading_day,
        producer_key=dp.panel_key(trading_day),
        producer_sha256=manifest["panel_sha256"],
        consumer_key=consumer_key,
        consumer_sha256=dp.sha256_hex(consumer_payload),
        generated_at=now or dt.datetime.now(dt.timezone.utc),
    )
    problems = dp.schema_problems(receipt, "parity")
    if problems:
        raise PanelCompileError(f"refusing to publish: parity receipt breaks its schema: {problems}")
    put(dp.parity_key(trading_day), json.dumps(receipt, indent=2).encode(), "application/json")
    return receipt


def run(
    bucket: str,
    *,
    trading_day: dt.date,
    lookback_days: int = DEFAULT_LOOKBACK_CALENDAR_DAYS,
    region: str | None = None,
    dry_run: bool = False,
    loader: Loader | None = None,
    put: Callable[[str, bytes, str], None] | None = None,
) -> dict:
    """Compile and publish one session's panel; return a collector-shaped result.

    The collector's entry (``weekly_collector.py --daily-panel``). It never
    raises on a contract refusal: :class:`PanelCompileError` becomes
    ``status: error`` with the reason, so the whole-mode wrapper records the
    unit ``failed`` and ``main`` exits 1 rather than a traceback standing in
    for a verdict. Nothing is written on that path.

    ``dry_run`` compiles and validates (reads are real) and writes nothing,
    status ``ok_dry_run``. On success the result names the two keys written and
    the counts the manifest carries, so the run manifest records the outputs
    from what was actually published, not from the descriptor.
    """
    result: dict[str, Any] = {"status": "error", "trading_day": str(trading_day)}
    try:
        panel = compile_panel(
            bucket, trading_day=trading_day, lookback_days=lookback_days, region=region, loader=loader
        )
        result["rows"] = int(len(panel))
        if dry_run:
            result["status"] = "ok_dry_run"
            return result
        if put is None:
            _get, put = _s3_io(bucket, None)
        manifest = publish(panel, trading_day=trading_day, lookback_days=lookback_days, put=put)
    except PanelCompileError as exc:
        logger.error("%s", exc)
        result["error"] = str(exc)
        return result
    result.update(
        status="ok",
        rows=int(manifest["row_count"]),
        panel_key=dp.panel_key(trading_day),
        manifest_key=dp.manifest_key(trading_day),
        panel_sha256=manifest["panel_sha256"],
    )
    return result


# -- frozen history export ---------------------------------------------------
#
# A one-off, operator-started read of everything ArcticDB holds up to a cutoff,
# frozen to one parquet so a backtest can hold out a window the live libraries
# will later drop: `universe` is a rolling ten-year window the weekly rebuild
# slides forward (builders/backfill.py), so its oldest sessions disappear every
# Saturday. First use: the model zoo's M-slot confirmation window, 2016 to
# 2021 (alpha-engine-config-I12399). Never a scheduled output, so D51 declares
# the key under `completeness.operator_writes`.

HISTORY_KEY_PREFIX = f"{dp.KEY_PREFIX}history/"
HISTORY_SCHEMA_VERSION = "panel_history_export.v1"
DELISTED_HISTORY_LIB = "delisted_history"
CLASSIFICATION_KEY = "market_data/universe_classification/latest.json"
_HISTORY_EXTRA_COLUMNS = {"VWAP": "vwap_raw", "source": "source"}
HISTORY_COLUMNS: tuple[str, ...] = (
    "trading_day",
    "ticker",
    *_OHLCV_RENAME.values(),
    "vwap_raw",
    "source",
    "library",
    "delisted",
    "sector",
)


def history_key(name: str, leaf: str) -> str:
    if not name or "/" in name or name.startswith("."):
        raise PanelCompileError(f"history export name {name!r} must be one plain path segment")
    return f"{HISTORY_KEY_PREFIX}{name}/{leaf}"


def frames_to_history(
    frames: dict[str, Any],
    *,
    library: str,
    start: dt.date,
    end: dt.date,
    sectors: dict[str, str | None],
) -> tuple[pd.DataFrame, dict[str, str]]:
    """``{ticker: frame}`` -> long history rows in ``[start, end]``, plus ``{ticker: why}`` skips.

    Unlike the daily panel this does not refuse the whole export on one bad
    symbol: a delisted name backfilled from a free source can lack a column,
    and the export says so per ticker in its manifest instead of either
    dropping it silently or publishing nothing.
    """
    skipped: dict[str, str] = {}
    blocks: list[pd.DataFrame] = []
    for ticker, frame in sorted(frames.items()):
        if frame is None or len(frame) == 0:
            skipped[ticker] = "read failed or empty frame"
            continue
        missing = [c for c in _OHLCV_RENAME if c not in frame.columns]
        if missing:
            skipped[ticker] = f"lacks {missing}"
            continue
        block = frame[list(_OHLCV_RENAME)].rename(columns=_OHLCV_RENAME)
        for column, out in _HISTORY_EXTRA_COLUMNS.items():
            block[out] = frame[column] if column in frame.columns else None
        index = pd.to_datetime(frame.index)
        try:
            index = index.tz_localize(None)
        except TypeError:
            index = index.tz_convert(None)
        block = block.assign(
            trading_day=index.date,
            ticker=ticker,
            library=library,
            delisted=library == DELISTED_HISTORY_LIB,
            sector=sectors.get(ticker),
        ).reset_index(drop=True)
        block = block[(block["trading_day"] >= start) & (block["trading_day"] <= end)]
        if block.empty:
            skipped[ticker] = f"no rows in {start}..{end}"
            continue
        blocks.append(block[list(HISTORY_COLUMNS)])
    if not blocks:
        return pd.DataFrame(columns=list(HISTORY_COLUMNS)), skipped
    history = pd.concat(blocks, ignore_index=True)
    history = history.drop_duplicates(subset=["trading_day", "ticker", "library"], keep="last")
    for column in (*_OHLCV_RENAME.values(), "vwap_raw"):
        history[column] = history[column].astype("float64")
    history["source"] = history["source"].astype("string")
    history["sector"] = history["sector"].astype("string")
    return history, skipped


def _coverage(history: pd.DataFrame) -> dict[str, Any]:
    if history.empty:
        return {"rows": 0, "symbols": 0, "first_day": None, "last_day": None}
    return {
        "rows": int(len(history)),
        "symbols": int(history["ticker"].nunique()),
        "first_day": str(history["trading_day"].min()),
        "last_day": str(history["trading_day"].max()),
    }


def export_history(
    *,
    name: str,
    start: dt.date,
    end: dt.date,
    load: Callable[[str], dict[str, Any]],
    sectors: dict[str, str | None],
    put: Callable[[str, bytes, str], None],
    code_sha: str | None = None,
    now: dt.datetime | None = None,
) -> dict:
    """Read both libraries through ``load(library)``, write the parquet, then the manifest.

    Refuses (writes nothing) when either library returns no symbols or no row
    falls in the window: an empty library is an outage, not an empty history.
    """
    if start > end:
        raise PanelCompileError(f"history export start {start} is after end {end}")
    parts: list[pd.DataFrame] = []
    libraries: dict[str, Any] = {}
    for library in ("universe", DELISTED_HISTORY_LIB):
        frames = load(library)
        if not frames:
            raise PanelCompileError(f"the {library!r} library returned zero symbols; refusing a partial export")
        part, skipped = frames_to_history(frames, library=library, start=start, end=end, sectors=sectors)
        libraries[library] = {
            **_coverage(part),
            "symbols_listed": len(frames),
            "skipped": skipped,
            "per_ticker": {
                str(t): [str(g.min()), str(g.max())]
                for t, g in part.groupby("ticker")["trading_day"]
            },
        }
        parts.append(part)
    history = pd.concat(parts, ignore_index=True).sort_values(["trading_day", "library", "ticker"])
    history = history.reset_index(drop=True)
    if history.empty:
        raise PanelCompileError(f"no row in either library falls in {start}..{end}")
    payload = serialize(history)
    manifest = {
        "schema_version": HISTORY_SCHEMA_VERSION,
        "name": name,
        "window": {"start": str(start), "end": str(end)},
        "parquet_key": history_key(name, "history.parquet"),
        "parquet_sha256": dp.sha256_hex(payload),
        "columns": list(HISTORY_COLUMNS),
        **_coverage(history),
        "libraries": libraries,
        "sector_source": f"{CLASSIFICATION_KEY} (current classification, not point-in-time)",
        "notes": [
            "Prices are the *_raw ArcticDB columns, copied unconverted; dollar volume is close_raw * volume_raw.",
            "universe is a rolling ten-year window; its first_day is where that window stood at export time.",
            "delisted_history holds only the names the pruner retained or the backfill recovered; it is not "
            "a complete survivorship-free set.",
        ],
        "module": MODULE,
        "code_sha": code_sha or _repo_code_sha(),
        "generated_at": (now or dt.datetime.now(dt.timezone.utc)).isoformat(),
    }
    put(history_key(name, "history.parquet"), payload, "application/vnd.apache.parquet")
    put(history_key(name, "manifest.json"), json.dumps(manifest, indent=2).encode(), "application/json")
    return manifest


def _arctic_history_loader(bucket: str, *, start: dt.date, end: dt.date, region: str | None):
    """``load(library)`` over the live libraries, read-only (never creates a library)."""
    from nousergon_lib import arcticdb as nl_adb

    lookback_days = (end - start).days + 1

    full = [*_OHLCV_RENAME, *_HISTORY_EXTRA_COLUMNS]

    def load(library: str) -> dict[str, Any]:
        """Every listed symbol, mapped to its frame or to None when no read succeeded.

        Narrow columns first (universe frames also carry ~100 feature
        columns), then OHLCV only for a symbol whose frame lacks VWAP or
        source. A symbol that fails both stays in the map as None, so the
        export names it as skipped instead of losing it.
        """
        lib = nl_adb.open_arctic(bucket, region=region).get_library(library, create_if_missing=False)
        listed = sorted(lib.list_symbols())

        def read(symbols: list[str], columns: list[str]) -> dict[str, Any]:
            if not symbols:
                return {}
            return nl_adb._load_arctic_frames(  # noqa: SLF001 - the shared read core both public readers use
                lib, symbols, lookback_days=lookback_days, end=str(end), columns=columns, label=library
            )

        frames = read(listed, full)
        frames.update(read([t for t in listed if t not in frames], list(_OHLCV_RENAME)))
        return {t: frames.get(t) for t in listed}

    return load


def _sectors(get: Callable[[str], bytes]) -> dict[str, str | None]:
    doc = json.loads(get(CLASSIFICATION_KEY))
    return {str(t): (row or {}).get("sector") for t, row in (doc.get("data") or {}).items()}


# -- CLI --------------------------------------------------------------------


def published_manifest_keys(bucket: str, *, s3: Any = None) -> set[str]:
    """Every ``panel/{day}/manifest.json`` key on the bucket, from one listing.

    The catch-up in ``weekly_collector.py --daily-panel`` (alpha-engine-config-
    I12652) reads this to find the sessions with no published panel. A listing
    that fails RAISES: an unreadable prefix must never read as "nothing is
    missing", nor as "everything is missing" and trigger a burst of compiles.
    """
    if s3 is None:
        import boto3

        s3 = boto3.client("s3")
    keys: set[str] = set()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=dp.KEY_PREFIX):
        for obj in page.get("Contents") or []:
            if obj["Key"].endswith("/manifest.json"):
                keys.add(obj["Key"])
    return keys


def _s3_io(bucket: str, dry_run_dir: Path | None):
    """``(get, put)`` over the bucket; ``put`` writes under ``dry_run_dir`` when set."""
    import boto3

    s3 = boto3.client("s3")

    def get(key: str) -> bytes:
        if dry_run_dir is not None and (dry_run_dir / key).is_file():
            return (dry_run_dir / key).read_bytes()
        return s3.get_object(Bucket=bucket, Key=key)["Body"].read()

    def put(key: str, body: bytes, content_type: str) -> None:
        if dry_run_dir is not None:
            path = dry_run_dir / key
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
            logger.info("dry-run: wrote %s (%d bytes) locally, not to s3://%s", path, len(body), bucket)
            return
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)
        logger.info("wrote s3://%s/%s (%d bytes)", bucket, key, len(body))

    return get, put


def _read_uri(uri: str) -> bytes:
    if uri.startswith("s3://"):
        import boto3

        bucket, _, key = uri[len("s3://") :].partition("/")
        return boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    return Path(uri).read_bytes()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m builders.daily_panel", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("publish", "parity"):
        p = sub.add_parser(name)
        p.add_argument("--date", required=True, type=dt.date.fromisoformat, help="the NYSE session (YYYY-MM-DD)")
        p.add_argument("--bucket", default=DEFAULT_BUCKET)
        p.add_argument(
            "--dry-run",
            metavar="DIR",
            nargs="?",
            const="daily_panel_dry_run",
            default=None,
            help="write under DIR (default ./daily_panel_dry_run) instead of S3; reads are real",
        )
    sub.choices["publish"].add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_CALENDAR_DAYS)
    sub.choices["publish"].add_argument("--region", default=None)
    sub.choices["parity"].add_argument(
        "--consumer-panel", required=True, help="s3:// URI or path of the consumer's directly compiled panel"
    )
    history = sub.add_parser("history", help="one-off frozen export of both libraries up to --end")
    history.add_argument("--name", required=True, help="export name, one path segment")
    history.add_argument("--start", required=True, type=dt.date.fromisoformat)
    history.add_argument("--end", required=True, type=dt.date.fromisoformat)
    history.add_argument("--bucket", default=DEFAULT_BUCKET)
    history.add_argument("--region", default=None)
    history.add_argument("--dry-run", metavar="DIR", nargs="?", const="daily_panel_dry_run", default=None)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    dry_run_dir = Path(args.dry_run) if args.dry_run else None
    get, put = _s3_io(args.bucket, dry_run_dir)
    try:
        if args.command == "history":
            manifest = export_history(
                name=args.name,
                start=args.start,
                end=args.end,
                load=_arctic_history_loader(args.bucket, start=args.start, end=args.end, region=args.region),
                sectors=_sectors(get),
                put=put,
            )
            result = {k: v for k, v in manifest.items() if k not in ("libraries", "notes", "columns")}
            result["libraries"] = {
                lib: {k: v for k, v in meta.items() if k != "per_ticker"}
                for lib, meta in manifest["libraries"].items()
            }
        elif args.command == "publish":
            panel = compile_panel(
                args.bucket, trading_day=args.date, lookback_days=args.lookback_days, region=args.region
            )
            result = publish(panel, trading_day=args.date, lookback_days=args.lookback_days, put=put)
        else:
            result = parity(
                trading_day=args.date,
                get=get,
                consumer_payload=_read_uri(args.consumer_panel),
                consumer_key=args.consumer_panel,
                put=put,
            )
    except PanelCompileError as exc:
        logger.error("%s", exc)
        return 1
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
