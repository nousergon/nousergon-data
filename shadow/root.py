"""The shadow output root: where a pre-cutover shadow run is allowed to write.

Plan `data_collection_plan_260914.md` §6.2 step 4, `alpha-engine-config-I10778`.

**What this is for.** Before the standalone collection stack takes over from the
v1 Step Functions pipelines, we need evidence that it produces the same bytes.
The strangler-fig way to get that evidence is a *shadow run*: one standalone
execution that reads the same inputs and writes its entire output somewhere
nobody consumes, so each key can be diffed against the same trading day's v1
output. The thing that makes it safe is not care — it is that a shadow run
**cannot** write a live key.

**Two redirect surfaces, because there are two writers.**

1. **S3 through boto3** — every published key in this repository is written by
   a ``put_object`` (parquet is serialised into a ``BytesIO`` first; nothing
   here writes through ``s3fs``). ``shadow.interceptor`` rewrites the key of
   every mutating S3 call to sit under ``staging/shadow/{trading_day}/`` and
   RAISES on anything it cannot rewrite.
2. **ArcticDB** — ArcticDB does not use botocore at all; it carries its own
   C++ S3 client, so the interceptor is blind to it. Skipping ArcticDB would
   make the shadow run a partial test of the thing being cut over, so instead
   the *library name* is redirected: ``universe`` becomes
   ``shadow_20260912_universe``. A shadow ArcticDB write therefore lands under
   ``arcticdb/shadow_20260912_universe/``, which is **not** under the shadow
   S3 prefix — see ``INVARIANT`` below, which states the invariant in the only
   form that is true of both surfaces.

``INVARIANT``: under an active shadow root, no write reaches a key or an
ArcticDB library that the live pipelines read. Concretely: every boto3 S3
mutation lands under ``staging/shadow/{trading_day}/``, and every ArcticDB
library name carries the ``shadow_{YYYYMMDD}_`` prefix, which
``LIVE_ARCTIC_LIBRARIES`` can never be a member of. That is the
no-double-write property plan §6.2 step 4 requires, and
``tests/test_shadow_root.py`` is where it is asserted rather than asserted
here in prose.

**ArcticDB reads: seeded from live, never from nothing** (`alpha-engine-config-I10866`).
The name redirect applies to reads as well as writes, so without seeding, a
shadow library is EMPTY. The first real shadow run (2026-09-15, trading day
2026-09-14) read an empty ``shadow_20260914_universe_schema_meta`` as baseline
v0 and was refused at the pre-append assert. Deleting that assert would have
been worse: ``daily_append`` would have computed every rolling and z-score
column over a single bar, and the parity diff would have been read as
evidence. S3 does not have this problem, because the interceptor passes reads
through to live. ArcticDB needs the equivalent: the shadow run must read the
same live state the live run reads.

*Mechanism chosen: a bounded whole-library seed on first open*
(``shadow/arctic_seed.py::ensure_seeded``, called from
``store/arctic_store.py::_open_library``). On the first shadow open for a
stamp, every ``LIVE_ARCTIC_LIBRARIES`` member is copied through ArcticDB
``read_batch`` (live, via a read-only wrapper) and ``write_batch`` (shadow).
The copy keeps only rows strictly before the trading day
(``date_range=(None, trading_day - 1ns)``), is chunked at 50 symbols, keeps
the live library's ``LibraryOptions``, and keeps each symbol's metadata. The
data libraries are verified (symbol set plus per-symbol row count) BEFORE the
live schema stamp is copied verbatim, and the seed manifest is committed LAST.
The alternatives, and why they were rejected:

* *Copy-on-first-access per symbol* needs an interposer on every Library
  method (``read``, ``read_batch``, ``tail``, ``list_symbols``,
  ``update_batch``, ...). ``daily_append`` reads the whole universe through
  ``list_symbols`` + ``read_batch`` anyway, so the lazy scheme copies the same
  bytes behind a much larger surface, and any method it forgets reads empty.
  That is this defect again, one method at a time.
* *``read(as_of=...)`` redirect of reads to live* offers no row-level as-of:
  live writes use ``prune_previous_versions=True``, so earlier versions do not
  exist. It would also mix live-read and shadow-write handles inside one
  producer function that holds a single ``Library`` object.
* *Stamping the shadow meta library at the expected version* fakes exactly
  what the assert exists to catch. It is never done. The shadow stamp is a
  byte copy of the live stamp, written only after the data it describes has
  been copied and verified. If live is unstamped, shadow stays unstamped, and
  both read as baseline through the same code path.

Idempotent per stamp: a committed ``shadow_{stamp}_seed_manifest`` makes every
later open (the other legs of ``shadow-weekday``) two metadata reads. No
manifest means no completed seed, so every existing ``shadow_{stamp}_*``
library is deleted and the copy starts from nothing. That includes the empty
libraries the 2026-09-15 run created.

*Budget.* The universe holds about 900 symbols × about 2,500 daily rows.
Measured in-region, sequentially: a full-series read costs about 0.3-0.5 s per
symbol (``builders/daily_append.py`` step 4a comment), and a full-series write
costs about 1.5 s per symbol (``daily_append`` docstring, 904 × 1.5 s ≈ 22 min).
Even sequentially, a whole-universe copy is therefore bounded near 30 minutes.
``read_batch``/``write_batch`` parallelise the S3 I/O, so the real number is
lower. ``macro`` (tens of symbols) and ``delisted_history`` add minutes. The
seed carries its own 45-minute budget (``DATA_COLLECTION_SHADOW_SEED_BUDGET_SECONDS``)
and raises on overrun, instead of being cut off silently by the box's timer.
The ``shadow-weekday`` dispatcher workload gets an 18,000 s (5 h) runtime cap,
so the SSM ``executionTimeout`` and the box's hard-stop timer both cover it.
The shared 7,200 s default could not cover the four chained legs
(``morning-enrich`` about 50 min + ``morning-arctic-append`` about 38 min +
``post-market-data`` + ``daily-arctic-append`` about 38 min) even before the
seed was added.

**Fail loud.** Nothing in this module degrades. An S3 operation nobody
classified, a key that will not rewrite, an ArcticDB library name that
collides with a live one — each raises ``ShadowGuardViolation`` and kills the
run. A shadow harness that silently lets one write through is worse than no
harness, because the parity report it produces would be read as evidence.
"""

from __future__ import annotations

import datetime as dt
import os
import re
import threading
from dataclasses import dataclass

__all__ = [
    "ARCTIC_SHADOW_PREFIX_TEMPLATE",
    "ENV_TRADING_DAY",
    "LIVE_ARCTIC_LIBRARIES",
    "SHADOW_ROOT_TEMPLATE",
    "ShadowGuardViolation",
    "ShadowRoot",
    "activate",
    "activate_from_env",
    "active_root",
    "deactivate",
    "root_from_env",
    "shadow_arctic_library",
]

#: The output root, exactly as plan §6.2 step 4 writes it. One template, read
#: by the writer side (this module) and by the reader side
#: (`shadow.parity`), so the two cannot drift into diffing a prefix nothing
#: wrote.
SHADOW_ROOT_TEMPLATE = "staging/shadow/{trading_day}/"

#: ArcticDB library prefix. Date-stamped for the same reason the S3 root is:
#: two shadow runs on different trading days must not share a library, or the
#: second one's parity report grades the first one's rows.
ARCTIC_SHADOW_PREFIX_TEMPLATE = "shadow_{stamp}_"

#: The ArcticDB libraries the live pipelines read. A shadow library name that
#: equals one of these is a bug in this module, not a configuration choice, so
#: it raises rather than being corrected.
LIVE_ARCTIC_LIBRARIES: frozenset[str] = frozenset(
    {"universe", "macro", "delisted_history", "universe_schema_meta"}
)

#: Set by `python -m shadow run` for the child process; also honoured by
#: `activate_from_env` so a container entrypoint that cannot be wrapped can
#: still opt in. Absent means "not a shadow run" — there is no default-on.
ENV_TRADING_DAY = "DATA_COLLECTION_SHADOW_TRADING_DAY"

_LOCK = threading.Lock()
_ACTIVE: "ShadowRoot | None" = None


class ShadowGuardViolation(RuntimeError):
    """A write under an active shadow root that would not land in the shadow.

    Always fatal. The shadow run's whole value is the guarantee in
    ``INVARIANT``; a violation means the guarantee does not hold, and a run
    that continues past it produces a parity report that cannot be trusted.
    """


@dataclass(frozen=True)
class ShadowRoot:
    """One trading day's shadow output root."""

    trading_day: dt.date

    @property
    def prefix(self) -> str:
        """``staging/shadow/2026-09-12/`` — always with the trailing slash."""
        return SHADOW_ROOT_TEMPLATE.format(trading_day=self.trading_day.isoformat())

    @property
    def arctic_prefix(self) -> str:
        return ARCTIC_SHADOW_PREFIX_TEMPLATE.format(stamp=self.trading_day.strftime("%Y%m%d"))

    def key(self, key: str) -> str:
        """The shadow key for ``key``. Idempotent; raises rather than guessing."""
        if not isinstance(key, str):
            raise ShadowGuardViolation(
                f"refusing to rewrite a non-string S3 key under the shadow root: {key!r}"
            )
        cleaned = key.lstrip("/")
        if not cleaned:
            raise ShadowGuardViolation("refusing to rewrite an empty S3 key under the shadow root")
        if cleaned.startswith(self.prefix):
            return cleaned
        return self.prefix + cleaned

    def live_key(self, shadow_key: str) -> str:
        """The live key a shadow key shadows — the inverse of :meth:`key`."""
        if not shadow_key.startswith(self.prefix):
            raise ShadowGuardViolation(
                f"{shadow_key!r} is not under the shadow root {self.prefix!r}"
            )
        return shadow_key[len(self.prefix) :]

    def arctic_library(self, name: str) -> str:
        """The shadow ArcticDB library name for ``name``. Idempotent."""
        if not name:
            raise ShadowGuardViolation("refusing to shadow an empty ArcticDB library name")
        shadowed = name if name.startswith(self.arctic_prefix) else self.arctic_prefix + name
        if shadowed in LIVE_ARCTIC_LIBRARIES:
            raise ShadowGuardViolation(
                f"the shadow library name {shadowed!r} collides with a LIVE library "
                f"({sorted(LIVE_ARCTIC_LIBRARIES)}) — refusing rather than writing it"
            )
        return shadowed

    def assert_shadow_key(self, key: str, *, operation: str, bucket: str | None = None) -> None:
        """The guard itself: the key that is about to be written is in the shadow.

        Called after every rewrite, not instead of it. Rewriting is the
        mechanism; this is the *proof obligation* — if a future S3 operation
        carries its key somewhere this module does not know about, the rewrite
        silently no-ops and this raises.
        """
        if not isinstance(key, str) or not key.startswith(self.prefix):
            raise ShadowGuardViolation(
                f"{operation} would write s3://{bucket or '?'}/{key!r}, which is outside the "
                f"shadow root {self.prefix!r}. A shadow run may not touch a live key "
                f"(plan §6.2 step 4, alpha-engine-config-I10778)."
            )


def root_from_env(env: "dict[str, str] | None" = None) -> "ShadowRoot | None":
    """The shadow root this process is configured for, or ``None``.

    ``None`` is the normal production answer: a shadow run is opt-in, and an
    unparseable value is an error rather than a silent opt-out — a typo'd date
    that quietly ran LIVE is the failure this refuses.
    """
    source = os.environ if env is None else env
    raw = (source.get(ENV_TRADING_DAY) or "").strip()
    if not raw:
        return None
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        raise ShadowGuardViolation(
            f"{ENV_TRADING_DAY}={raw!r} is not an ISO trading day. Refusing to start: a run "
            "with an unreadable shadow root would write live keys."
        )
    return ShadowRoot(dt.date.fromisoformat(raw))


def activate(root: ShadowRoot) -> ShadowRoot:
    """Install the redirect for this process. Idempotent for the same root."""
    from shadow import interceptor

    global _ACTIVE
    with _LOCK:
        if _ACTIVE is not None and _ACTIVE != root:
            raise ShadowGuardViolation(
                f"a shadow root is already active for {_ACTIVE.trading_day}; refusing to "
                f"switch to {root.trading_day} mid-process"
            )
        _ACTIVE = root
    interceptor.install()
    return root


def activate_from_env(env: "dict[str, str] | None" = None) -> "ShadowRoot | None":
    """Activate if ``ENV_TRADING_DAY`` is set. Returns the root, or ``None``."""
    root = root_from_env(env)
    if root is None:
        return None
    return activate(root)


def deactivate() -> None:
    """Remove the redirect. For tests and for the runner's own teardown."""
    from shadow import interceptor

    global _ACTIVE
    with _LOCK:
        _ACTIVE = None
    interceptor.uninstall()


def active_root() -> "ShadowRoot | None":
    """The active shadow root, or ``None`` when this is a normal run."""
    return _ACTIVE


def shadow_arctic_library(name: str) -> str:
    """``name`` under the active shadow root, or ``name`` unchanged.

    The one call `store/arctic_store.py` makes. Returning the name unchanged
    when no root is active is what keeps the production path byte-identical to
    what it is today: outside a shadow run this function is the identity.
    """
    root = active_root()
    return name if root is None else root.arctic_library(name)
