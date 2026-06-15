# Market-data source adapters — canonical price contract

**Authoritative reference for the provider-agnostic price record (`PriceBar`)
that every upstream vendor is normalized into, and the `PriceSourceAdapter`
port each vendor plugs into.**

`alpha-engine-data` is the SOLE market-data producer for the whole Nous Ergon
system; all other modules (Metron, predictor, research, backtester) are pure S3
consumers. Isolating the vendor behind this contract makes swapping or adding a
provider (yfinance / Polygon / FRED today; **Databento** / Twelve Data next) a
one-adapter change with zero downstream impact.

Status: stable. The companion test `tests/test_price_source_contract.py`
cross-checks the `PriceBar` dataclass against this file (§2) — a PR that adds or
renames a canonical field without updating SCHEMA.md fails CI. Mirrors the
feature-store contract (`features/SCHEMA.md`).

See alpha-engine-config#1082 for the design + rollout phases.

---

## 1. The contract, in one breath

- An **adapter** (`sources/<vendor>.py`) owns everything provider-specific: HTTP/SDK
  calls, response parsing, symbol-quirk mapping (`map_symbol`), rate-limit and error
  handling, and a declared `SourceCapabilities`.
- Every adapter exposes two fetch methods over the same logic: **`fetch_ohlcv(...)`**
  returns a clean list of **`PriceBar`** (the API-neutral record below); **`fetch_into(records, ...)`**
  is the pipeline-facing form that appends legacy record dicts to a passed list in
  place (preserving partial-on-error + `window_cache` semantics) and returns a count.
- **`collectors.daily_closes.collect()` dispatches its fetches through the registry**
  via `fetch_into`, selecting the adapter per role from injectable params
  (`equities_source` / `index_source` / `fallback_source`, defaults `polygon` / `fred`
  / `yfinance`). Swapping polygon→**databento** is one param + the new adapter — no
  change to `collect()`, the persisted artifacts, or any consumer.
- The orchestration core (chain selection, source-priority coalescing, validation,
  S3 + ArcticDB writes) is provider-agnostic and unchanged.

**Naming/units discipline** (inherited from `features/SCHEMA.md`): any NEW field
added to the canonical record that could be misread by a consumer MUST carry an
explicit units suffix (`_raw` / `_ratio` / `_pct` / `_zscore` / `_log_return`) or be
documented in §2 with its units. The OHLCV fields below are grandfathered with the
units stated.

---

## 2. `PriceBar` — canonical field catalog (authoritative)

| Field | Type / units | Notes |
|---|---|---|
| `ticker` | str | Canonical store-key (dash form for US class shares, e.g. `BRK-B`), caret-stripped. The persisted record always keeps this key regardless of the vendor's symbol convention. |
| `date` | str `YYYY-MM-DD` | Trading day of the bar. |
| `open` | float (price, `currency`) | Persisted column `Open`. |
| `high` | float (price, `currency`) | Persisted column `High`. |
| `low` | float (price, `currency`) | Persisted column `Low`. |
| `close` | float (price, `currency`) | Persisted column `Close`. |
| `adj_close` | float (price, `currency`) | Persisted column `Adj_Close`. Split/dividend-adjusted close where the source provides one (yfinance); equals `close` where it does not (Polygon, FRED). |
| `volume` | int (raw shares) | `0` when the source carries no volume (FRED single-value closes). |
| `source` | str | Provenance — the producing adapter's `name` (`polygon` / `fred` / `yfinance` / …). |
| `currency` | str (ISO-4217) | Native currency of the listing. Defaults `USD`. **Carried in-memory only in Phase 1a** — see §4. |
| `vwap` | float \| None (price) | True volume-weighted price. `None` when the source can't provide it — never a `(H+L+C)/3` proxy (2026-04-17 VWAP-centralization decision). Only Polygon supplies real VWAP today. |

`to_record()` / `from_record()` bridge `PriceBar` ↔ the legacy persisted dict
(`RECORD_KEYS` in `sources/contract.py`): keys
`ticker, date, Open, High, Low, Close, Adj_Close, Volume, VWAP, source`.

---

## 3. Adapters & capabilities (today)

| Adapter | `vwap` | `adjusted_close` | `intraday` | `regions` | `asset_classes` |
|---|---|---|---|---|---|
| `polygon` | ✓ | ✗ | ✓ | US | equity, etf |
| `fred` | ✗ | ✗ | ✗ | US | index, macro |
| `yfinance` | ✗ | ✓ | ✗ | global | equity, etf, index |
| `databento` *(planned)* | — | — | — | US | equity, etf |

`SourceCapabilities` lets the orchestrator route by need (e.g. only `vwap=True`
adapters are asked for true VWAP; international listings route to an adapter whose
`regions` cover the market).

---

## 4. `currency` materialization (deferred, by design)

Decision 2026-06-15: `currency` belongs in the canonical model now so that adding
international coverage later is an adapter/config change, not a schema migration.
But Phase 1a is strictly **additive** — it does NOT change the persisted
`staging/daily_closes/{date}.parquet` / ArcticDB schema. So `PriceBar` carries
`currency` in-memory, while `to_record()` omits it. Materializing `currency` to the
persisted artifact (additive column, default `USD`) rides the phase that introduces
non-USD listings or rewires `collect()` (Phase 1b+).

---

## 5. PR checklist

- **Adding a vendor:** implement `PriceSourceAdapter` in `sources/<vendor>.py`
  (own its HTTP/symbol-map/errors, declare `SourceCapabilities`, emit `PriceBar`),
  `register()` it, add it to the §3 table, and add a golden test (mock the vendor
  boundary → assert canonical `PriceBar` output).
- **Adding/renaming a canonical field:** update the `PriceBar` dataclass AND §2
  here (the contract test enforces parity) AND `to_record()`/`from_record()`; carry
  a units suffix per §1.
