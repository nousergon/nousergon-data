"""Loughran-McDonald finance-domain sentiment scoring.

The Loughran-McDonald (2011, Journal of Finance) sentiment dictionary
is the academic gold standard for finance-domain sentiment. General-
domain dictionaries (e.g. Harvard IV) systematically misclassify
finance vocabulary — "liability", "depreciation", "tax", "vice
president" all read as negative in general-domain but are neutral
finance terms. LM was built specifically on 10-K filings to correct
this.

Dictionary categories (we use the first 3 for composite scoring):

  positive       — clearly positive sentiment in finance context
  negative       — clearly negative sentiment in finance context
  uncertainty    — hedging language ("approximately", "perhaps")
  litigious      — legal-action vocabulary
  modal_strong   — assertive language ("must", "will")
  modal_weak     — hedged language ("may", "could")
  constraining   — restriction vocabulary ("required to", "limited")

License: Loughran-McDonald Master Dictionary is freely available for
academic and research use from Bill McDonald's site at Notre Dame
(https://sraf.nd.edu/loughranmcdonald-master-dictionary/). Bundle the
canonical CSV under ``collectors/nlp/data/lm_master_dict.csv`` via
the operator-run ``scripts/download_lm_dict.py`` script. The constructor
accepts the loaded dict so tests use a synthetic 5-word fixture.

Composite formula (standard LM-paper convention):

    composite = (positive_count - negative_count) / max(total_tokens, 1)

clipped to [-1, +1]. Some practitioners normalize by (positive +
negative) for a "polarity-among-sentiment-words" reading; we use the
denominator-over-total convention because it preserves "neutral
articles get small magnitude" semantics that compose better with
trust-weighted aggregation.
"""

from __future__ import annotations

import csv
import logging
import re
from pathlib import Path

from collectors.nlp.protocols import SentimentScore

logger = logging.getLogger(__name__)


class LmDictUnavailable(RuntimeError):
    """The Loughran-McDonald master dictionary could not be made present
    (missing locally AND the S3 fallback fetch failed). Producers should
    treat this as a HARD failure rather than silently scoring all-zero
    sentiment — see ``ensure_lm_master_dict`` + [[feedback_no_silent_fails]]."""


# ── Lexicon I/O ────────────────────────────────────────────────────────


_DEFAULT_LM_PATH = (
    Path(__file__).parent / "data" / "lm_master_dict.csv"
)

# Durable fallback source for the dict (the upstream Notre Dame Google-Drive
# URL rotates and 404s — see L4575). We mirror the canonical CSV into our own
# private S3 so any news-producing host can self-heal a missing local copy.
_S3_DICT_BUCKET = "alpha-engine-research"
_S3_DICT_KEY = "reference/nlp/lm_master_dict.csv"


def ensure_lm_master_dict(
    path: Path | None = None,
    *,
    s3_client=None,
    bucket: str = _S3_DICT_BUCKET,
    key: str = _S3_DICT_KEY,
) -> Path:
    """Ensure the LM master dict exists locally, fetching from S3 if missing.

    Returns the resolved path on success. Raises :class:`LmDictUnavailable`
    when the dict is absent locally AND the S3 fallback fetch fails — the
    caller (a producer) must then fail loud rather than score all-zero
    sentiment. Self-heals any host (trading box, Saturday spot) that never
    ran the install script.
    """
    path = path or _DEFAULT_LM_PATH
    if path.exists() and path.stat().st_size > 0:
        return path
    try:
        if s3_client is None:
            import boto3

            s3_client = boto3.client("s3")
        path.parent.mkdir(parents=True, exist_ok=True)
        s3_client.download_file(bucket, key, str(path))
    except Exception as e:  # noqa: BLE001 — re-raise as the named hard failure
        raise LmDictUnavailable(
            f"LM master dict missing at {path} and S3 fallback fetch "
            f"failed (s3://{bucket}/{key}): {e}"
        ) from e
    if not (path.exists() and path.stat().st_size > 0):
        raise LmDictUnavailable(
            f"LM master dict still absent/empty after S3 fetch to {path}"
        )
    logger.info(
        "[loughran_mcdonald] fetched master dict from s3://%s/%s → %s",
        bucket, key, path,
    )
    return path


def load_lm_master_dict(
    path: Path | None = None,
) -> dict[str, dict[str, bool]]:
    """Load the Loughran-McDonald Master Dictionary CSV into a per-word
    category-flags dict.

    Returns a mapping like::

        {
            "good":        {"positive": True,  "negative": False, ...},
            "bad":         {"positive": False, "negative": True,  ...},
            "approximately": {"uncertainty": True, ...},
            ...
        }

    The canonical CSV has the columns: Word, Sequence Number, Word
    Count, Word Proportion, Average Proportion, Std Dev, Doc Count,
    Negative, Positive, Uncertainty, Litigious, Strong_Modal,
    Weak_Modal, Constraining, Syllables, Source. We only care about
    Word + the category flag columns; the cell value is non-zero
    (typically a year-stamp) when the word is in that category.

    Returns an empty dict if the file doesn't exist — caller should
    fall back to a stub or fail loud per their own policy. Production
    deploy must run ``scripts/download_lm_dict.py`` once.
    """
    path = path or _DEFAULT_LM_PATH
    if not path.exists():
        logger.warning(
            "[loughran_mcdonald] master dict not found at %s — pipeline "
            "will return all-zero sentiment. Run scripts/download_lm_dict.py.",
            path,
        )
        return {}

    out: dict[str, dict[str, bool]] = {}
    with path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            word = (row.get("Word") or "").strip().lower()
            if not word:
                continue
            out[word] = {
                "positive": _truthy(row.get("Positive")),
                "negative": _truthy(row.get("Negative")),
                "uncertainty": _truthy(row.get("Uncertainty")),
                "litigious": _truthy(row.get("Litigious")),
                "strong_modal": _truthy(row.get("Strong_Modal")),
                "weak_modal": _truthy(row.get("Weak_Modal")),
                "constraining": _truthy(row.get("Constraining")),
            }
    return out


def _truthy(cell: str | None) -> bool:
    """LM CSV uses non-zero year-stamps as the truthy flag and 0 as
    falsy. Tolerate empty strings."""
    if cell is None:
        return False
    cell = cell.strip()
    if not cell:
        return False
    try:
        return int(float(cell)) != 0
    except ValueError:
        return False


# ── Tokenization ──────────────────────────────────────────────────────


_WORD_RE = re.compile(r"[A-Za-z]+")


def _tokenize(text: str) -> list[str]:
    """Lowercase tokenization on alphabetic-only spans.

    LM's lexicon is alphabetic; numbers and punctuation contribute no
    sentiment. Inline numerals + dates + currency symbols are
    intentionally dropped at tokenization time."""
    return [m.group(0).lower() for m in _WORD_RE.finditer(text)]


# ── Scorer ─────────────────────────────────────────────────────────────


class LoughranMcDonaldScorer:
    """LM-dict-based sentiment scorer. Implements ``SentimentScorer``."""

    name = "loughran_mcdonald"

    def __init__(
        self,
        lm_dict: dict[str, dict[str, bool]] | None = None,
        *,
        composite_clip: float = 1.0,
    ) -> None:
        self._lm = lm_dict if lm_dict is not None else load_lm_master_dict()
        self._clip = composite_clip
        if not self._lm:
            logger.warning(
                "[loughran_mcdonald] initialized with empty dict — all "
                "scores will be 0. Provide a dict via constructor or "
                "place the canonical CSV at "
                "collectors/nlp/data/lm_master_dict.csv."
            )

    def score(self, *, text: str, article_fingerprint: str) -> SentimentScore:
        tokens = _tokenize(text)
        total = len(tokens)
        pos = neg = unc = 0
        for tok in tokens:
            cats = self._lm.get(tok)
            if cats is None:
                continue
            if cats.get("positive"):
                pos += 1
            if cats.get("negative"):
                neg += 1
            if cats.get("uncertainty"):
                unc += 1

        if total == 0:
            composite = 0.0
        else:
            raw = (pos - neg) / total
            composite = max(-self._clip, min(self._clip, raw))

        return SentimentScore(
            scorer=self.name,
            article_fingerprint=article_fingerprint,
            composite=composite,
            positive_word_count=pos,
            negative_word_count=neg,
            uncertainty_word_count=unc,
            total_token_count=total,
        )
