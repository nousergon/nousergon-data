"""Rule-based event extractor — deterministic replacement for the
Haiku-backed ``AnthropicEventExtractor``.

Maps a NewsArticle to one-or-more :class:`EventFlag` records using
two zero-cost signals already on the wire:

1. **Vendor tags** (``NewsArticle.tags``). Polygon emits keywords;
   GDELT emits structured event codes; Benzinga emits Channels. The
   ``alpha_engine_lib.sources.protocols.NewsArticle`` docstring on
   ``tags`` explicitly names this as "a soft signal for downstream
   event-flag extraction" — this module is the consumer.

2. **Title-keyword regex**. Backstop for Yahoo RSS + any source that
   doesn't populate ``tags``. Pattern table maps short phrases to
   ``DEFAULT_EVENT_CATEGORIES`` slugs (e.g. "earnings beat" →
   ``earnings_release``, "FDA approves" → ``fda_action``).

**Why rule-based + not LLM:** the Haiku output's structured per-article
EventFlag was aggregated to scalars + a category set + top-N
descriptions before any research consumer touched it. That aggregation
collapses the "zero-shot novel-event detection" capability the LLM
nominally provided; downstream agents only see counts, severity stats,
and a category list. Tag-based + keyword-based classification produces
equivalent rollups deterministically. Per
``[[preference_llm_calls_confined_to_research_module]]`` — LLM calls
live in alpha-engine-research; data/executor/etc. should use existing
metadata + rule-based classifiers.

**Severity convention:** all rule-based flags emit ``severity=0.5``
(the EventFlag protocol's documented default). The aggregator's
``event_severity_max/mean`` columns thus reflect "events present"
(0.5) vs "no events" (0.0). The previous LLM severity was a
free-floating Haiku judgment that didn't map to any operational
threshold — operators never tuned alerts on it. Future operator
calibration can bump severity by category from a YAML table if the
need arises (e.g., FDA actions = 0.9, analyst_action = 0.3).
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from collectors.nlp.protocols import EventFlag

__all__ = ["DEFAULT_EVENT_CATEGORIES", "RuleBasedEventExtractor"]


# Closed taxonomy of event categories the rule-based classifier emits.
# Originated in the (now-deleted) Anthropic LLM extractor; the rule-
# based replacement uses the same closed set so downstream consumers
# (research's substrate snapshot, the news_aggregates row builder)
# see the same category values they always have.
DEFAULT_EVENT_CATEGORIES: tuple[str, ...] = (
    "earnings_release",        # quarterly or annual earnings results
    "earnings_guidance",       # forward guidance update (raise/lower/maintain)
    "merger_or_acquisition",   # M&A announcement (any side)
    "ipo_or_secondary",        # IPO filing, secondary offering, direct listing
    "spinoff_or_divestiture",
    "management_change",       # CEO/CFO/exec departure or appointment
    "board_change",
    "buyback_or_dividend",     # capital return announcements
    "regulatory_action",       # SEC/DOJ/CFTC investigation, lawsuit
    "fda_action",              # drug approval, denial, recall, adverse events
    "product_launch",
    "partnership_or_contract", # major customer/supplier/JV deal
    "credit_rating_change",
    "analyst_action",          # upgrade/downgrade/price-target change
    "insider_transaction",     # 10b5-1 sale, insider buying disclosure
    "macro_or_sector",         # company-tangential macro/sector commentary
    "operational_disruption",  # outage, cyberattack, supply-chain breakage
    "other",                   # fallback — should be rare
)


# ── Category mapping tables ──────────────────────────────────────────────


# Polygon emits free-text keywords; GDELT emits CAMEO/GKG codes.
# Lowercase substring match against each tag string. Multiple tags
# matching distinct categories produce multiple EventFlag records on
# the same article — the Haiku path did the same.
_TAG_KEYWORDS: dict[str, tuple[str, ...]] = {
    "earnings_release": (
        "earnings", "results", "quarter", "q1", "q2", "q3", "q4",
        "fy20", "fy21", "fy22", "fy23", "fy24", "fy25", "fy26",
    ),
    "earnings_guidance": (
        "guidance", "outlook", "forecast", "raises guidance",
        "lowers guidance", "withdraws guidance",
    ),
    "merger_or_acquisition": (
        "merger", "acquisition", "acquire", "acquires", "buyout",
        "takeover", "m&a", "consolidation",
    ),
    "ipo_or_secondary": (
        "ipo", "initial public offering", "secondary offering",
        "direct listing", "spac",
    ),
    "spinoff_or_divestiture": (
        "spinoff", "spin-off", "divestiture", "carveout", "carve-out",
    ),
    "management_change": (
        "ceo", "cfo", "coo", "executive", "resignation", "appointment",
        "stepping down", "successor",
    ),
    "board_change": (
        "board", "director", "chairman", "chairwoman",
    ),
    "buyback_or_dividend": (
        "buyback", "repurchase", "dividend", "capital return",
    ),
    "regulatory_action": (
        "sec", "doj", "cftc", "ftc", "regulator", "lawsuit",
        "investigation", "subpoena", "fine", "settlement",
    ),
    "fda_action": (
        "fda", "drug approval", "clinical trial", "recall",
        "adverse event", "phase 1", "phase 2", "phase 3", "phase iii",
    ),
    "product_launch": (
        "launch", "unveils", "introduces", "release", "rollout",
    ),
    "partnership_or_contract": (
        "partnership", "contract", "deal", "agreement", "joint venture",
        "jv", "collaboration",
    ),
    "credit_rating_change": (
        "credit rating", "moody's", "s&p global", "fitch", "downgrade",
        "upgrade",  # ambiguous with analyst_action; resolved by ordering
    ),
    "analyst_action": (
        "analyst", "price target", "rating", "upgraded", "downgraded",
        "initiated coverage",
    ),
    "insider_transaction": (
        "insider", "10b5-1", "form 4", "insider buying", "insider selling",
    ),
    "macro_or_sector": (
        "sector", "macro", "industry", "economy", "fed", "interest rate",
        "inflation", "gdp",
    ),
    "operational_disruption": (
        "outage", "cyberattack", "breach", "supply chain", "disruption",
        "shortage",
    ),
}


# Title-keyword regex map — backstop for sources that don't populate
# ``tags`` (Yahoo RSS in particular). Pattern + category pairs are
# evaluated in order; first match wins per category. Designed to match
# headline phrasings, not body text.
_TITLE_PATTERNS: tuple[tuple[re.Pattern, str], ...] = (
    (re.compile(r"\bearnings\b|\bbeats?\s+(?:expect|estimat)|\bmisses?\s+(?:expect|estimat)|\bquarterly results\b", re.I), "earnings_release"),
    (re.compile(r"\b(?:raises|lowers|withdraws|updates?|reaffirms?)\s+(?:guidance|outlook|forecast)\b", re.I), "earnings_guidance"),
    (re.compile(r"\b(?:acquir(?:es?|er|ing|ed|ition)|merging with|to (?:buy|acquire)|merger|takeover|buyout|all-stock deal)\b", re.I), "merger_or_acquisition"),
    (re.compile(r"\bIPO\b|\binitial public offering\b|\bsecondary offering\b|\bdirect listing\b", re.I), "ipo_or_secondary"),
    (re.compile(r"\bspin-?off\b|\bdivestiture\b|\bcarve-?out\b", re.I), "spinoff_or_divestiture"),
    (re.compile(r"\b(?:names?|appoints?|hires?)\s+new\s+(?:CEO|CFO|COO|chief)|\b(?:CEO|CFO|COO)\s+(?:steps? down|resigns?|to (?:resign|retire))", re.I), "management_change"),
    (re.compile(r"\b(?:names?|appoints?|elects?)\s+(?:new\s+)?(?:director|board member|chairman|chairwoman)", re.I), "board_change"),
    (re.compile(r"\b(?:share buyback|repurchase program|declares? dividend|raises? dividend|special dividend)\b", re.I), "buyback_or_dividend"),
    (re.compile(r"\bSEC (?:probe|investigation|charges|settles)\b|\bDOJ (?:probe|investigation|charges)\b|\b(?:sued|lawsuit|class action|settlement)\b|\bsubpoena", re.I), "regulatory_action"),
    (re.compile(r"\bFDA (?:approves?|denies?|rejects?|grants?)\b|\bclinical trial\b|\bphase \d\b|\brecalls?\b", re.I), "fda_action"),
    (re.compile(r"\b(?:launches?|unveils?|introduces?|debuts?)\s+(?:new\s+)?(?:product|service|platform|feature|tool|version)\b", re.I), "product_launch"),
    (re.compile(r"\b(?:partners?|partnership)\s+with\b|\bjoint venture\b|\bsigns?\s+(?:agreement|contract|deal)\b", re.I), "partnership_or_contract"),
    (re.compile(r"\b(?:Moody'?s|S&P Global|Fitch)\s+(?:downgrades?|upgrades?|cuts?|raises?)\b|\bcredit rating\b", re.I), "credit_rating_change"),
    (re.compile(r"\banalysts?\s+(?:upgrade|downgrade|cut|raise|initiate)\b|\bprice target\b|\b(?:upgraded|downgraded)\s+(?:to|from)\b", re.I), "analyst_action"),
    (re.compile(r"\binsider (?:buying|selling|sale)\b|\b10b5-1\b|\bForm 4\b", re.I), "insider_transaction"),
    (re.compile(r"\b(?:outage|cyber\s*attack|data breach|supply chain)\b", re.I), "operational_disruption"),
    (re.compile(r"\b(?:sector|industry|macro|economy|Fed|inflation|GDP|interest rate)\b", re.I), "macro_or_sector"),
)


_DEFAULT_SEVERITY: float = 0.5


# ── Extractor ────────────────────────────────────────────────────────────


class RuleBasedEventExtractor:
    """Maps a NewsArticle's vendor tags + title to EventFlag records.

    Implements the :class:`EventExtractor` Protocol (duck-typed; the
    ``name`` + ``extract`` shape matches). Drop-in replacement for
    :class:`collectors.nlp.event_extraction.AnthropicEventExtractor`.

    Stateless — safe to share across threads / async tasks. Construction
    is cheap (no model load, no API client, no warm-up).
    """

    name = "rule_based"

    def extract(
        self,
        *,
        text: str,
        article_fingerprint: str,
        article_tickers: tuple[str, ...],
        article_tags: tuple[str, ...] = (),
    ) -> list[EventFlag]:
        """Pipeline-shape entry point — combines tag + title classification.

        ``text`` is the article body (title + body_excerpt per
        ``pipeline._article_text``); the title-keyword regex matches
        over it. ``article_tags`` is the vendor-provided tag set
        (Polygon keywords / GDELT codes / Benzinga channels) plumbed
        through from ``NewsArticle.tags`` in
        ``NewsNLPPipeline.process``. Default empty for back-compat with
        any caller still on the flat-tuple shape.
        """
        if not text or not text.strip():
            return []
        categories: set[str] = set()
        categories.update(_categorize_from_tags(article_tags))
        categories.update(_categorize_from_title(text))
        if not categories:
            return []
        # Order matches DEFAULT_EVENT_CATEGORIES so deduplicated rows
        # have a deterministic sort.
        ordered = [c for c in DEFAULT_EVENT_CATEGORIES if c in categories]

        now = datetime.now(timezone.utc)
        # Description is the article text passed in. Pipeline normally
        # hands us title + body_excerpt; just use the first line (title)
        # to keep aggregated ``top_event_descriptions`` readable in the
        # EOD-style consumer surfaces.
        description = text.split("\n", 1)[0].strip() or text
        return [
            EventFlag(
                extractor=self.name,
                article_fingerprint=article_fingerprint,
                category=cat,
                description=description,
                tickers=article_tickers,
                severity=_DEFAULT_SEVERITY,
                extracted_at=now,
            )
            for cat in ordered
        ]

# ── Helpers ──────────────────────────────────────────────────────────────


def _categorize_from_tags(tags: tuple[str, ...]) -> set[str]:
    """Map vendor tags to category slugs via substring keyword match."""
    if not tags:
        return set()
    tag_blob = " ".join(t.lower() for t in tags)
    matched: set[str] = set()
    for category, keywords in _TAG_KEYWORDS.items():
        for kw in keywords:
            if kw in tag_blob:
                matched.add(category)
                break
    return matched


def _categorize_from_title(title: str) -> set[str]:
    """Map title text to category slugs via regex."""
    if not title:
        return set()
    matched: set[str] = set()
    for pattern, category in _TITLE_PATTERNS:
        if pattern.search(title):
            matched.add(category)
    return matched
