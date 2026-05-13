"""LLM-based structured event extraction (Anthropic Haiku-tier).

Reads a news article + its associated tickers and emits a list of
structured ``EventFlag`` records, one per identified event. Uses
Anthropic's structured-output API (``tool_use``) to enforce the
schema at the model boundary — invalid outputs fail validation and
the article is logged + skipped rather than producing malformed data.

Why LLM over rule-based regex / NER:

- Finance events are highly heterogeneous in surface form. "Files
  for IPO", "S-1 filing announced", "begins trading on NYSE today"
  all describe the same IPO_FILING event. Maintaining regex rules
  across this space is brittle and recall-bounded.
- We already pay Anthropic; Haiku-tier extraction is ~$0.001 per
  article at typical lengths.
- Structured output via tool_use gives schema validation for free.

Cost telemetry routes through the standard cost-tracking callback so
this extractor is billed under ``agent_id="news_event_extractor"``.

Categories are a closed taxonomy (see DEFAULT_EVENT_CATEGORIES). The
extractor prompt names the full list — model returns at most one of
these per event. Open-vocabulary events ("management mood shift on
earnings call") map to the nearest category or are dropped.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

from collectors.nlp.protocols import EventFlag

logger = logging.getLogger(__name__)


DEFAULT_EVENT_CATEGORIES: tuple[str, ...] = (
    "earnings_release",      # quarterly or annual earnings results
    "earnings_guidance",     # forward guidance update (raise/lower/maintain)
    "merger_or_acquisition", # M&A announcement (any side)
    "ipo_or_secondary",      # IPO filing, secondary offering, direct listing
    "spinoff_or_divestiture",
    "management_change",     # CEO/CFO/exec departure or appointment
    "board_change",
    "buyback_or_dividend",   # capital return announcements
    "regulatory_action",     # SEC/DOJ/CFTC investigation, lawsuit
    "fda_action",            # drug approval, denial, recall, adverse events
    "product_launch",
    "partnership_or_contract", # major customer/supplier/JV deal
    "credit_rating_change",
    "analyst_action",        # upgrade/downgrade/price-target change
    "insider_transaction",   # 10b5-1 sale, insider buying disclosure
    "macro_or_sector",       # company-tangential macro/sector commentary
    "operational_disruption", # outage, cyberattack, supply-chain breakage
    "other",                 # fallback — should be rare
)


# Tool spec for Anthropic structured-output. The schema mirrors the
# EventFlag Pydantic shape minus the fields the extractor doesn't fill
# (extractor name + article_fingerprint + extracted_at — those are
# stamped by the wrapper).
_EVENT_TOOL_NAME = "EmitEventFlags"


def _build_tool_spec(
    categories: tuple[str, ...] = DEFAULT_EVENT_CATEGORIES,
) -> dict[str, Any]:
    return {
        "name": _EVENT_TOOL_NAME,
        "description": (
            "Emit structured event flags for the news article. Use one "
            "event per distinct material event. Return an empty list if "
            "no events qualify."
        ),
        "input_schema": {
            "type": "object",
            "required": ["events"],
            "properties": {
                "events": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["category", "description", "tickers", "severity"],
                        "properties": {
                            "category": {
                                "type": "string",
                                "enum": list(categories),
                            },
                            "description": {"type": "string"},
                            "tickers": {
                                "type": "array",
                                "items": {"type": "string"},
                            },
                            "severity": {
                                "type": "number",
                                "minimum": 0,
                                "maximum": 1,
                            },
                        },
                    },
                },
            },
        },
    }


_SYSTEM_PROMPT = """You are a financial event extractor.

Given a news article, identify each material event it reports and emit one
structured record per event via the EmitEventFlags tool.

Severity guide:
  0.9-1.0  market-moving (M&A, FDA approval, major earnings miss/beat,
           investigation announcement, CEO departure mid-cycle)
  0.6-0.8  meaningful (guidance change, analyst upgrade/downgrade
           by major firm, dividend change, product launch in core market)
  0.3-0.5  routine (small partnerships, mid-tier analyst notes,
           secondary product launches, scheduled events)
  0.0-0.2  background / atmospheric (macro commentary, peer mentions,
           re-reports of stale events)

Use the closed category taxonomy. If an event genuinely doesn't fit,
use 'other' — but prefer the closest category.

Tickers should reflect WHICH companies the event directly concerns.
For a merger between A and B, list both. For an A-acquires-B with A
named in 1 ticker, list A only if B isn't tradeable.

Return an empty events list if the article describes no material event
(e.g. pure macro commentary not tied to any single company)."""


class AnthropicEventExtractor:
    """Haiku-tier structured event extraction. Implements ``EventExtractor``.

    ``client`` is the Anthropic SDK client. Tests inject a mock. Production
    uses ``anthropic.Anthropic(api_key=...)``.
    """

    name = "anthropic_haiku"

    def __init__(
        self,
        client: Any,
        *,
        model: str = "claude-haiku-4-5",
        max_tokens: int = 1024,
        categories: tuple[str, ...] = DEFAULT_EVENT_CATEGORIES,
    ) -> None:
        self._client = client
        self._model = model
        self._max_tokens = max_tokens
        self._categories = categories
        self._tool_spec = _build_tool_spec(categories)

    def extract(
        self, *, text: str, article_fingerprint: str,
        article_tickers: tuple[str, ...],
    ) -> list[EventFlag]:
        if not text or not text.strip():
            return []
        try:
            response = self._client.messages.create(
                model=self._model,
                max_tokens=self._max_tokens,
                system=_SYSTEM_PROMPT,
                tools=[self._tool_spec],
                tool_choice={"type": "tool", "name": _EVENT_TOOL_NAME},
                messages=[{
                    "role": "user",
                    "content": (
                        f"Article tickers: {list(article_tickers)}\n\n"
                        f"Article text:\n{text}"
                    ),
                }],
            )
        except Exception as e:
            logger.warning(
                "[event_extraction] anthropic call failed for fingerprint "
                "%s: %s", article_fingerprint, e,
            )
            return []

        events_payload = _extract_tool_input(response)
        if events_payload is None:
            return []

        out: list[EventFlag] = []
        now = datetime.now(timezone.utc)
        for entry in events_payload.get("events", []):
            try:
                out.append(EventFlag(
                    extractor=self.name,
                    article_fingerprint=article_fingerprint,
                    category=entry["category"],
                    description=entry["description"],
                    tickers=tuple(entry.get("tickers") or ()),
                    severity=float(entry.get("severity", 0.5)),
                    extracted_at=now,
                ))
            except Exception as e:
                logger.warning(
                    "[event_extraction] dropping malformed event from "
                    "fingerprint %s: %s (entry=%r)",
                    article_fingerprint, e, entry,
                )
        return out


def _extract_tool_input(response: Any) -> dict | None:
    """Pull the EmitEventFlags tool's ``input`` dict out of the
    Anthropic response. Anthropic's response.content is a list of
    content blocks; we want the one with .type == 'tool_use'.

    Returns None if the response shape is unexpected (logged, not raised).
    """
    try:
        for block in (response.content or []):
            if getattr(block, "type", None) == "tool_use":
                if getattr(block, "name", None) == _EVENT_TOOL_NAME:
                    raw = block.input
                    if isinstance(raw, str):
                        return json.loads(raw)
                    return raw
    except Exception as e:
        logger.warning(
            "[event_extraction] response parse error: %s", e
        )
    return None
