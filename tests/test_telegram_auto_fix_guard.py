"""Every direct Telegram sender in this repo carries an ``Auto-fix:`` line.

Brian, 2026-10-23: every alert or error posted to Telegram says what fix is
being applied automatically. Rule: nous-ergon-ops observability-policy.md
§ Notification — auto-fix line. Senders that go through
``krepis.telegram.send_message`` get the line from the transport; this guard
covers the files that call the Bot API directly, so a new raw sender cannot
skip it.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# A Bot API send call site: the sendMessage endpoint URL.
_BOT_API_SEND = re.compile(r"api\.telegram\.org/bot[^\s\"']*/sendMessage|api\.telegram\.org/bot")
SCAN_DIRS = ("scripts", "infrastructure", "shadow", ".github/workflows")
SUFFIXES = {".py", ".sh", ".yml", ".yaml"}
# Files that mention the Bot API URL without sending a message.
NOT_SENDERS = {
    "requirements.txt",  # a comment, no send
}


def _raw_senders() -> list[Path]:
    out = []
    for d in SCAN_DIRS:
        for p in (ROOT / d).rglob("*"):
            if p.suffix not in SUFFIXES or not p.is_file():
                continue
            rel = p.relative_to(ROOT).as_posix()
            if rel in NOT_SENDERS or Path(rel).name.startswith("test_"):
                continue
            text = p.read_text(errors="ignore")
            if _BOT_API_SEND.search(text) and "sendMessage" in text:
                out.append(p)
    return out


def test_guard_finds_the_known_senders():
    # A scan that finds nothing is a broken scan, not a clean repo.
    assert len(_raw_senders()) >= 2


def test_every_raw_telegram_sender_carries_an_auto_fix_line():
    missing = [
        p.relative_to(ROOT).as_posix()
        for p in _raw_senders()
        if "Auto-fix:" not in p.read_text(errors="ignore")
    ]
    assert not missing, (
        "These files post to Telegram directly with no 'Auto-fix:' line. Say "
        "what is fixed automatically, who owns it and when (or route the send "
        "through krepis.telegram.send_message, which appends it): "
        + ", ".join(missing)
    )
