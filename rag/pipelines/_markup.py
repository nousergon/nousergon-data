"""Parser selection for EDGAR filing documents (alpha-engine-config-I11476).

An EDGAR primary document is usually HTML — including inline-XBRL filings,
which open with an ``<?xml ...?>`` declaration but whose root element is
``<html>`` (XHTML). Some primary documents are plain XML instead. Handing one
of those to an HTML parser makes bs4 emit ``XMLParsedAsHTMLWarning`` and parse
it with HTML rules, which bs4 documents as unreliable.

:func:`parse_filing_markup` makes the same judgement bs4 itself uses for that
warning (an XML declaration followed by a root tag other than ``html``) and
picks the XML parser in that case. Every other document keeps the ``lxml``
HTML parser the pipelines already used.
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup

# Leading constructs that may precede the root element: whitespace, a BOM,
# processing instructions (including the XML declaration), comments and a
# doctype.
_PROLOG_ITEM = re.compile(
    r"\s+|﻿|<\?.*?\?>|<!--.*?-->|<!DOCTYPE[^>]*>",
    re.IGNORECASE | re.DOTALL,
)
_ROOT_TAG = re.compile(r"<([A-Za-z_][\w:.-]*)")
_XML_DECL = re.compile(r"<\?xml\s", re.IGNORECASE)

# Enough to get past any realistic prolog without scanning the whole filing.
_PROLOG_SCAN_CHARS = 4096


def is_non_xhtml_xml(markup: str) -> bool:
    """True when ``markup`` opens with an XML declaration and its root element
    is not ``html`` — the case bs4 warns about when given an HTML parser."""
    head = markup[:_PROLOG_SCAN_CHARS]
    pos = 0
    saw_xml_decl = False
    while True:
        m = _PROLOG_ITEM.match(head, pos)
        if m is None or m.end() == pos:
            break
        if _XML_DECL.match(m.group(0)):
            saw_xml_decl = True
        pos = m.end()
    if not saw_xml_decl:
        return False
    root = _ROOT_TAG.match(head, pos)
    if root is None:
        return False
    local_name = root.group(1).rsplit(":", 1)[-1]
    return local_name.lower() != "html"


_LEADING_XML_DECL = re.compile(r"\A(?:\s|\ufeff)*<\?xml\s.*?\?>", re.IGNORECASE | re.DOTALL)


def parse_filing_markup(markup: str) -> BeautifulSoup:
    """Parse an EDGAR document with the parser that matches what it is.

    XHTML (inline XBRL) goes to the HTML parser WITHOUT its leading XML
    declaration (alpha-engine-config-I11472). bs4's own raw-markup check
    decides "XML, not HTML" from a document that starts with ``<?xml`` and has
    no match for ``<[^ +]html`` in its first 500 characters — a pattern that
    ``<html`` itself does not match, so every real iXBRL filing (whose first
    500 characters are the declaration, generator comments and the opening
    ``<html ...>`` tag) still raised ``XMLParsedAsHTMLWarning``: measured on
    60 of 60 XML-declared 10-K / 10-Q / 20-F primary documents. The HTML
    parser discards the declaration anyway, so removing it changes no text.
    """
    if is_non_xhtml_xml(markup):
        return BeautifulSoup(markup, "xml")
    return BeautifulSoup(_LEADING_XML_DECL.sub("", markup, count=1), "lxml")
