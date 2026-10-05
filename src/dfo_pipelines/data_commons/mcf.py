"""Encode values for MCF, the text format Data Commons nodes are written in."""

import json


def escape_markdown(markdown: str) -> str:
    """Encode markdown as a single-line MCF value, readable as a JSON string

    MCF ends a value at any quote, even an escaped one, so quotes are written as
    `\\u0022`, which JSON decodes the same way.
    """
    # Only the inside is re-escaped, so the closing quote survives a body that
    # ends in a backslash.
    inner = json.dumps(markdown, ensure_ascii=False)[1:-1]
    return '"' + inner.replace('\\"', "\\u0022") + '"'
