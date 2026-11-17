"""Read and encode MCF, the text format Data Commons nodes are written in."""

import json
from collections.abc import Iterator

from upath import UPath


def escape_markdown(markdown: str) -> str:
    """Encode markdown as a single-line MCF value, readable as a JSON string

    MCF ends a value at any quote, even an escaped one, so quotes are written as
    `\\u0022`, which JSON decodes the same way.
    """
    # Only the inside is re-escaped, so the closing quote survives a body that
    # ends in a backslash.
    inner = json.dumps(markdown, ensure_ascii=False)[1:-1]
    return '"' + inner.replace('\\"', "\\u0022") + '"'


def _mcf_node_lines(mcf_path: UPath) -> Iterator[str]:
    """Every `Node: ` line in an MCF file, one per declared node"""
    return (
        line for line in mcf_path.read_text().splitlines() if line.startswith("Node: ")
    )


def count_mcf_nodes(mcf_path: UPath) -> int:
    """Count the nodes an MCF file declares."""
    return sum(1 for _ in _mcf_node_lines(mcf_path))


def mcf_node_dcids(mcf_path: UPath) -> set[str]:
    """Every node dcid an MCF file declares, read from its `Node: ` lines"""
    return {line.removeprefix("Node: ").strip() for line in _mcf_node_lines(mcf_path)}
