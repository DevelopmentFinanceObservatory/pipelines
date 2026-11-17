from pathlib import Path

import pytest
from upath import UPath

from dfo_pipelines.data_commons.mcf import escape_markdown, mcf_node_dcids


@pytest.mark.parametrize(
    ("markdown", "expected"),
    [
        ("plain", '"plain"'),
        ("a\nb", '"a\\nb"'),
        ("a\tb", '"a\\tb"'),
        ('say "hi"', '"say \\u0022hi\\u0022"'),
        ("a\\b", '"a\\\\b"'),
        ("ends\\", '"ends\\\\"'),
        ('a\\"b', '"a\\\\\\u0022b"'),
        ("café", '"café"'),
    ],
    ids=[
        "plain",
        "newline",
        "tab",
        "quote",
        "backslash",
        "trailing-backslash",
        "backslash-before-quote",
        "non-ascii",
    ],
)
def test_escape_markdown_writes_one_quoted_line(markdown: str, expected: str) -> None:
    """Test markdown is encoded as a single MCF value holding no inner quote"""
    assert escape_markdown(markdown) == expected


def test_mcf_node_dcids_collects_only_the_node_lines(tmp_path: Path) -> None:
    """Test the parser collects each node's dcid, not the property lines under it"""
    mcf_path = tmp_path / "nodes.mcf"
    mcf_path.write_text("""# bench: prod

Node: dcid:FinancialFlow
typeOf: dcid:Class
name: "Financial flow"

Node: dcid:crsPurpose
typeOf: dcid:Property
name: "CRS purpose"
""")

    assert mcf_node_dcids(UPath(mcf_path)) == {"dcid:FinancialFlow", "dcid:crsPurpose"}
