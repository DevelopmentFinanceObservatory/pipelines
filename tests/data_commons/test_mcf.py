import pytest

from dfo_pipelines.data_commons.mcf import escape_markdown


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
