import sys

import dagster as dg
import pytest

from dfo_pipelines.defs.subprocesses import run_logged
from tests.helpers import logged_messages


def messages_from_running(code: str) -> list[str]:
    """The run log's messages from an asset running `code` in a Python subprocess"""

    @dg.asset
    def runs_python() -> None:
        run_logged([sys.executable, "-c", code])

    return logged_messages([runs_python], {})


@pytest.mark.parametrize("stream", ["stdout", "stderr"])
def test_run_logged_logs_a_stream_with_output(stream: str) -> None:
    """Test a stream's output reaches the run log, and the other, empty one does not"""
    messages = messages_from_running(f"import sys; print('printed', file=sys.{stream})")

    assert "printed" in messages
    assert "" not in messages


def test_run_logged_logs_nothing_when_the_command_fails() -> None:
    """Test a failing command's output is left to the caller"""
    messages = messages_from_running(
        "import sys; print('to stderr', file=sys.stderr); sys.exit(1)"
    )

    assert "to stderr" not in messages
