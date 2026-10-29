import subprocess
from collections.abc import Mapping
from pathlib import Path

import dagster as dg


def run_logged(
    command: list[str],
    *,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run a command, logging its output to the run log when it succeeds

    A failing command's output is left to the caller, which puts it in the error.
    """
    result = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        timeout=timeout,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0:
        for output in (result.stdout.strip(), result.stderr.strip()):
            if output:
                dg.get_dagster_logger().info(output)
    return result
