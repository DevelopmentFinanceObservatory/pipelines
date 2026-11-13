"""Check the StatVar tables of a build against the graph and write the report.

Run it after `dbt build`, against the DuckDB file the build wrote:

    DUCKDB_PATH=/tmp/ci.duckdb GRAPH_CHECK_URL=<check route URL> \\
        uv run python scripts/graph_check.py report.md

The report goes to the file named by the argument, is appended to the file named
by `GITHUB_STEP_SUMMARY` when that is set, and its verdict line is printed. The
report starts with `MARKER`, a hidden line the pull request workflow uses to find
its own comment. The exit status is 0 whenever a report was written, including a
report that says the check could not run; the check is advisory.
"""

import argparse
import os
import sys
from collections.abc import Sequence
from pathlib import Path

from dfo_pipelines.defs.export_specs import EXPORT_SPECS
from dfo_pipelines.graph_check import run_graph_check

# The first line of every report. Markdown renders it as nothing.
MARKER = "<!-- dfo-graph-check -->"


def main(argv: Sequence[str] | None = None) -> int:
    """Write the report and return the exit status"""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("report", type=Path, help="file to write the report to")
    args = parser.parse_args(argv)

    report, _ = run_graph_check(
        os.environ["DUCKDB_PATH"],
        specs=EXPORT_SPECS,
        url=os.environ.get("GRAPH_CHECK_URL"),
    )
    marked = f"{MARKER}\n{report}"
    args.report.write_text(marked)
    if summary_path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary_path, "a") as summary:
            summary.write(f"{report}\n")
    print(report.splitlines()[0])
    return 0


if __name__ == "__main__":
    sys.exit(main())
