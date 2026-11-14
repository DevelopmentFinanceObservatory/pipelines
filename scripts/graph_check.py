"""Check the StatVar tables of a build against the graph and write the report.

Run it after `dbt build`, against the DuckDB file the build wrote:

    DUCKDB_PATH=/tmp/ci.duckdb GRAPH_CHECK_URL=<check route URL> \\
        uv run python scripts/graph_check.py report.md

When `GRAPH_CHECK_COMMIT` holds the pull request head commit the build was made
for, the report names it under the verdict. The report goes to the file named by the argument, is
appended to the file named by `GITHUB_STEP_SUMMARY` when that is set, and its
verdict line is printed. The report starts with `MARKER`, a hidden line the pull
request workflow uses to find its own comment. The exit status is 0 when the check
produced a report, including one that says the check did not run. Any other error
still writes a report, whose first line says the check could not complete, and
then exits 1.
"""

import argparse
import os
import sys
import traceback
from collections.abc import Sequence
from pathlib import Path

from dfo_pipelines.defs.export_specs import EXPORT_SPECS
from dfo_pipelines.graph_check import run_graph_check
from dfo_pipelines.graph_check.report import failure_report

# The first line of every report. Markdown renders it as nothing.
MARKER = "<!-- dfo-graph-check -->"


def main(argv: Sequence[str] | None = None) -> int:
    """Write the report and return the exit status"""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("report", type=Path, help="file to write the report to")
    args = parser.parse_args(argv)

    commit = os.environ.get("GRAPH_CHECK_COMMIT")
    try:
        report, _ = run_graph_check(
            os.environ["DUCKDB_PATH"],
            specs=EXPORT_SPECS,
            url=os.environ.get("GRAPH_CHECK_URL"),
            commit=commit,
        )
        status = 0
    except Exception as error:  # every failure still replaces the earlier report
        traceback.print_exc()
        report = failure_report(f"{type(error).__name__}: {error}", commit=commit)
        status = 1
    marked = f"{MARKER}\n{report}"
    args.report.write_text(marked)
    if summary_path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary_path, "a") as summary:
            summary.write(f"{report}\n")
    print(report.splitlines()[0])
    return status


if __name__ == "__main__":
    sys.exit(main())
