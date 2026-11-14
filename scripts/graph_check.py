"""Check the StatVar tables of a build against the graph and write the report.

Run it after `dbt build`, against the DuckDB file the build wrote:

    DUCKDB_PATH=/tmp/ci.duckdb GRAPH_CHECK_URL=<check route URL> \\
        uv run python scripts/graph_check.py report.md

When `GRAPH_CHECK_COMMIT` holds the pull request head commit whose build was
checked, the report names it under the verdict. When `GRAPH_CHECK_BASE_BRANCH`
also names the branch the head was merged into for the build, as the pull
request workflow builds it, the report names the merge. The report goes to the file named
by the argument, is appended to the file named by `GITHUB_STEP_SUMMARY` when that
is set, and its verdict line is printed. `dfo_pipelines.graph_check.comment` posts
the file as the pull request comment. The exit status is 0 when the check produced
a report, including one that says the check did not run. Any other error still
writes a report, whose first line says the check could not complete, and then
exits 1.
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


def main(argv: Sequence[str] | None = None) -> int:
    """Write the report and return the exit status"""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("report", type=Path, help="file to write the report to")
    args = parser.parse_args(argv)

    commit = os.environ.get("GRAPH_CHECK_COMMIT")
    base_branch = os.environ.get("GRAPH_CHECK_BASE_BRANCH")
    try:
        report, _ = run_graph_check(
            os.environ["DUCKDB_PATH"],
            specs=EXPORT_SPECS,
            url=os.environ.get("GRAPH_CHECK_URL"),
            commit=commit,
            base_branch=base_branch,
        )
        status = 0
    except Exception as error:  # every failure still replaces the earlier report
        traceback.print_exc()
        report = failure_report(
            f"{type(error).__name__}: {error}", commit=commit, base_branch=base_branch
        )
        status = 1
    args.report.write_text(report)
    if summary_path := os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(summary_path, "a") as summary:
            summary.write(f"{report}\n")
    print(report.splitlines()[0])
    return status


if __name__ == "__main__":
    sys.exit(main())
