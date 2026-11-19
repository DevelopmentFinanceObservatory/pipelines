"""Check an export's StatVars against the schema the Graph API serves.

Reads the StatVar model of the export named `import_name` from the database at
`DUCKDB_PATH`, posts its rows to the public Graph API, or the one `DFO_GRAPH_API_URL`
names, and prints each finding. Run it after `dbt build`:

    uv run --env-file .env python scripts/check_statvars.py oecd_dac2a

It exits 0 when the check finds no error, 1 when it finds an error or the Graph API
refuses a row, and 2 when the check could not run: `DUCKDB_PATH` is
unset, the model cannot be read, the Graph API is
unreachable, kept answering 503 or 429 past the wait limit, or answered something
the check cannot read. Warnings are printed and leave the exit code alone.
"""

import argparse
import os
import sys

import duckdb

from dfo_pipelines.data_commons.graph_check import (
    GraphCheckError,
    check_spec,
    graph_api_url,
    plural,
)
from dfo_pipelines.defs.export_specs import EXPORT_SPECS
from dfo_pipelines.warehouse import get_model_rows

EXIT_CLEAN = 0
EXIT_FINDINGS = 1
EXIT_UNCHECKED = 2


def main(argv: list[str] | None = None) -> int:
    specs = {spec.import_name: spec for spec in EXPORT_SPECS}
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("import_name", choices=sorted(specs))
    spec = specs[parser.parse_args(argv).import_name]

    base_url = graph_api_url()
    database = os.environ.get("DUCKDB_PATH")
    if not database:
        print("DUCKDB_PATH is unset", file=sys.stderr)
        return EXIT_UNCHECKED
    try:
        with duckdb.connect(database, read_only=True) as connection:
            statvars = get_model_rows(connection, spec.statvar_model)
    except duckdb.Error as error:
        print(f"Could not read {spec.statvar_model}: {error}", file=sys.stderr)
        return EXIT_UNCHECKED
    try:
        check = check_spec(spec, statvars, base_url=base_url)
    except GraphCheckError as error:
        print(f"The check did not finish: {error}", file=sys.stderr)
        return EXIT_UNCHECKED

    for finding in check.findings:
        print(finding)
    for refused in check.refused:
        print(refused)
    print(
        f"{plural(len(check.findings), 'finding')} and"
        f" {plural(len(check.refused), 'refused row')} over"
        f" {plural(len(statvars), 'StatVar')} of {spec.import_name}",
        file=sys.stderr,
    )
    return EXIT_FINDINGS if check.has_errors else EXIT_CLEAN


if __name__ == "__main__":
    sys.exit(main())
