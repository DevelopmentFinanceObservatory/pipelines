"""Check an export's StatVars against the schema the Graph API serves.

Reads the StatVar model of the export named `import_name` from the database at
`DUCKDB_PATH`, posts its rows to the Graph API at `DFO_GRAPH_API_URL`, and prints
each finding. Run it after `dbt build`:

    uv run --env-file .env python scripts/check_statvars.py oecd_dac2a

It exits 0 when the check finds nothing, 1 when it prints findings, and 2 when the
check could not run: `DFO_GRAPH_API_URL` is unset, the Graph API is unreachable, or
it kept answering 503 or 429 past the wait limit.
"""

import argparse
import os
import sys

import duckdb

from dfo_pipelines.data_commons.graph_check import (
    GRAPH_API_URL_VARIABLE,
    GraphCheckError,
    check_statvars,
    graph_api_url,
    open_client,
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
    if base_url is None:
        print(f"{GRAPH_API_URL_VARIABLE} is unset", file=sys.stderr)
        return EXIT_UNCHECKED
    with duckdb.connect(os.environ["DUCKDB_PATH"], read_only=True) as connection:
        statvars = get_model_rows(connection, spec.statvar_model)
    try:
        with open_client(base_url) as client:
            findings = check_statvars(
                statvars,
                column_to_property=spec.statvar_column_to_property,
                client=client,
            )
    except GraphCheckError as error:
        print(f"The check did not finish: {error}", file=sys.stderr)
        return EXIT_UNCHECKED

    for finding in findings:
        print(finding)
    print(
        f"{len(findings)} findings over {len(statvars)} StatVars of {spec.import_name}",
        file=sys.stderr,
    )
    return EXIT_FINDINGS if findings else EXIT_CLEAN


if __name__ == "__main__":
    sys.exit(main())
