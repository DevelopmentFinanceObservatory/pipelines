import re
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import httpx

from dfo_pipelines.data_commons.export import ExportSpec, ObservationInput
from dfo_pipelines.graph_check.rows import CheckRow

URL = "https://graph.test/api/graph/schema/check"

# A StatVar table shaped like the CRS and DAC models: the fixed columns, the two
# list columns, one column per constraint property, a name, and a bookkeeping
# column the export ignores. The second row leaves its purpose code null and its
# denominator an empty string, and writes its netting basis with a space after the
# namespace.
STATVARS_SQL = """
create or replace table checked.statvars as
select * from (values
    (
        'dcid:Variable1', 'Variable One', 'dcid:FinancialFlow', 'dcid:amount',
        'dcid:measuredValue', 'dcid:Nominal', null,
        'nettingBasis,crsPurpose', 'dcid:Gross', 'dcid:CRSPurposeCode12220',
        'financingProvider,financingRecipient', true
    ),
    (
        'dcid:Variable2', 'Variable Two', 'dcid:FinancialFlow', 'dcid:amount',
        'dcid:measuredValue', 'dcid:Nominal', '',
        'nettingBasis,crsPurpose', 'dcid: Net', null,
        null, false
    )
) as statvars (
    dcid, name, population_type, measured_property, stat_type,
    measurement_qualifier, measurement_denominator, constraint_properties,
    netting_basis, purpose_code, observation_properties, has_provider
)
"""

SPEC = ExportSpec(
    import_name="checked",
    provenance_dcid="Provenance1",
    provenance_model="checked.provenance",
    conversion_recipe=None,
    statvar_model="checked.statvars",
    observation_inputs=(
        ObservationInput(model="checked.observations", property_to_column={}),
    ),
    statvar_column_to_property={
        "name": "name",
        "netting_basis": "nettingBasis",
        "purpose_code": "crsPurpose",
        "has_provider": "hasProvider",
    },
    statvar_ignore_columns=("has_provider",),
    docs_dir=Path("unused"),
)


def answer(
    rows: list[CheckRow],
    *,
    findings: Sequence[dict[str, Any]] = (),
    generation: str | None = "g1",
    rows_checked: int | None = None,
) -> httpx.Response:
    """The route's 200 answer, checking the first `rows_checked` of `rows` or all"""
    checked = len(rows) if rows_checked is None else rows_checked
    return httpx.Response(
        200,
        json={
            "generation": generation,
            "rows_checked": checked,
            "findings": list(findings),
        },
    )


def finding(row: Any, /, **changes: Any) -> dict[str, Any]:
    """A finding about request row `row`, with `changes`; `...` drops a member"""
    base = {
        "row": row,
        "field": "measured_property",
        "property": None,
        "subject": "value",
        "value": "amount",
        "served": None,
        "code": "node_not_held",
        "severity": "error",
    }
    return {key: value for key, value in (base | changes).items() if value != ...}


def refused(row: int | None, detail: str = "bad") -> httpx.Response:
    """The route's 400, with a pointer into request row `row` or to `/rows`"""
    pointer = "/rows" if row is None else f"/rows/{row}/constraint_properties"
    return httpx.Response(
        400, json={"code": "request_invalid", "detail": detail, "pointer": pointer}
    )


def clean_route(url: str, body: dict[str, Any], timeout: float) -> httpx.Response:
    """A route that finds nothing wrong"""
    return answer(body["rows"])


def first_line(report: str) -> str:
    return report.splitlines()[0]


def spans(text: str) -> list[str]:
    """The contents of every code span in `text`, in order"""
    return re.findall(r"`([^`]*)`", text)
