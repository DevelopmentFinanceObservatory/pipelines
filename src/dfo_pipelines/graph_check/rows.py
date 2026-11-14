"""Turn the StatVars of a table into the rows the graph check route reads.

The check reads a StatVar table as the export publishes it. `read_statvars`
writes the table with the export's own `write_statvars_csv` and parses it with
the same dcp-tools reader, `schema_tools.csv_metadata_to_nodes`, under the
export's `statvar_column_to_property`, `statvar_ignore_columns` and
`statvar_csv_options`. Each StatVar comes back as the properties of its MCF node:
ignored columns dropped, mapped columns renamed, every cell pandas reads as
missing (empty, `NA`, `None`, `null` and the other pandas NA tokens) left out, and
an absent stat type set to `measuredValue`.

The node's `constraintProperties` and `observationProperties` hold
comma-separated property names. Each constraint property takes the value the node
publishes under the property's name.
"""

import re
import tempfile
from pathlib import Path
from typing import Any, TypedDict

from dcp_tools.custom_data.schema_tools import csv_metadata_to_nodes
from duckdb import DuckDBPyConnection

from dfo_pipelines.data_commons.export import ExportSpec, write_statvars_csv

# The MCF property each fixed check row member reads.
FIXED_PROPERTIES = {
    "dcid": "dcid",
    "population_type": "populationType",
    "measured_property": "measuredProperty",
    "stat_type": "statType",
    "measurement_qualifier": "measurementQualifier",
    "measurement_denominator": "measurementDenominator",
}

# The MCF namespaces a value may be written in. dcp-tools writes `dcid:MyNode` and
# `MyNode` as the same node. The route reads bare dcids.
_NAMESPACE = re.compile(r"^(?:dcid|dcs|schema):")


class StatVarReadError(Exception):
    """The export's StatVar reader refused a table, so its export would fail."""

    def __init__(self, table: str) -> None:
        super().__init__(f"The export cannot read the StatVars of {table}")
        self.table = table


class CheckRow(TypedDict):
    """One StatVar definition, as the check route reads it."""

    dcid: str | None
    population_type: str | None
    measured_property: str | None
    stat_type: str | None
    measurement_qualifier: str | None
    measurement_denominator: str | None
    constraint_properties: dict[str, str | None]
    observation_properties: list[str]


def read_statvars(
    connection: DuckDBPyConnection, spec: ExportSpec
) -> list[dict[str, Any]]:
    """The StatVars of `spec`'s table as the export publishes them.

    Args:
        connection: The build's database.
        spec: The export whose StatVar table is read.

    Returns:
        Each StatVar's MCF properties, keyed on property name, in the order the
        export writes them.

    Raises:
        duckdb.CatalogException: The table is not in the database.
        StatVarReadError: The export's reader refuses the table, for a value
            that is not a dcid where one is required or an ignored column the
            table lacks.
    """
    with tempfile.TemporaryDirectory() as directory:
        statvars_csv = Path(directory) / "statvars.csv"
        write_statvars_csv(connection, spec, statvars_csv)
        try:
            nodes = csv_metadata_to_nodes(
                statvars_csv,
                column_to_property_mapping=spec.statvar_column_to_property,
                csv_options=spec.statvar_csv_options,
                ignore_columns=list(spec.statvar_ignore_columns),
            )
        # A pydantic ValidationError is a ValueError. pandas raises KeyError for
        # an ignored column the table lacks.
        except (ValueError, KeyError) as error:
            raise StatVarReadError(spec.statvar_model) from error
    return [node.model_dump(exclude_none=True) for node in nodes.nodes]


def statvar_check_rows(statvars: list[dict[str, Any]]) -> list[CheckRow]:
    """The check rows of one StatVar table.

    A value loses its MCF namespace prefix. A constraint property the StatVar
    publishes no value for takes None, which the route reports as a missing
    value.

    Rows that agree in every member describe one StatVar twice, so only the first
    is kept. Two rows sharing a dcid with different definitions are both kept.

    Args:
        statvars: The table's StatVars, as `read_statvars` returns them.

    Returns:
        One row per distinct definition, in table order.
    """
    rows: list[CheckRow] = []
    seen_definitions: set[str] = set()
    for statvar in statvars:
        fixed = {
            member: _node(statvar.get(name))
            for member, name in FIXED_PROPERTIES.items()
        }
        constraints = {
            name: _node(statvar.get(name))
            for name in _names(statvar.get("constraintProperties"))
        }
        observation_properties = _names(statvar.get("observationProperties"))
        definition = repr((fixed, sorted(constraints.items()), observation_properties))
        if definition in seen_definitions:
            continue
        seen_definitions.add(definition)
        rows.append(
            CheckRow(
                dcid=fixed["dcid"],
                population_type=fixed["population_type"],
                measured_property=fixed["measured_property"],
                stat_type=fixed["stat_type"],
                measurement_qualifier=fixed["measurement_qualifier"],
                measurement_denominator=fixed["measurement_denominator"],
                constraint_properties=constraints,
                observation_properties=observation_properties,
            )
        )
    return rows


def _node(value: Any) -> str | None:
    """A cell as a bare dcid, or None for an empty cell"""
    if value is None:
        return None
    text = _NAMESPACE.sub("", str(value).strip()).strip()
    return text or None


def _names(value: Any) -> list[str]:
    """The bare dcids of a comma-separated cell, in order, without repeats"""
    if value is None:
        return []
    names = (_node(name) for name in str(value).split(","))
    return list(dict.fromkeys(name for name in names if name is not None))
