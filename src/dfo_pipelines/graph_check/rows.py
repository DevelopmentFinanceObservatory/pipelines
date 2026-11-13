"""Turn the rows of a StatVar table into the rows the graph check route reads.

A StatVar table holds one row per StatisticalVariable. Its fixed columns name the
variable's population type, measured property, stat type, measurement qualifier
and denominator. `constraint_properties` and `observation_properties` hold
comma-separated property names, and each constraint property takes its value from
the column `ExportSpec.statvar_column_to_property` maps onto it. A table that
lacks a fixed or list column, as the conversion tables lack the constraint
columns, declares nothing there.
"""

import re
from typing import Any, TypedDict

from dfo_pipelines.data_commons.export import ExportSpec

# The StatVar table columns that map onto check row members of the same name.
FIXED_COLUMNS = (
    "dcid",
    "population_type",
    "measured_property",
    "stat_type",
    "measurement_qualifier",
    "measurement_denominator",
)

# The longest `ref` the route accepts.
MAX_REF_LENGTH = 512

# The MCF namespaces a value may be written in. dcp-tools writes `dcid:MyNode` and
# `MyNode` as the same node; the route reads bare dcids.
_NAMESPACE = re.compile(r"^(?:dcid|dcs|schema):")


class CheckRow(TypedDict):
    """One StatVar definition, as the check route reads it."""

    ref: str
    dcid: str | None
    population_type: str | None
    measured_property: str | None
    stat_type: str | None
    measurement_qualifier: str | None
    measurement_denominator: str | None
    constraint_properties: dict[str, str | None]
    observation_properties: list[str]


def statvar_check_rows(
    spec: ExportSpec, statvars: list[dict[str, Any]]
) -> list[CheckRow]:
    """The check rows of one StatVar table.

    A value loses its MCF namespace prefix and an empty cell becomes None. A
    constraint property whose column is empty, ignored or unmapped takes None,
    which the route reports as a missing value.

    Rows that agree in every member but `ref` describe one StatVar twice, so only
    the first is kept. Two rows sharing a dcid with different definitions are both
    kept, the second onwards with its row number in its `ref`.

    Args:
        spec: The export the table belongs to.
        statvars: The table's rows, keyed on column name, as `get_model_rows`
            returns them.

    Returns:
        One row per distinct definition, each `ref` naming the table and the
        StatVar, in table order.
    """
    property_to_column = {
        property_name: column
        for column, property_name in spec.statvar_column_to_property.items()
        if column not in spec.statvar_ignore_columns
    }
    rows: list[CheckRow] = []
    seen_definitions: set[str] = set()
    seen_refs: set[str] = set()
    for position, statvar in enumerate(statvars, start=1):
        fixed = {column: _node(statvar.get(column)) for column in FIXED_COLUMNS}
        constraints = {
            property_name: _node(statvar[column])
            if (column := property_to_column.get(property_name)) in statvar
            else None
            for property_name in _names(statvar.get("constraint_properties"))
        }
        observation_properties = _names(statvar.get("observation_properties"))
        definition = repr((fixed, sorted(constraints.items()), observation_properties))
        if definition in seen_definitions:
            continue
        seen_definitions.add(definition)

        ref = f"{spec.statvar_model}:{fixed['dcid']}"
        if ref in seen_refs:
            ref = f"{ref} (row {position})"
        if len(ref) > MAX_REF_LENGTH:
            ref = f"{spec.statvar_model}:row {position}"
        seen_refs.add(ref)
        rows.append(
            CheckRow(
                ref=ref,
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
    text = _NAMESPACE.sub("", str(value).strip())
    return text or None


def _names(value: Any) -> list[str]:
    """The bare dcids of a comma-separated cell, in order, without repeats"""
    if value is None:
        return []
    names = (_node(name) for name in str(value).split(","))
    return list(dict.fromkeys(name for name in names if name is not None))
