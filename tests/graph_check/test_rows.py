from dataclasses import replace

import pytest
from duckdb import DuckDBPyConnection

from dfo_pipelines.data_commons.export import ExportSpec
from dfo_pipelines.graph_check.rows import (
    CheckRow,
    StatVarReadError,
    read_statvars,
    statvar_check_rows,
)
from tests.graph_check.tables import SPEC


def check_rows_of(connection: DuckDBPyConnection, spec: ExportSpec) -> list[CheckRow]:
    return statvar_check_rows(spec, read_statvars(connection, spec))


def test_rows_carry_bare_dcids_and_mapped_constraint_values(
    connection: DuckDBPyConnection,
) -> None:
    rows = check_rows_of(connection, SPEC)

    assert rows == [
        {
            "ref": "checked.statvars:Variable1",
            "dcid": "Variable1",
            "population_type": "FinancialFlow",
            "measured_property": "amount",
            "stat_type": "measuredValue",
            "measurement_qualifier": "Nominal",
            "measurement_denominator": None,
            "constraint_properties": {
                "nettingBasis": "Gross",
                "crsPurpose": "CRSPurposeCode12220",
            },
            "observation_properties": ["financingProvider", "financingRecipient"],
        },
        {
            "ref": "checked.statvars:Variable2",
            "dcid": "Variable2",
            "population_type": "FinancialFlow",
            "measured_property": "amount",
            "stat_type": "measuredValue",
            "measurement_qualifier": "Nominal",
            "measurement_denominator": None,
            "constraint_properties": {"nettingBasis": "Net", "crsPurpose": None},
            "observation_properties": [],
        },
    ]


@pytest.mark.parametrize(
    ("ignored", "properties", "values"),
    [
        ((), "hasProvider", {"hasProvider": None}),
        ((), "netting_basis", {"netting_basis": None}),
        ((), "fooBar", {"fooBar": "Baz"}),
        (("fooBar",), "fooBar", {"fooBar": None}),
    ],
    ids=[
        "mapped column ignored",
        "column renamed by its mapping",
        "unmapped column of the property's name",
        "that column ignored",
    ],
)
def test_a_constraint_reads_the_column_the_export_publishes_under_its_name(
    connection: DuckDBPyConnection,
    ignored: tuple[str, ...],
    properties: str,
    values: dict[str, str | None],
) -> None:
    connection.execute(
        "alter table checked.statvars add column fooBar varchar default 'dcid:Baz'"
    )
    connection.execute(
        "update checked.statvars set constraint_properties = ?", [properties]
    )
    spec = replace(
        SPEC, statvar_ignore_columns=(*SPEC.statvar_ignore_columns, *ignored)
    )

    (first, _) = check_rows_of(connection, spec)

    assert first["constraint_properties"] == values


def test_a_table_without_list_columns_declares_no_constraints(
    connection: DuckDBPyConnection,
) -> None:
    # The conversion tables carry only the fixed columns a StatVar needs.
    connection.execute(
        "create table checked.bare as select dcid, name, population_type,"
        " measured_property, stat_type, measurement_qualifier"
        " from checked.statvars where dcid = 'dcid:Variable1'"
    )
    spec = replace(
        SPEC,
        statvar_model="checked.bare",
        statvar_column_to_property={"name": "name"},
        statvar_ignore_columns=(),
    )

    (row,) = check_rows_of(connection, spec)

    assert row["constraint_properties"] == {}
    assert row["observation_properties"] == []
    assert row["measurement_denominator"] is None


def test_repeated_definitions_collapse_and_differing_ones_keep_their_rows(
    connection: DuckDBPyConnection,
) -> None:
    connection.execute(
        "insert into checked.statvars"
        " select * replace ('Variable One, renamed' as name) from checked.statvars"
        " where dcid = 'dcid:Variable1'"
        " union all"
        " select * replace ('dcid:Net' as netting_basis) from checked.statvars"
        " where dcid = 'dcid:Variable1'"
    )

    rows = check_rows_of(connection, SPEC)

    # In the export's order the redefined row is second and the renamed third.
    assert [row["ref"] for row in rows] == [
        "checked.statvars:Variable1",
        "checked.statvars:Variable1 (row 2)",
        "checked.statvars:Variable2",
    ]


def test_a_ref_past_the_routes_limit_names_the_row_instead(
    connection: DuckDBPyConnection,
) -> None:
    connection.execute(
        "update checked.statvars set dcid = 'dcid:' || repeat('V', 600)"
        " where dcid = 'dcid:Variable1'"
    )

    (row, _) = check_rows_of(connection, SPEC)

    assert row["ref"] == "checked.statvars:row 1"


@pytest.mark.parametrize(
    ("statement", "ignored"),
    [
        ("update checked.statvars set population_type = 'FinancialFlow'", ()),
        ("select 1", ("no_such_column",)),
    ],
    ids=["a bare dcid where the export needs a prefix", "a missing ignored column"],
)
def test_a_table_the_export_cannot_read_is_refused(
    connection: DuckDBPyConnection, statement: str, ignored: tuple[str, ...]
) -> None:
    connection.execute(statement)
    spec = replace(SPEC, statvar_ignore_columns=ignored)

    with pytest.raises(StatVarReadError) as raised:
        read_statvars(connection, spec)

    assert raised.value.table == SPEC.statvar_model
