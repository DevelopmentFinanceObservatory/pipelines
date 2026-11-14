from dataclasses import replace

import pytest
from duckdb import DuckDBPyConnection

from dfo_pipelines.data_commons.export import get_model_rows
from dfo_pipelines.graph_check.rows import statvar_check_rows
from tests.graph_check.tables import SPEC


def test_rows_carry_bare_dcids_and_mapped_constraint_values(
    connection: DuckDBPyConnection,
) -> None:
    rows = statvar_check_rows(SPEC, get_model_rows(connection, SPEC.statvar_model))

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
    (first, _) = get_model_rows(connection, SPEC.statvar_model)
    statvar = first | {"constraint_properties": properties, "fooBar": "dcid:Baz"}
    spec = replace(
        SPEC, statvar_ignore_columns=(*SPEC.statvar_ignore_columns, *ignored)
    )

    (row,) = statvar_check_rows(spec, [statvar])

    assert row["constraint_properties"] == values


@pytest.mark.parametrize(
    ("columns", "expected"),
    [
        (["fooBar", "foo_col"], "Mapped"),
        (["foo_col", "fooBar"], "Unmapped"),
    ],
    ids=["mapped column last", "unmapped column last"],
)
def test_the_last_of_two_columns_under_one_name_is_the_one_read(
    columns: list[str], expected: str
) -> None:
    cells = {"foo_col": "dcid:Mapped", "fooBar": "dcid:Unmapped"}
    statvar = {
        "dcid": "dcid:V",
        "constraint_properties": "fooBar",
        **{column: cells[column] for column in columns},
    }
    spec = replace(SPEC, statvar_column_to_property={"foo_col": "fooBar"})

    (row,) = statvar_check_rows(spec, [statvar])

    assert row["constraint_properties"] == {"fooBar": expected}


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
        SPEC, statvar_model="checked.bare", statvar_column_to_property={"name": "name"}
    )

    (row,) = statvar_check_rows(spec, get_model_rows(connection, spec.statvar_model))

    assert row["constraint_properties"] == {}
    assert row["observation_properties"] == []
    assert row["measurement_denominator"] is None


def test_repeated_definitions_collapse_and_differing_ones_keep_their_rows(
    connection: DuckDBPyConnection,
) -> None:
    statvars = get_model_rows(connection, SPEC.statvar_model)
    renamed = statvars[0] | {"name": "Variable One, renamed"}
    redefined = statvars[0] | {"netting_basis": "dcid:Net"}

    rows = statvar_check_rows(SPEC, [statvars[0], renamed, redefined])

    assert [row["ref"] for row in rows] == [
        "checked.statvars:Variable1",
        "checked.statvars:Variable1 (row 3)",
    ]


def test_a_ref_past_the_routes_limit_names_the_row_instead(
    connection: DuckDBPyConnection,
) -> None:
    (first, _) = get_model_rows(connection, SPEC.statvar_model)

    (row,) = statvar_check_rows(SPEC, [first | {"dcid": "V" * 600}])

    assert row["ref"] == "checked.statvars:row 1"
