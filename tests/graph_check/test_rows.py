from dataclasses import replace

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


def test_an_ignored_column_gives_its_property_no_value(
    connection: DuckDBPyConnection,
) -> None:
    connection.execute(
        "update checked.statvars set constraint_properties = 'hasProvider'"
        " where dcid = 'dcid:Variable1'"
    )

    (first, _) = statvar_check_rows(
        SPEC, get_model_rows(connection, SPEC.statvar_model)
    )

    assert first["constraint_properties"] == {"hasProvider": None}


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
