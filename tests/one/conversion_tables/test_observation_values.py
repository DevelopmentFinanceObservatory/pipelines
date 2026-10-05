from pathlib import Path

import duckdb
import pytest
import yaml

MODELS_YML = (
    Path(__file__).parents[3]
    / "dbt"
    / "models"
    / "one"
    / "conversion_tables"
    / "models.yml"
)
TEST_NAME = "every_value_is_positive_and_finite"

# Each table's observations carry the test, so a bad value stops only its export.
OBSERVATION_MODELS = (
    "int_conversion_tables_dac_observations",
    "int_conversion_tables_imf_observations",
)


def _declared_expression(model_name: str) -> str:
    """The expression the dbt test runs, read from the model's own declaration"""
    models = yaml.safe_load(MODELS_YML.read_text())["models"]
    (model,) = (m for m in models if m["name"] == model_name)
    (test,) = (
        t["dbt_utils.expression_is_true"]
        for t in model["data_tests"]
        if t.get("dbt_utils.expression_is_true", {}).get("name") == TEST_NAME
    )
    return test["arguments"]["expression"]


def _failing_values(model_name: str, values: list[float]) -> list[float]:
    """The values the dbt test reports, which is every row failing the expression"""
    conn = duckdb.connect()
    conn.execute("create table observations (value double)")
    conn.executemany("insert into observations values (?)", [(v,) for v in values])
    rows = conn.execute(
        f"select value from observations where not ({_declared_expression(model_name)})"
    ).fetchall()
    return sorted(value for (value,) in rows)


@pytest.mark.parametrize("model_name", OBSERVATION_MODELS)
def test_a_positive_finite_value_passes_the_loaders_value_rule(model_name: str) -> None:
    """Test the dbt test accepts the values the loader accepts"""
    assert _failing_values(model_name, [0.001, 1.0, 100.0, 1e12]) == []


@pytest.mark.parametrize("model_name", OBSERVATION_MODELS)
@pytest.mark.parametrize(
    "value",
    [0.0, -0.5, float("-inf"), float("inf"), float("nan")],
    ids=["zero", "negative", "-inf", "inf", "nan"],
)
def test_a_zero_negative_or_non_finite_value_fails_the_loaders_value_rule(
    model_name: str, value: float
) -> None:
    """Test a zero deflator, a negative one or a non-finite one fails the build"""
    assert len(_failing_values(model_name, [1.0, value])) == 1
