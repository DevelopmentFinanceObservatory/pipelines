import csv
from pathlib import Path

import pytest

SEED = (
    Path(__file__).parents[2]
    / "dbt/seeds/one/climate_finance/climate_finance_methods.csv"
)


def _seed_rows() -> list[dict[str, str]]:
    with SEED.open(newline="") as seed:
        return list(csv.DictReader(seed))


@pytest.mark.parametrize(
    "row",
    [row for row in _seed_rows() if row["method"]],
    ids=lambda row: (
        f"{row['import_name']}-{row['family']}-{row['method']}-{row['attribution']}"
    ),
)
def test_joint_marker_assignment_follows_the_package_method(
    row: dict[str, str],
) -> None:
    """Test each weighting's flag is the opposite of the package's highest_marker

    The re-split subtracts the cross-cutting part from adaptation and mitigation
    only where the flag is true, so a package method that changes its marker
    assignment must change the seed with it.
    """
    climate_finance = pytest.importorskip("climate_finance")
    method = getattr(climate_finance.methods, row["method"])

    assert row["joint_marker_assignment"] == str(not method.highest_marker).lower()


def test_figures_without_a_weighting_have_no_marker_assignment() -> None:
    """Test a row with no package method leaves the flag empty"""
    unweighted = [row for row in _seed_rows() if not row["method"]]

    assert unweighted
    assert all(row["joint_marker_assignment"] == "" for row in unweighted)
