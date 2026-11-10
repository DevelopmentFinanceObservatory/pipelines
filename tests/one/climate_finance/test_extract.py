"""Tests of the extracts in scripts/export_climate_finance_release.py.

They need the climate-finance package, which CI does not install, and skip without
it. Run them from the repository root with the package at the commit that built
rc9:

    uv run --with "climate-finance @ git+https://github.com/ONEcampaign/climate-finance-package@efc1ddde3bf99d3e8e6ad787aaf916211d5049c9" \
        pytest tests/one/climate_finance
"""

from types import SimpleNamespace
from typing import Any

import pytest

from tests.one.climate_finance.conftest import RELEASE

cf = pytest.importorskip("climate_finance")
pl = pytest.importorskip("polars")
assert_frame_equal = pytest.importorskip("polars.testing").assert_frame_equal
export_script = pytest.importorskip("scripts.export_climate_finance_release")
spending = export_script.spending
crdf_commitments = export_script.crdf_commitments

# The cut holds Australia and Ireland, developed providers, IDA, a multilateral,
# and the Gates Foundation, a private foundation.
AUSTRALIA, IRELAND, IDA, GATES = 801, 21, 905, 1601
CORE_CONTRIBUTION = 2
QUERY = ["family", "flow", "method", "attribution"]


def test_spending_holds_every_query_by_bi_multi(
    raw_tables: dict[str, Any],
) -> None:
    """Test spending holds each family, flow, method and attribution rule, keyed
    once by year, provider, recipient and bi_multi"""
    table = raw_tables["spending"]
    assert table.columns == [
        *QUERY[:2], "year", "provider_code", "recipient_code", "bi_multi",
        *QUERY[2:], "value", "adaptation", "mitigation", "cross_cutting", "unsplit",
        "no_adaptation_marker",
    ]  # fmt: skip
    assert sorted(table.select(QUERY).unique().iter_rows()) == sorted(
        [
            *(
                ("received", flow, method, "contributions")
                for flow in ("commitments", "disbursements")
                for method in ("ONE", "OECD")
            ),
            *(
                ("provided", flow, method, rule)
                for flow in ("commitments", "disbursements")
                for method in ("ONE", "OECD")
                for rule in ("contributions", "outflows")
            ),
        ]
    )
    keys = [*QUERY, "year", "provider_code", "recipient_code", "bi_multi"]
    assert table.select(keys).is_unique().all()


def test_spending_keeps_every_bi_multi_of_each_view(
    raw_tables: dict[str, Any],
) -> None:
    """Test received holds bilateral rows and IDA's own outflows, and provided the
    developed providers' bilateral rows beside their core contributions, with none
    dropped or summed"""
    table = raw_tables["spending"]
    held = {
        family: set(table.filter(pl.col("family") == family)["bi_multi"])
        for family in ("received", "provided")
    }
    assert {1, 4} <= held["received"]
    assert CORE_CONTRIBUTION not in held["received"]
    assert {1, CORE_CONTRIBUTION} <= held["provided"]
    providers = {
        family: set(table.filter(pl.col("family") == family)["provider_code"])
        for family in ("received", "provided")
    }
    assert providers == {
        "received": {AUSTRALIA, IRELAND, IDA},
        "provided": {AUSTRALIA, IRELAND},
    }


def test_spending_keeps_the_package_objective_parts(
    raw_tables: dict[str, Any],
) -> None:
    """Test the parts are the package's: under ONE they add up to the value, and
    under the OECD's method adaptation and mitigation each still hold the
    cross-cutting value"""
    table = raw_tables["spending"]
    parts = pl.col("adaptation") + pl.col("mitigation") + pl.col("unsplit")
    gap = {
        method: table.filter(pl.col("method") == method)
        .select((parts + sign * pl.col("cross_cutting") - pl.col("value")).abs().max())
        .item()
        for method, sign in (("ONE", 1), ("OECD", -1))
    }
    assert gap == {
        "ONE": pytest.approx(0, abs=1e-3),
        "OECD": pytest.approx(0, abs=1e-3),
    }
    assert table.filter(pl.col("method") == "OECD")["cross_cutting"].sum() > 0


def test_spending_leaves_only_unspread_contributions_without_a_recipient(
    raw_tables: dict[str, Any],
) -> None:
    """Test only core contributions under the spending share lack a recipient, and
    recipient 998 stays as the package gives it"""
    table = raw_tables["spending"]
    unspread = table.filter(pl.col("recipient_code").is_null())
    assert unspread.select("family", "attribution", "bi_multi").unique().rows() == [
        ("provided", "contributions", CORE_CONTRIBUTION)
    ]
    assert not table.filter(pl.col("recipient_code") == 998).is_empty()


@pytest.mark.filterwarnings("ignore::climate_finance.errors.CoverageWarning")
def test_spending_refuses_a_result_for_another_method(release: Any) -> None:
    """Test a result whose method column differs from the query is refused"""

    def mislabelled(**inputs: object) -> SimpleNamespace:
        table = release.spending(**inputs).table
        return SimpleNamespace(table=table.with_columns(method=pl.lit("RANGE")))

    stand_in = SimpleNamespace(spending=mislabelled)
    with pytest.raises(ValueError, match="method holds"):
        spending(stand_in)


def test_crdf_commitments_keeps_every_row_of_the_received_years(
    raw_tables: dict[str, Any],
) -> None:
    """Test CRDF commitments keep every row, private grants included, of the years
    2010 to 2024"""
    table = raw_tables["crdf_commitments"]
    assert (
        table.height
        == pl.scan_parquet(RELEASE / "commitments.parquet")
        .select(pl.len())
        .collect()
        .item()
    )
    assert GATES in set(table["provider_code"])

    years = pl.LazyFrame(
        {column: [None] * 4 for column in table.columns}, schema=table.schema
    ).with_columns(year=pl.Series([2009, 2010, 2024, 2025], dtype=pl.Int16))
    stand_in = SimpleNamespace(table=lambda name: years)
    kept = crdf_commitments(stand_in)
    assert kept["year"].to_list() == [2010, 2024]


def test_btr_totals_is_the_release_table_unchanged(
    raw_tables: dict[str, Any],
) -> None:
    """Test BTR totals keep every row, those with no status or face value too"""
    table = raw_tables["btr_totals"]
    assert_frame_equal(table, pl.read_parquet(RELEASE / "btr_totals.parquet"))
    assert table["status"].is_null().any()
    assert table["face_usd_at_dac"].is_null().any()
