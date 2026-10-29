import json
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import patch

import dagster as dg
import pandas as pd
import pytest
from dagster_duckdb import DuckDBResource
from oda_data.clean_data.schema import ODASchema
from oda_reader.download.download_tools import CRS_FLOW_URL, MULTI_FLOW_URL
from upath import UPath

from dfo_pipelines.data_commons.export import DOCUMENTATION_FILES
from dfo_pipelines.defs.one.imputed_multilateral_sectors.assets import (
    CRS_BULK_LABEL,
    CURRENCY,
    FLOW_TYPES,
    IMPUTED_MULTILATERAL_SECTORS_EXPORT_SPEC,
    MULTISYSTEM_BULK_LABEL,
    build_quality_metadata,
    one_imputed_multilateral_sectors_dc_export,
    one_imputed_multilateral_sectors_dc_ingestion,
    one_imputed_multilateral_sectors_raw_file,
    one_imputed_multilateral_sectors_raw_metadata,
    one_imputed_multilateral_sectors_raw_table,
    one_imputed_multilateral_sectors_source,
    parse_release_date,
    resolve_bulk_versions,
    verify_release_provenance,
)
from dfo_pipelines.defs.resources import (
    KnowledgeGraphResource,
    UPathFileStoreResource,
)
from dfo_pipelines.extraction.metadata import (
    read_extraction_metadata,
    write_extraction_metadata,
)
from tests.helpers import column_list, list_first_row, meta, row_count

ASSETS_MODULE = "dfo_pipelines.defs.one.imputed_multilateral_sectors.assets"

FIXTURES = Path(__file__).parent / "fixtures"
DOCS_DIR = Path(__file__).parents[3] / "docs" / "one" / "imputed_multilateral_sectors"

CRS_TEST_URL = "https://webfs-dcd.oecd.org/data/dac_crs/CRS.parquet"
MULTI_TEST_URL = "https://webfs-dcd.oecd.org/data/multisystem/MULTISYSTEM.parquet"

CRS_VERSION = "v20260801"
MULTI_VERSION = "v20260715"

OBSERVED_TOKENS = {"crs": CRS_VERSION, "multisystem": MULTI_VERSION}

CRS_RELEASE_RECORD = {
    "dataset": "CRSData",
    "release_id": CRS_VERSION,
    "downloaded_at": "2026-08-01T00:00:00",
    "version": CRS_VERSION,
}
MULTISYSTEM_RELEASE_RECORD = {
    "dataset": "MultiSystemData",
    "release_id": MULTI_VERSION,
    "downloaded_at": "2026-07-15T00:00:00",
    "version": MULTI_VERSION,
}
VALID_PROVENANCE = {
    "crs_release": CRS_RELEASE_RECORD,
    "multisystem_release": MULTISYSTEM_RELEASE_RECORD,
    "crosswalk_vintage": "2026-01-01",
    "proxy_table_vintage": "2026-01-01",
    "crs_channel_mapping_vintage": "2026-01-01",
    "package_version": "2.8.1",
    "parameters": {},
}


def _bulk_lookup(*, flow_url: str, label: str, bypass_annotation_cache: bool) -> tuple:
    """Stand in for get_bulk_file_url_with_version, keyed on flow_url"""
    assert bypass_annotation_cache is True
    if flow_url == CRS_FLOW_URL:
        assert label == CRS_BULK_LABEL
        return CRS_TEST_URL, CRS_VERSION
    if flow_url == MULTI_FLOW_URL:
        assert label == MULTISYSTEM_BULK_LABEL
        return MULTI_TEST_URL, MULTI_VERSION
    raise AssertionError(f"unexpected flow_url {flow_url}")


def test_resolve_bulk_versions_reads_both_releases() -> None:
    """Test both the CRS and Members' Total Use bulk files are resolved"""
    with patch(
        f"{ASSETS_MODULE}.get_bulk_file_url_with_version",
        autospec=True,
        side_effect=_bulk_lookup,
    ) as lookup:
        crs, multi = resolve_bulk_versions()

    assert crs == (CRS_TEST_URL, CRS_VERSION)
    assert multi == (MULTI_TEST_URL, MULTI_VERSION)
    assert lookup.call_count == 2


def test_resolve_bulk_versions_raises_when_crs_version_unavailable() -> None:
    """Test a missing CRS version fails loudly rather than fingerprinting one input"""
    with (
        patch(
            f"{ASSETS_MODULE}.get_bulk_file_url_with_version",
            autospec=True,
            return_value=(CRS_TEST_URL, None),
        ),
        pytest.raises(ValueError, match="CRS version check"),
    ):
        resolve_bulk_versions()


def test_resolve_bulk_versions_raises_when_multisystem_version_unavailable() -> None:
    """Test a missing Members' Total Use version fails loudly too

    Both upstreams are fingerprinted, so either one resolving to no version
    must stop the observation rather than silently pinning only the CRS side.
    """

    def lookup(*, flow_url: str, label: str, bypass_annotation_cache: bool) -> tuple:
        if flow_url == CRS_FLOW_URL:
            return CRS_TEST_URL, CRS_VERSION
        return MULTI_TEST_URL, None

    with (
        patch(
            f"{ASSETS_MODULE}.get_bulk_file_url_with_version",
            autospec=True,
            side_effect=lookup,
        ),
        pytest.raises(ValueError, match="Members' Total Use version check"),
    ):
        resolve_bulk_versions()


ETAG_TOKEN = '"5f2a-1b3c"'


def test_resolve_bulk_versions_raises_when_the_crs_token_carries_no_date() -> None:
    """Test an ETag/Last-Modified fallback CRS token refuses before any caller sees it

    oda_reader's get_bulk_file_url_with_version falls back to a non-vYYYYMMDD
    revalidation token when the OECD annotation label carries no release
    date; resolve_bulk_versions must catch that itself, rather than letting
    an unparsable token reach a caller that writes a file before finding
    out.
    """
    with (
        patch(
            f"{ASSETS_MODULE}.get_bulk_file_url_with_version",
            autospec=True,
            return_value=(CRS_TEST_URL, ETAG_TOKEN),
        ),
        pytest.raises(ValueError, match="CRS version token carries no release date"),
    ):
        resolve_bulk_versions()


def test_resolve_bulk_versions_raises_when_the_multisystem_token_carries_no_date() -> (
    None
):
    """Test an ETag/Last-Modified fallback Members' Total Use token refuses too"""

    def lookup(*, flow_url: str, label: str, bypass_annotation_cache: bool) -> tuple:
        if flow_url == CRS_FLOW_URL:
            return CRS_TEST_URL, CRS_VERSION
        return MULTI_TEST_URL, ETAG_TOKEN

    with (
        patch(
            f"{ASSETS_MODULE}.get_bulk_file_url_with_version",
            autospec=True,
            side_effect=lookup,
        ),
        pytest.raises(
            ValueError,
            match="Members' Total Use version token carries no release date",
        ),
    ):
        resolve_bulk_versions()


def test_parse_release_date_reads_the_date_from_the_version_token() -> None:
    """Test the publication date is read out of a vYYYYMMDD token"""
    assert parse_release_date("v20260801", label="CRS") == date(2026, 8, 1)


def test_parse_release_date_reads_the_date_from_a_same_day_republish_token() -> None:
    """Test a vYYYYMMDD-N same-day republish token resolves to the bare date

    oda_reader appends a within-day sequence number when OECD republishes a
    bulk file twice in the same day; it carries no date information of its
    own, so it must resolve to the same date as the bare token.
    """
    assert parse_release_date("v20260801-1", label="CRS") == date(2026, 8, 1)


@pytest.mark.parametrize(
    "version",
    ["20260801", "v2026080", "vYYYYMMDD", "v20260801\n"],
    ids=["no prefix", "wrong length", "not digits", "trailing newline"],
)
def test_parse_release_date_raises_when_the_token_carries_no_date(
    version: str,
) -> None:
    """Test a changed token format fails rather than recording a wrong release date"""
    with pytest.raises(ValueError, match="Members' Total Use version token"):
        parse_release_date(version, label="Members' Total Use")


def test_verify_release_provenance_accepts_matching_tokens() -> None:
    """Test matching provenance and observations pass without raising"""
    verify_release_provenance(
        provenance=VALID_PROVENANCE,
        observed_before=OBSERVED_TOKENS,
        observed_after=OBSERVED_TOKENS,
    )


def test_verify_release_provenance_raises_when_provenance_is_missing() -> None:
    """Test a result with no provenance attrs is refused rather than trusted blind"""
    with pytest.raises(ValueError, match=r"no result.attrs\['provenance'\]"):
        verify_release_provenance(
            provenance=None,
            observed_before=OBSERVED_TOKENS,
            observed_after=OBSERVED_TOKENS,
        )


@pytest.mark.parametrize("release", [None, "supplied"], ids=["none", "supplied"])
def test_verify_release_provenance_raises_when_the_crs_release_is_not_a_record(
    release: object,
) -> None:
    """Test an unread or caller-supplied CRS leg is refused, not assumed fresh"""
    provenance = {**VALID_PROVENANCE, "crs_release": release}
    with pytest.raises(ValueError, match=f"CRS release provenance is {release!r}"):
        verify_release_provenance(
            provenance=provenance,
            observed_before=OBSERVED_TOKENS,
            observed_after=OBSERVED_TOKENS,
        )


@pytest.mark.parametrize("release", [None, "supplied"], ids=["none", "supplied"])
def test_verify_release_provenance_raises_when_the_multisystem_release_is_not_a_record(
    release: object,
) -> None:
    """Test an unread or caller-supplied Members' Total Use leg is refused too"""
    provenance = {**VALID_PROVENANCE, "multisystem_release": release}
    with pytest.raises(
        ValueError, match=f"Members' Total Use release provenance is {release!r}"
    ):
        verify_release_provenance(
            provenance=provenance,
            observed_before=OBSERVED_TOKENS,
            observed_after=OBSERVED_TOKENS,
        )


def test_verify_release_provenance_raises_when_the_crs_release_id_mismatches() -> None:
    """Test a CRS release_id that disagrees with the pre-compute observed token is refused

    oda_data resolves its own token through oda_reader's 7-day annotation
    cache, so it can silently lag resolve_bulk_versions()'s bypassing lookup
    even when both run back-to-back.
    """
    provenance = {
        **VALID_PROVENANCE,
        "crs_release": {**CRS_RELEASE_RECORD, "release_id": "v20260701"},
    }
    with pytest.raises(ValueError, match="CRS release provenance reports"):
        verify_release_provenance(
            provenance=provenance,
            observed_before=OBSERVED_TOKENS,
            observed_after=OBSERVED_TOKENS,
        )


def test_verify_release_provenance_raises_when_the_multisystem_release_id_mismatches() -> (
    None
):
    """Test a Members' Total Use release_id mismatch is refused too"""
    provenance = {
        **VALID_PROVENANCE,
        "multisystem_release": {
            **MULTISYSTEM_RELEASE_RECORD,
            "release_id": "v20260601",
        },
    }
    with pytest.raises(
        ValueError, match="Members' Total Use release provenance reports"
    ):
        verify_release_provenance(
            provenance=provenance,
            observed_before=OBSERVED_TOKENS,
            observed_after=OBSERVED_TOKENS,
        )


def test_verify_release_provenance_raises_when_the_crs_token_moves_mid_compute() -> (
    None
):
    """Test a CRS republish between the two observations is refused

    The bulk file's URL stays stable across a republish, so the provenance
    check alone cannot see a republish that happens while the compute is
    running; only the second observation can.
    """
    observed_after = {**OBSERVED_TOKENS, "crs": "v20260901"}
    with pytest.raises(ValueError, match="CRS release moved"):
        verify_release_provenance(
            provenance=VALID_PROVENANCE,
            observed_before=OBSERVED_TOKENS,
            observed_after=observed_after,
        )


def test_verify_release_provenance_raises_when_the_multisystem_token_moves_mid_compute() -> (
    None
):
    """Test a Members' Total Use republish between the two observations is refused too"""
    observed_after = {**OBSERVED_TOKENS, "multisystem": "v20260901"}
    with pytest.raises(ValueError, match="Members' Total Use release moved"):
        verify_release_provenance(
            provenance=VALID_PROVENANCE,
            observed_before=OBSERVED_TOKENS,
            observed_after=observed_after,
        )


def _row(
    *,
    year: int,
    donor_code: int,
    channel_code: int,
    recipient_code: int | None,
    purpose_code: int | None,
    value: float,
    currency: str = "USD",
    prices: str = "current",
    allocation_status: str,
    share_channel_code: int | None,
    share_years: str | None,
) -> dict:
    return {
        ODASchema.YEAR: year,
        ODASchema.PROVIDER_CODE: donor_code,
        ODASchema.CHANNEL_CODE: channel_code,
        ODASchema.RECIPIENT_CODE: recipient_code,
        ODASchema.PURPOSE_CODE: purpose_code,
        ODASchema.VALUE: value,
        ODASchema.CURRENCY: currency,
        ODASchema.PRICES: prices,
        "allocation_status": allocation_status,
        "share_channel_code": share_channel_code,
        "share_years": share_years,
    }


def _result_frame(rows: list[dict], *, provenance: dict | None) -> pd.DataFrame:
    """A 2.8.1-shaped imputed_multilateral_by_purpose result frame"""
    frame = pd.DataFrame(rows).astype(
        {
            ODASchema.RECIPIENT_CODE: "Int64",
            ODASchema.PURPOSE_CODE: "Int64",
            "share_channel_code": "Int64",
        }
    )
    frame.attrs["provenance"] = provenance
    return frame


# A frame stratified by allocation_status, with an unallocated row per year
# (null recipient/purpose) and one negative value, used to hand-check
# build_quality_metadata's output.
QUALITY_ROWS = [
    _row(
        year=2021,
        donor_code=1,
        channel_code=100,
        recipient_code=200,
        purpose_code=300,
        value=10.0,
        allocation_status="imputed",
        share_channel_code=100,
        share_years="2019-2021",
    ),
    _row(
        year=2021,
        donor_code=1,
        channel_code=100,
        recipient_code=201,
        purpose_code=301,
        value=5.0,
        allocation_status="imputed",
        share_channel_code=100,
        share_years="2019-2021",
    ),
    _row(
        year=2021,
        donor_code=2,
        channel_code=110,
        recipient_code=None,
        purpose_code=None,
        value=3.0,
        allocation_status="unallocated",
        share_channel_code=None,
        share_years=None,
    ),
    _row(
        year=2022,
        donor_code=1,
        channel_code=100,
        recipient_code=200,
        purpose_code=300,
        value=-2.0,
        allocation_status="imputed",
        share_channel_code=100,
        share_years="2020-2022",
    ),
    _row(
        year=2022,
        donor_code=3,
        channel_code=120,
        recipient_code=None,
        purpose_code=None,
        value=7.0,
        allocation_status="unallocated",
        share_channel_code=None,
        share_years=None,
    ),
    _row(
        year=2022,
        donor_code=4,
        channel_code=130,
        recipient_code=210,
        purpose_code=310,
        value=4.0,
        allocation_status="stale_share",
        share_channel_code=130,
        share_years="2018-2020",
    ),
    _row(
        year=2022,
        donor_code=5,
        channel_code=140,
        recipient_code=220,
        purpose_code=320,
        value=4.0,
        allocation_status="proxy",
        share_channel_code=140,
        share_years="2019-2021",
    ),
    _row(
        year=2022,
        donor_code=5,
        channel_code=140,
        recipient_code=221,
        purpose_code=321,
        value=4.0,
        allocation_status="proxy",
        share_channel_code=140,
        share_years="2019-2021",
    ),
    _row(
        year=2022,
        donor_code=5,
        channel_code=140,
        recipient_code=222,
        purpose_code=322,
        value=4.0,
        allocation_status="proxy",
        share_channel_code=140,
        share_years="2019-2021",
    ),
]


def test_build_quality_metadata_reports_hand_computed_values() -> None:
    """Test the metadata matches values worked out by hand from QUALITY_ROWS

    Grand total value is 39 (13 imputed + 10 unallocated + 4 stale_share + 12
    proxy). 2021's total is 18 with 3 unallocated (1/6); 2022's total is 21
    with 7 unallocated (1/3). One row (donor 1, channel 100, 2022) is
    negative at -2.0. Of the four (share_channel_code, share_years) windows,
    three concentrate all their money in one purpose code (a single row each
    for channel 100/2020-2022 and channel 130/2018-2020, and a 2:1 split for
    channel 100/2019-2021); the fourth (channel 140/2019-2021) splits evenly
    across three purposes at 1/3 each, under the 0.5 flag threshold.
    """
    frame = _result_frame(QUALITY_ROWS, provenance=VALID_PROVENANCE)

    metadata = build_quality_metadata(frame, provenance=VALID_PROVENANCE)

    assert metadata["value_share_status_imputed"] == dg.MetadataValue.float(13 / 39)
    assert metadata["value_share_status_unallocated"] == dg.MetadataValue.float(10 / 39)
    assert metadata["value_share_status_stale_share"] == dg.MetadataValue.float(4 / 39)
    assert metadata["value_share_status_proxy"] == dg.MetadataValue.float(12 / 39)

    table = metadata["unallocated_value_share_by_year"]
    assert isinstance(table, dg.TableMetadataValue)
    by_year = {
        row.data["year"]: row.data["unallocated_value_share"] for row in table.records
    }
    assert by_year[2021] == pytest.approx(3 / 18)
    assert by_year[2022] == pytest.approx(7 / 21)

    assert metadata["negative_row_count"] == dg.MetadataValue.int(1)
    assert metadata["negative_value_sum"] == dg.MetadataValue.float(-2.0)
    assert metadata[
        "single_purpose_concentration_flagged_count"
    ] == dg.MetadataValue.int(3)
    assert metadata["crosswalk_vintage"] == dg.MetadataValue.text("2026-01-01")
    assert metadata["package_version"] == dg.MetadataValue.text("2.8.1")
    assert metadata["crs_release_id"] == dg.MetadataValue.text(CRS_VERSION)
    assert metadata["multisystem_release_id"] == dg.MetadataValue.text(MULTI_VERSION)


def _patched_bulk_versions(*results: tuple[tuple[str, str], tuple[str, str]]):
    """Stand in for resolve_bulk_versions, returning the given (crs, multi) pairs in order"""
    if len(results) == 1:
        return patch(
            f"{ASSETS_MODULE}.resolve_bulk_versions",
            autospec=True,
            return_value=results[0],
        )
    return patch(
        f"{ASSETS_MODULE}.resolve_bulk_versions",
        autospec=True,
        side_effect=list(results),
    )


def test_source_observe_fn_returns_metadata_and_data_version() -> None:
    """Test the source's observe fn records both releases and joins their data version"""
    observe = one_imputed_multilateral_sectors_source.observe_fn
    assert observe is not None

    with (
        _patched_bulk_versions(
            ((CRS_TEST_URL, CRS_VERSION), (MULTI_TEST_URL, MULTI_VERSION))
        ),
        patch(f"{ASSETS_MODULE}.installed_version", return_value="2.8.1"),
    ):
        result = observe()

    assert isinstance(result, dg.ObserveResult)
    assert result.data_version == dg.DataVersion(
        ";".join([CRS_TEST_URL, CRS_VERSION, MULTI_TEST_URL, MULTI_VERSION, "2.8.1"])
    )
    metadata = result.metadata
    assert metadata is not None
    assert metadata["crs_url"] == dg.MetadataValue.text(CRS_TEST_URL)
    assert metadata["crs_version"] == dg.MetadataValue.text(CRS_VERSION)
    assert metadata["multisystem_url"] == dg.MetadataValue.text(MULTI_TEST_URL)
    assert metadata["multisystem_version"] == dg.MetadataValue.text(MULTI_VERSION)
    assert metadata["oda_data_version"] == dg.MetadataValue.text("2.8.1")


def test_source_data_version_moves_when_the_crs_token_alone_changes() -> None:
    """Test a CRS republish alone is enough to move the observed data version

    Both upstreams are fingerprinted because either can move on its own; a
    data version that only tracked one input would leave the pipeline stale
    after a CRS-only republish.
    """
    observe = one_imputed_multilateral_sectors_source.observe_fn
    assert observe is not None

    with _patched_bulk_versions(
        ((CRS_TEST_URL, CRS_VERSION), (MULTI_TEST_URL, MULTI_VERSION))
    ):
        baseline = observe()
    with _patched_bulk_versions(
        ((CRS_TEST_URL, "v20260901"), (MULTI_TEST_URL, MULTI_VERSION))
    ):
        changed = observe()

    assert baseline.data_version != changed.data_version


def test_source_data_version_moves_when_the_multisystem_token_alone_changes() -> None:
    """Test a Members' Total Use republish alone is enough to move the observed data version"""
    observe = one_imputed_multilateral_sectors_source.observe_fn
    assert observe is not None

    with _patched_bulk_versions(
        ((CRS_TEST_URL, CRS_VERSION), (MULTI_TEST_URL, MULTI_VERSION))
    ):
        baseline = observe()
    with _patched_bulk_versions(
        ((CRS_TEST_URL, CRS_VERSION), (MULTI_TEST_URL, "v20260901"))
    ):
        changed = observe()

    assert baseline.data_version != changed.data_version


def test_source_data_version_moves_when_the_oda_data_version_alone_changes() -> None:
    """Test an oda-data upgrade alone is enough to move the observed data version

    oda-data computes the imputation, so a new release of it changes the
    result even when neither OECD source has republished.
    """
    observe = one_imputed_multilateral_sectors_source.observe_fn
    assert observe is not None

    with (
        _patched_bulk_versions(
            ((CRS_TEST_URL, CRS_VERSION), (MULTI_TEST_URL, MULTI_VERSION))
        ),
        patch(f"{ASSETS_MODULE}.installed_version", return_value="2.8.1"),
    ):
        baseline = observe()
    with (
        _patched_bulk_versions(
            ((CRS_TEST_URL, CRS_VERSION), (MULTI_TEST_URL, MULTI_VERSION))
        ),
        patch(f"{ASSETS_MODULE}.installed_version", return_value="2.9.0"),
    ):
        changed = observe()

    assert baseline.data_version != changed.data_version


DOWNLOADED_AT = datetime(2026, 8, 3, 23, 47, 5, tzinfo=UTC)


@contextmanager
def _patch_download_clock():
    """Freeze the instant the raw file asset stamps a download with"""
    with patch(f"{ASSETS_MODULE}.datetime") as mock_datetime:
        mock_datetime.now.return_value = DOWNLOADED_AT
        yield


CONFORMING_ROWS = [
    _row(
        year=2022,
        donor_code=1,
        channel_code=100,
        recipient_code=200,
        purpose_code=300,
        value=1.0,
        allocation_status="imputed",
        share_channel_code=100,
        share_years="2020-2022",
    ),
    _row(
        year=2022,
        donor_code=2,
        channel_code=110,
        recipient_code=201,
        purpose_code=301,
        value=1.0,
        allocation_status="imputed",
        share_channel_code=110,
        share_years="2020-2022",
    ),
    _row(
        year=2022,
        donor_code=3,
        channel_code=120,
        recipient_code=202,
        purpose_code=302,
        value=1.0,
        allocation_status="imputed",
        share_channel_code=120,
        share_years="2020-2022",
    ),
]


def _matching_bulk_and_frame(rows: list[dict], *, provenance: dict | None):
    """Context managers patching resolve_bulk_versions (twice, matching) and the compute"""
    frame = _result_frame(rows, provenance=provenance)
    bulk_patch = _patched_bulk_versions(
        ((CRS_TEST_URL, CRS_VERSION), (MULTI_TEST_URL, MULTI_VERSION)),
        ((CRS_TEST_URL, CRS_VERSION), (MULTI_TEST_URL, MULTI_VERSION)),
    )
    compute_patch = patch(
        f"{ASSETS_MODULE}.sector_imputations.imputed_multilateral_by_purpose",
        autospec=True,
        return_value=frame,
    )
    return bulk_patch, compute_patch


def test_raw_file_raises_when_imputed_multilateral_by_purpose_returns_mixed_currencies(
    tmp_path: Path,
) -> None:
    """Test a currency other than USD in the returned frame stops the asset

    Nothing else checks this: a silent oda_data default change here would
    publish figures in the wrong currency while still labelling them
    dcid:CurrencyUSD.
    """
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))
    rows = [
        {**CONFORMING_ROWS[0], ODASchema.CURRENCY: "USD"},
        {**CONFORMING_ROWS[1], ODASchema.CURRENCY: "EUR"},
    ]
    bulk_patch, compute_patch = _matching_bulk_and_frame(
        rows, provenance=VALID_PROVENANCE
    )

    with (
        bulk_patch,
        compute_patch,
        pytest.raises(ValueError, match="unexpected currencies"),
    ):
        one_imputed_multilateral_sectors_raw_file(
            context=dg.build_asset_context(), raw_file_store=file_store
        )


def test_raw_file_raises_when_imputed_multilateral_by_purpose_returns_non_current_prices(
    tmp_path: Path,
) -> None:
    """Test constant-price rows in the returned frame stop the asset

    base_year stays unset so imputed_multilateral_by_purpose is expected to
    report "current" prices; a silent default change to constant prices would
    publish figures still labelled dcid:Nominal.
    """
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))
    rows = [
        {**CONFORMING_ROWS[0], ODASchema.PRICES: "current"},
        {**CONFORMING_ROWS[1], ODASchema.PRICES: "constant"},
    ]
    bulk_patch, compute_patch = _matching_bulk_and_frame(
        rows, provenance=VALID_PROVENANCE
    )

    with (
        bulk_patch,
        compute_patch,
        pytest.raises(ValueError, match="unexpected prices"),
    ):
        one_imputed_multilateral_sectors_raw_file(
            context=dg.build_asset_context(), raw_file_store=file_store
        )


def test_raw_file_calls_the_compute_with_refresh_and_flow_types(tmp_path: Path) -> None:
    """Test the compute is forced fresh and the ODA+OOF share basis is explicit

    imputed_multilateral_by_purpose has no cache to invalidate directly;
    refresh=True is what forces a fresh CRS and Members' Total Use read.
    flow_types is passed explicitly rather than relied on as a default; see
    FLOW_TYPES for why both flow types are needed.
    """
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))
    bulk_patch, compute_patch = _matching_bulk_and_frame(
        CONFORMING_ROWS, provenance=VALID_PROVENANCE
    )

    with (
        _patch_download_clock(),
        bulk_patch,
        compute_patch as imputed_multilateral_by_purpose,
    ):
        one_imputed_multilateral_sectors_raw_file(
            context=dg.build_asset_context(), raw_file_store=file_store
        )

    imputed_multilateral_by_purpose.assert_called_once_with(
        currency=CURRENCY, base_year=None, flow_types=FLOW_TYPES, refresh=True
    )


def test_raw_file_writes_the_parquet_and_metadata_for_a_conforming_frame(
    tmp_path: Path,
) -> None:
    """Test a conforming frame passes the guards and is written out in full

    A conforming frame carries {"USD"} currency and {"current"} prices,
    with matching provenance.
    """
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))
    directory = file_store.build_date_partitioned_path(
        "one_imputed_multilateral_sectors/imputed_multilateral_sectors",
        partition_date=DOWNLOADED_AT.date(),
    )
    expected = _result_frame(CONFORMING_ROWS, provenance=VALID_PROVENANCE)
    bulk_patch = _patched_bulk_versions(
        ((CRS_TEST_URL, CRS_VERSION), (MULTI_TEST_URL, MULTI_VERSION)),
        ((CRS_TEST_URL, CRS_VERSION), (MULTI_TEST_URL, MULTI_VERSION)),
    )
    compute_patch = patch(
        f"{ASSETS_MODULE}.sector_imputations.imputed_multilateral_by_purpose",
        autospec=True,
        return_value=expected,
    )

    with _patch_download_clock(), bulk_patch, compute_patch:
        result = one_imputed_multilateral_sectors_raw_file(
            context=dg.build_asset_context(), raw_file_store=file_store
        )

    written = pd.read_parquet(directory / "data.parquet")
    # Parquet round trip preserves the nullable Int64 columns and float/str
    # dtypes here, so the frame comes back byte-for-byte comparable.
    pd.testing.assert_frame_equal(written, expected)

    recorded = read_extraction_metadata(directory, release_names=("crs", "multisystem"))
    assert recorded.downloaded_at == DOWNLOADED_AT
    assert recorded.release_dates == {
        "crs": date(2026, 8, 1),
        "multisystem": date(2026, 7, 15),
    }

    assert isinstance(result, dg.MaterializeResult)
    assert result.value == str(directory / "data.parquet")
    assert meta(result, "dagster/row_count", dg.IntMetadataValue).value == 3
    assert meta(result, "crs_version", dg.TextMetadataValue).value == CRS_VERSION
    assert (
        meta(result, "multisystem_version", dg.TextMetadataValue).value == MULTI_VERSION
    )
    assert meta(result, "crs_release_id", dg.TextMetadataValue).value == CRS_VERSION
    assert (
        meta(result, "multisystem_release_id", dg.TextMetadataValue).value
        == MULTI_VERSION
    )
    assert meta(result, "package_version", dg.TextMetadataValue).value == "2.8.1"
    assert meta(result, "crs_release_date", dg.TextMetadataValue).value == "2026-08-01"
    assert (
        meta(result, "multisystem_release_date", dg.TextMetadataValue).value
        == "2026-07-15"
    )
    assert (
        meta(result, "value_share_status_imputed", dg.FloatMetadataValue).value == 1.0
    )
    [check] = result.check_results
    assert check.check_name == "file_not_empty"
    assert check.passed


def _refused_and_nothing_written(tmp_path: Path, *, bulk_patch, compute_patch) -> None:
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))
    directory = file_store.build_date_partitioned_path(
        "one_imputed_multilateral_sectors/imputed_multilateral_sectors",
        partition_date=DOWNLOADED_AT.date(),
    )

    with _patch_download_clock(), bulk_patch, compute_patch, pytest.raises(ValueError):
        one_imputed_multilateral_sectors_raw_file(
            context=dg.build_asset_context(), raw_file_store=file_store
        )

    assert not (directory / "data.parquet").exists()
    assert not (directory / "metadata.json").exists()


def test_raw_file_writes_nothing_when_provenance_is_missing(tmp_path: Path) -> None:
    """Test a result with no provenance attrs refuses before writing anything"""
    bulk_patch, compute_patch = _matching_bulk_and_frame(
        CONFORMING_ROWS, provenance=None
    )
    _refused_and_nothing_written(
        tmp_path, bulk_patch=bulk_patch, compute_patch=compute_patch
    )


def test_raw_file_writes_nothing_when_a_release_is_none(tmp_path: Path) -> None:
    """Test an unread CRS leg refuses before writing anything"""
    provenance = {**VALID_PROVENANCE, "crs_release": None}
    bulk_patch, compute_patch = _matching_bulk_and_frame(
        CONFORMING_ROWS, provenance=provenance
    )
    _refused_and_nothing_written(
        tmp_path, bulk_patch=bulk_patch, compute_patch=compute_patch
    )


def test_raw_file_writes_nothing_when_a_release_is_supplied(tmp_path: Path) -> None:
    """Test a caller-supplied Members' Total Use leg refuses before writing anything"""
    provenance = {**VALID_PROVENANCE, "multisystem_release": "supplied"}
    bulk_patch, compute_patch = _matching_bulk_and_frame(
        CONFORMING_ROWS, provenance=provenance
    )
    _refused_and_nothing_written(
        tmp_path, bulk_patch=bulk_patch, compute_patch=compute_patch
    )


def test_raw_file_writes_nothing_when_the_crs_token_mismatches(tmp_path: Path) -> None:
    """Test a CRS provenance token that disagrees with the observed one refuses"""
    provenance = {
        **VALID_PROVENANCE,
        "crs_release": {**CRS_RELEASE_RECORD, "release_id": "v20260701"},
    }
    bulk_patch, compute_patch = _matching_bulk_and_frame(
        CONFORMING_ROWS, provenance=provenance
    )
    _refused_and_nothing_written(
        tmp_path, bulk_patch=bulk_patch, compute_patch=compute_patch
    )


def test_raw_table_loads_the_parquet_fixture(tmp_path: Path) -> None:
    """Test the raw table asset loads the extracted parquet into DuckDB"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    expected_rows = len(pd.read_parquet(FIXTURES / "data.parquet"))

    result = one_imputed_multilateral_sectors_raw_table(
        duckdb=duckdb,
        one_imputed_multilateral_sectors_raw_file=str(FIXTURES / "data.parquet"),
    )

    assert (
        row_count(
            duckdb,
            "one_imputed_multilateral_sectors.raw_imputed_multilateral_sectors",
        )
        == expected_rows
    )
    assert isinstance(result, dg.MaterializeResult)
    assert result.metadata == {
        "dagster/table_name": dg.TextMetadataValue(
            "one_imputed_multilateral_sectors.raw_imputed_multilateral_sectors"
        ),
        "dagster/row_count": dg.IntMetadataValue(expected_rows),
    }
    [check] = result.check_results
    assert check.check_name == "table_not_empty"
    assert check.passed


def test_raw_table_check_fails_when_table_empty(tmp_path: Path) -> None:
    """Test the raw table check fails when the parquet file has no rows"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    empty = pd.read_parquet(FIXTURES / "data.parquet").iloc[0:0]
    empty_path = tmp_path / "empty.parquet"
    empty.to_parquet(empty_path, index=False)

    result = one_imputed_multilateral_sectors_raw_table(
        duckdb=duckdb, one_imputed_multilateral_sectors_raw_file=str(empty_path)
    )

    assert (
        row_count(
            duckdb,
            "one_imputed_multilateral_sectors.raw_imputed_multilateral_sectors",
        )
        == 0
    )
    assert isinstance(result, dg.MaterializeResult)
    [check] = result.check_results
    assert check.check_name == "table_not_empty"
    assert not check.passed


def test_raw_table_overwrites_existing_table(tmp_path: Path) -> None:
    """Test the raw table asset replaces the table rather than appending"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    full = pd.read_parquet(FIXTURES / "data.parquet")
    subset_path = tmp_path / "subset.parquet"
    full.head(5).to_parquet(subset_path, index=False)

    one_imputed_multilateral_sectors_raw_table(
        duckdb=duckdb,
        one_imputed_multilateral_sectors_raw_file=str(FIXTURES / "data.parquet"),
    )
    assert row_count(
        duckdb,
        "one_imputed_multilateral_sectors.raw_imputed_multilateral_sectors",
    ) == len(full)

    one_imputed_multilateral_sectors_raw_table(
        duckdb=duckdb, one_imputed_multilateral_sectors_raw_file=str(subset_path)
    )

    assert (
        row_count(
            duckdb,
            "one_imputed_multilateral_sectors.raw_imputed_multilateral_sectors",
        )
        == 5
    )


def test_raw_metadata_loads_the_recorded_extraction_metadata(tmp_path: Path) -> None:
    """Test the metadata asset loads both sources' release dates under their own columns

    The two dates fall in different years so a query that mixed the two up
    (or collapsed them to one column) would be caught by the value
    assertions, not just the column list.
    """
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    write_extraction_metadata(
        UPath(tmp_path),
        downloaded_at=DOWNLOADED_AT,
        release_dates={"crs": date(2026, 8, 1), "multisystem": date(2025, 7, 15)},
    )

    result = one_imputed_multilateral_sectors_raw_metadata(
        duckdb=duckdb,
        one_imputed_multilateral_sectors_raw_file=str(tmp_path / "data.parquet"),
    )

    assert isinstance(result, dg.MaterializeResult)
    assert result.metadata == {
        "dagster/table_name": dg.TextMetadataValue(
            "one_imputed_multilateral_sectors.raw_imputed_multilateral_sectors_metadata"
        ),
        "dagster/row_count": dg.IntMetadataValue(1),
    }

    table = "one_imputed_multilateral_sectors.raw_imputed_multilateral_sectors_metadata"
    assert column_list(duckdb, table) == [
        "downloaded_at",
        "crs_release_date",
        "multisystem_release_date",
    ]
    assert list_first_row(duckdb, table) == [
        DOWNLOADED_AT,
        pd.Timestamp(2026, 8, 1),
        pd.Timestamp(2025, 7, 15),
    ]


def test_raw_metadata_raises_when_the_extraction_recorded_a_single_release_date(
    tmp_path: Path,
) -> None:
    """Test the metadata asset refuses a single-source `source_release_date` record

    The asset expects per-source `crs_release_date` and
    `multisystem_release_date` columns; a record shaped like dac_crs's single
    `source_release_date` is missing both and must not load silently.
    """
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    (UPath(tmp_path) / "metadata.json").write_text(
        json.dumps(
            {
                "downloaded_at": DOWNLOADED_AT.isoformat(),
                "source_release_date": date(2026, 8, 1).isoformat(),
            }
        )
    )

    with pytest.raises(ValueError, match="release date keys do not match"):
        one_imputed_multilateral_sectors_raw_metadata(
            duckdb=duckdb,
            one_imputed_multilateral_sectors_raw_file=str(tmp_path / "data.parquet"),
        )


def test_raw_file_path_reaches_raw_table_and_raw_metadata(tmp_path: Path) -> None:
    """Test the raw file asset's output path is injected into both downstream assets"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    bulk_patch, compute_patch = _matching_bulk_and_frame(
        CONFORMING_ROWS, provenance=VALID_PROVENANCE
    )

    with _patch_download_clock(), bulk_patch, compute_patch:
        result = dg.materialize(
            [
                one_imputed_multilateral_sectors_raw_file,
                one_imputed_multilateral_sectors_raw_table,
                one_imputed_multilateral_sectors_raw_metadata,
            ],
            resources={"raw_file_store": file_store, "duckdb": duckdb},
        )

    assert result.success
    assert (
        row_count(
            duckdb,
            "one_imputed_multilateral_sectors.raw_imputed_multilateral_sectors",
        )
        == 3
    )
    assert (
        row_count(
            duckdb,
            "one_imputed_multilateral_sectors"
            ".raw_imputed_multilateral_sectors_metadata",
        )
        == 1
    )


def test_raw_file_writes_nothing_when_the_multisystem_token_mismatches(
    tmp_path: Path,
) -> None:
    """Test a Members' Total Use provenance token that disagrees with the observed one refuses"""
    provenance = {
        **VALID_PROVENANCE,
        "multisystem_release": {
            **MULTISYSTEM_RELEASE_RECORD,
            "release_id": "v20260601",
        },
    }
    bulk_patch, compute_patch = _matching_bulk_and_frame(
        CONFORMING_ROWS, provenance=provenance
    )
    _refused_and_nothing_written(
        tmp_path, bulk_patch=bulk_patch, compute_patch=compute_patch
    )


def test_raw_file_writes_nothing_when_a_release_moves_mid_compute(
    tmp_path: Path,
) -> None:
    """Test a republish observed between the pre- and post-compute checks refuses

    The bulk file's URL stays stable across a republish, so oda_data can
    download the new file under the old token; only comparing the token
    observed before the compute to one observed after can catch this.
    """
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))
    frame = _result_frame(CONFORMING_ROWS, provenance=VALID_PROVENANCE)
    bulk_patch = _patched_bulk_versions(
        ((CRS_TEST_URL, CRS_VERSION), (MULTI_TEST_URL, MULTI_VERSION)),
        ((CRS_TEST_URL, "v20260901"), (MULTI_TEST_URL, MULTI_VERSION)),
    )
    compute_patch = patch(
        f"{ASSETS_MODULE}.sector_imputations.imputed_multilateral_by_purpose",
        autospec=True,
        return_value=frame,
    )
    directory = file_store.build_date_partitioned_path(
        "one_imputed_multilateral_sectors/imputed_multilateral_sectors",
        partition_date=DOWNLOADED_AT.date(),
    )

    with (
        _patch_download_clock(),
        bulk_patch,
        compute_patch,
        pytest.raises(ValueError, match="CRS release moved"),
    ):
        one_imputed_multilateral_sectors_raw_file(
            context=dg.build_asset_context(), raw_file_store=file_store
        )

    assert not (directory / "data.parquet").exists()
    assert not (directory / "metadata.json").exists()


def test_raw_file_writes_nothing_when_a_release_token_carries_no_date(
    tmp_path: Path,
) -> None:
    """Test a non-date release token refuses before any file is written

    resolve_bulk_versions already refuses a token like this itself; this
    covers the asset's own write ordering directly, by patching
    resolve_bulk_versions to return an ETag-shaped token that agrees with
    provenance and so clears verify_release_provenance, in case that guard
    is ever bypassed.
    """
    provenance = {
        **VALID_PROVENANCE,
        "crs_release": {**CRS_RELEASE_RECORD, "release_id": ETAG_TOKEN},
    }
    frame = _result_frame(CONFORMING_ROWS, provenance=provenance)
    bulk_patch = _patched_bulk_versions(
        ((CRS_TEST_URL, ETAG_TOKEN), (MULTI_TEST_URL, MULTI_VERSION)),
        ((CRS_TEST_URL, ETAG_TOKEN), (MULTI_TEST_URL, MULTI_VERSION)),
    )
    compute_patch = patch(
        f"{ASSETS_MODULE}.sector_imputations.imputed_multilateral_by_purpose",
        autospec=True,
        return_value=frame,
    )
    _refused_and_nothing_written(
        tmp_path, bulk_patch=bulk_patch, compute_patch=compute_patch
    )


def test_raw_file_check_fails_when_no_rows(tmp_path: Path) -> None:
    """Test the file_not_empty check fails on a frame with no rows"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))
    frame = _result_frame(CONFORMING_ROWS, provenance=VALID_PROVENANCE).iloc[:0]
    frame.attrs["provenance"] = VALID_PROVENANCE
    bulk_patch = _patched_bulk_versions(
        ((CRS_TEST_URL, CRS_VERSION), (MULTI_TEST_URL, MULTI_VERSION)),
        ((CRS_TEST_URL, CRS_VERSION), (MULTI_TEST_URL, MULTI_VERSION)),
    )
    compute_patch = patch(
        f"{ASSETS_MODULE}.sector_imputations.imputed_multilateral_by_purpose",
        autospec=True,
        return_value=frame,
    )

    with _patch_download_clock(), bulk_patch, compute_patch:
        result = one_imputed_multilateral_sectors_raw_file(
            context=dg.build_asset_context(), raw_file_store=file_store
        )

    assert isinstance(result, dg.MaterializeResult)
    [check] = result.check_results
    assert check.check_name == "file_not_empty"
    assert not check.passed


IMPUTED_STATVAR_DCID = (
    "dcid:AttributedAmount_FinancialFlow_Gross_Disbursement_CRSTypeOfFlow10_"
    "DAC2aAidType106_CRSPurposeCode14010_Nominal"
)

IMPUTED_PROVENANCE_SQL = """
create or replace table
    one_imputed_multilateral_sectors.dc_imputed_multilateral_sectors_provenance
as
select
    'ONE Imputed Multilateral Sectors' as name,
    'source/ONEData' as source,
    'https://example.org/imputed' as url,
    'https://example.org/imputed/crs,https://example.org/imputed/multi'
        as source_data_url,
    'https://example.org/terms' as license,
    'dcid:TestLicenseType' as license_type,
    'P6M' as source_release_frequency,
    '2026-09-22' as imported_source_release_date,
    '2026-12' as next_source_release_date,
    '2026-12' as next_data_refresh_date,
    '2021' as earliest_observation_date,
    '2022' as latest_observation_date,
    'Test description' as description,
    'Test attribution' as license_attribution
"""

IMPUTED_STATVARS_SQL = f"""
create or replace table
    one_imputed_multilateral_sectors.dc_imputed_multilateral_sectors_statvars
as
select * from (values
    (
        '{IMPUTED_STATVAR_DCID}',
        'Test statvar',
        'dcid:FinancialFlow',
        'dcid:attributedAmount',
        'dcid:measuredValue',
        'nettingBasis,financialFlowType,crsTypeOfFlow,dac2aAidType,crsPurpose',
        'dcid:Gross',
        'dcid:Disbursement',
        'dcid:CRSTypeOfFlow10',
        'dcid:DAC2aAidType106',
        'dcid:CRSPurposeCode14010',
        'dcid:Nominal',
        'financingProvider,financingRecipient'
    )
) as statvars (
    dcid,
    name,
    population_type,
    measured_property,
    stat_type,
    constraint_properties,
    netting_basis,
    financial_flow_type,
    type_of_flow,
    aid_type,
    purpose_code,
    measurement_qualifier,
    observation_properties
)
"""

IMPUTED_OBSERVATIONS_SQL = f"""
create or replace table
    one_imputed_multilateral_sectors.dc_imputed_multilateral_sectors_observations
as
select
    variable,
    date::integer as date,
    financing_provider,
    financing_recipient,
    value::decimal(18, 2) as value,
    unit,
    measurement_method
from (values
    (
        '{IMPUTED_STATVAR_DCID}', 2021, 'country/FRA', 'country/KEN', 1.5,
        'USDollar', 'dcid:Modelled'
    ),
    (
        '{IMPUTED_STATVAR_DCID}', 2022, 'country/FRA', 'country/KEN', 2.5,
        'USDollar', 'dcid:Modelled'
    )
) as observations (
    variable, date, financing_provider, financing_recipient, value, unit,
    measurement_method
)
"""


def _create_dc_models(duckdb: DuckDBResource) -> None:
    """Create the DC schema models the export reads, holding a small fixture bundle"""
    with duckdb.get_connection() as connection:
        connection.execute(
            "create schema if not exists one_imputed_multilateral_sectors"
        )
        connection.execute(IMPUTED_PROVENANCE_SQL)
        connection.execute(IMPUTED_STATVARS_SQL)
        connection.execute(IMPUTED_OBSERVATIONS_SQL)


def _run_export(tmp_path: Path) -> tuple[dg.MaterializeResult, Path]:
    """Materialize the export asset, returning the result and the bundle directory"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "processed"))
    _create_dc_models(duckdb)

    with patch("dfo_pipelines.defs.resources.date") as mock_date:
        mock_date.today.return_value = date(2026, 8, 3)
        out_dir = file_store.build_date_partitioned_path(
            IMPUTED_MULTILATERAL_SECTORS_EXPORT_SPEC.import_name
        )
        result = one_imputed_multilateral_sectors_dc_export(
            context=dg.build_asset_context(),
            duckdb=duckdb,
            export_file_store=file_store,
        )

    assert isinstance(result, dg.MaterializeResult)
    return result, Path(str(out_dir))


def test_dc_export_writes_a_bundle_to_a_date_partitioned_path(tmp_path: Path) -> None:
    """Test the export asset reports the bundle it wrote to today's partition"""
    result, out_dir = _run_export(tmp_path)

    assert (
        out_dir
        == tmp_path
        / "processed"
        / "one_imputed_multilateral_sectors"
        / "2026"
        / "08"
        / "03"
    )
    assert result.metadata == {
        "dagster/uri": dg.TextMetadataValue(str(out_dir)),
        "observation_count": dg.IntMetadataValue(2),
        "statvar_count": dg.IntMetadataValue(1),
        "observation_file_count": dg.IntMetadataValue(2),
    }


def test_dc_ingestion_ingests_the_bundle_the_export_wrote(
    tmp_path: Path, knowledge_graph: KnowledgeGraphResource
) -> None:
    """Test the ingestion is handed the export's directory under its import name"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    _create_dc_models(duckdb)
    import_name = IMPUTED_MULTILATERAL_SECTORS_EXPORT_SPEC.import_name
    destination = f"gs://test-bucket/ingestion/input/{import_name}"

    with patch(f"{ASSETS_MODULE}.ingest_bundle", return_value=destination) as ingest:
        result = dg.materialize(
            [
                one_imputed_multilateral_sectors_dc_export,
                one_imputed_multilateral_sectors_dc_ingestion,
            ],
            resources={
                "duckdb": duckdb,
                "export_file_store": UPathFileStoreResource(
                    base_path=str(tmp_path / "processed")
                ),
                "knowledge_graph": knowledge_graph,
            },
        )

    assert result.success
    [export] = result.asset_materializations_for_node(
        "one_imputed_multilateral_sectors_dc_export"
    )
    [ingestion] = result.asset_materializations_for_node(
        "one_imputed_multilateral_sectors_dc_ingestion"
    )
    ingest.assert_called_once()
    args, kwargs = ingest.call_args
    assert str(args[0]) == export.metadata["dagster/uri"].value
    assert kwargs["import_name"] == import_name
    assert kwargs["settings"] == knowledge_graph.settings
    assert ingestion.metadata["dagster/uri"].value == destination


def test_dc_export_writes_the_pipeline_documentation_onto_the_provenance_node(
    tmp_path: Path,
) -> None:
    """Test the Provenance node carries this pipeline's transformations and changelog"""
    _, out_dir = _run_export(tmp_path)

    provenance = (out_dir / "provenance.mcf").read_text()
    for property_name, file_name in DOCUMENTATION_FILES.items():
        first_line = (DOCS_DIR / file_name).read_text().splitlines()[0]
        assert f'{property_name}: "{first_line}' in provenance


def test_dc_export_writes_the_provenance_source_description_and_refresh_date(
    tmp_path: Path,
) -> None:
    """Test the Provenance node names the ONE Data source and carries the description, next refresh date and both source data URLs"""
    _, out_dir = _run_export(tmp_path)

    provenance = (out_dir / "provenance.mcf").read_text()
    assert "source: dcid:source/ONEData\n" in provenance
    assert 'description: "Test description"\n' in provenance
    assert 'nextDataRefreshDate: "2026-12"\n' in provenance
    assert (
        'sourceDataUrl: "https://example.org/imputed/crs,'
        'https://example.org/imputed/multi"\n'
    ) in provenance


def test_dc_export_writes_the_conversion_recipe_onto_the_provenance_node(
    tmp_path: Path,
) -> None:
    """Test the Provenance node carries the spec's conversion recipe as a conversionRecipe arc"""
    _, out_dir = _run_export(tmp_path)

    provenance = (out_dir / "provenance.mcf").read_text()
    assert "conversionRecipe: dcid:ConversionRecipeDAC\n" in provenance


def test_dc_export_writes_the_import_config(tmp_path: Path) -> None:
    """Test the bundle's config registers its files, columns and observation period"""
    _, out_dir = _run_export(tmp_path)

    assert json.loads((out_dir / "config.json").read_text()) == {
        "importName": "one_imputed_multilateral_sectors",
        "inputFiles": [
            {
                "pattern": "*.mcf",
                "provenance": "dcid:provenance/ONEImputedMultilateralSectors",
            },
            {
                "pattern": "observations_*.csv",
                "provenance": "dcid:provenance/ONEImputedMultilateralSectors",
                "columnMappings": {
                    "dcid:variableMeasured": "variable",
                    "dcid:observationDate": "date",
                    "dcid:value": "value",
                    "dcid:unit": "unit",
                    "dcid:measurementMethod": "measurement_method",
                    "custom:financingProvider": "financing_provider",
                    "custom:financingRecipient": "financing_recipient",
                },
                "observationProperties": {"observationPeriod": "P1Y"},
                "format": "variablePerRow",
            },
        ],
    }


def test_dc_export_maps_the_statvar_dimensions_to_ontology_properties(
    tmp_path: Path,
) -> None:
    """Test the export asset names each StatVar dimension as its ontology property

    This is the piece of one_imputed_multilateral_sectors_dc_export that is
    specific to this pipeline: which model columns become which properties.
    export_bundle's own plumbing is covered by oecd/dac_crs's export tests.
    """
    _, out_dir = _run_export(tmp_path)

    nodes = {
        node.splitlines()[0].removeprefix("Node: "): node.strip()
        for node in (out_dir / "custom_nodes.mcf").read_text().split("\n\n")
        if node.startswith("Node: ")
    }

    node = nodes[IMPUTED_STATVAR_DCID]
    assert "nettingBasis: dcid:Gross" in node
    assert "financialFlowType: dcid:Disbursement" in node
    assert "crsTypeOfFlow: dcid:CRSTypeOfFlow10" in node
    assert "dac2aAidType: dcid:DAC2aAidType106" in node
    assert "crsPurpose: dcid:CRSPurposeCode14010" in node
    assert 'name: "Test statvar"' in node


def test_dc_export_writes_the_source_column_value_under_each_observation_property(
    tmp_path: Path,
) -> None:
    """Test the written observation CSV carries the right source column's value per property

    The observation input's property_to_column is the only place financingProvider,
    financingRecipient, and measurementMethod are wired to
    dc_imputed_multilateral_sectors_observations' own columns; export_bundle
    writes those columns straight to CSV under their own names, so a provider/
    recipient swap or a dropped measurementMethod entry in the mapping would
    not fail the export -- reading the CSV itself, rather than the mapping the
    export was configured with, is what would catch it.
    """
    _, out_dir = _run_export(tmp_path)

    [observations] = IMPUTED_MULTILATERAL_SECTORS_EXPORT_SPEC.observation_inputs
    mapping = observations.property_to_column
    written = pd.concat(
        pd.read_csv(csv_file) for csv_file in sorted(out_dir.glob("observations_*.csv"))
    )

    assert (written[mapping["custom:financingProvider"]] == "country/FRA").all()
    assert (written[mapping["custom:financingRecipient"]] == "country/KEN").all()

    assert "measurementMethod" in mapping
    assert (written[mapping["measurementMethod"]] == "dcid:Modelled").all()
