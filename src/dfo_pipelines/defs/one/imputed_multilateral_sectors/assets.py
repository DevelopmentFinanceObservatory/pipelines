import re
from collections.abc import Mapping
from datetime import date
from importlib.metadata import version as installed_version

import dagster as dg
import pandas as pd
from oda_data import sector_imputations
from oda_data.clean_data.schema import ODASchema
from oda_data.indicators.research.imputation_quality import imputation_quality_report
from oda_reader.download.download_tools import CRS_FLOW_URL, MULTI_FLOW_URL
from upath import UPath

from dfo_pipelines.data_commons.export import (
    DOCS_ROOT,
    ConversionRecipe,
    ExportSpec,
    ObservationInput,
)
from dfo_pipelines.defs.dfo.ontology.assets import dfo_ontology_dc_ingestion
from dfo_pipelines.defs.dfo.shared.assets import dfo_shared_dc_ingestion
from dfo_pipelines.extraction.metadata import FetchedRelease
from dfo_pipelines.extraction.oecd import resolve_bulk_file
from dfo_pipelines.orchestration.dc_export import build_dc_export_asset
from dfo_pipelines.orchestration.dc_ingestion import build_dc_ingestion_asset
from dfo_pipelines.orchestration.landing import Observation, build_landing_asset
from dfo_pipelines.orchestration.raw_tables import RawTableSpec, build_raw_table_assets

CRS_BULK_LABEL = "CRS-Parquet"
MULTISYSTEM_BULK_LABEL = "Entire dataset (dotStat format)"

# Gross disbursements, current USD prices: base_year stays unset so
# imputed_multilateral_by_purpose reports prices="current" instead of
# deflating; currency stays "USD" so it reports currency="USD".
CURRENCY = "USD"

# Several multilateral channels (IBRD, EBRD, IFC, IDB Invest) report only
# OOF, not ODA, to the CRS; an ODA-only share basis would leave them with no
# CRS spending to impute from. ("ODA", "OOF") is oda_data's own documented
# default share basis for this reason, kept explicit here rather than relied
# on as a default.
FLOW_TYPES = ("ODA", "OOF")

# The `result.attrs["provenance"]` key each source's release is recorded
# under, keyed by the name `resolve_bulk_versions()` and the extraction
# metadata use for that source.
_PROVENANCE_KEYS = {"crs": "crs_release", "multisystem": "multisystem_release"}
_SOURCE_LABELS = {"crs": "CRS", "multisystem": "Members' Total Use"}

# A window's money concentrated at or above this share in a single purpose
# code is flagged by imputation_quality_report's single_purpose_concentration.
_CONCENTRATION_THRESHOLD = 0.5


def resolve_bulk_versions() -> tuple[tuple[str, str], tuple[str, str]]:
    """Resolve (url, version) for the CRS and Members' Total Use bulk files.

    `imputed_multilateral_by_purpose` reads both releases through `oda_data`,
    which exposes only its own package version, not an upstream one. Both
    bulk files carry the same OECD `-vYYYYMMDD` republish annotation
    `oecd_dac_crs_source` already reads for CRS, so this reaches around
    `oda_data` into the `oda_reader` dependency it is built on to get an
    honest fingerprint for both.

    Returns:
        tuple: ((crs_url, crs_version), (multisystem_url, multisystem_version)).

    Raises:
        ValueError: If either lookup returns no version, or returns a token
            that does not carry a release date (see `parse_release_date`).
    """
    crs_url, crs_version = resolve_bulk_file(
        flow_url=CRS_FLOW_URL, label=CRS_BULK_LABEL
    )
    parse_release_date(crs_version, label="CRS")

    multi_url, multi_version = resolve_bulk_file(
        flow_url=MULTI_FLOW_URL, label=MULTISYSTEM_BULK_LABEL
    )
    parse_release_date(multi_version, label="Members' Total Use")
    return (crs_url, crs_version), (multi_url, multi_version)


def parse_release_date(version: str, *, label: str) -> date:
    """Parse the date out of a `vYYYYMMDD` or `vYYYYMMDD-N` version token.

    The optional `-N` suffix is `oda_reader`'s within-day sequence number for
    a same-day republish; it carries no date information, so it is dropped.

    Args:
        version: Version token as `oda_reader` resolved it.
        label: What the token is for, used only in the error message.

    Returns:
        date: The parsed date.

    Raises:
        ValueError: If the token does not carry a date.
    """
    matched = re.fullmatch(
        r"v(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})(?:-\d+)?", version
    )
    if matched is None:
        raise ValueError(
            f"{label} version token carries no release date, expected "
            f"vYYYYMMDD or vYYYYMMDD-N: {version}"
        )
    return date(int(matched["year"]), int(matched["month"]), int(matched["day"]))


def verify_release_provenance(
    *,
    provenance: Mapping[str, object] | None,
    observed_before: Mapping[str, str],
    observed_after: Mapping[str, str],
) -> None:
    """Refuse a result whose provenance disagrees with the observed releases.

    `imputed_multilateral_by_purpose` resolves its own release token through
    `oda_reader`'s 7-day HTTP annotation cache, so it can lag the source's
    bypassing lookup, or read a release published since the observation. This
    checks the token `result.attrs["provenance"]` reports against the token
    observed, then checks that same token again against one looked up
    immediately after the compute: the bulk file's URL stays stable across a
    republish, so a mid-compute republish would otherwise be invisible to the
    first check alone.

    Args:
        provenance: `result.attrs.get("provenance")` from
            `imputed_multilateral_by_purpose`, read before any further
            transformation of the result frame.
        observed_before: `{"crs": ..., "multisystem": ...}` version tokens
            the source observed.
        observed_after: The same tokens, from `resolve_bulk_versions()`
            called immediately after the compute.

    Raises:
        ValueError: If `provenance` is missing, a source's release is `None`
            or `"supplied"`, a source's reported `release_id` differs from
            the token observed, or a source's token moved between the
            observation and the lookup after the compute.
    """
    if provenance is None:
        raise ValueError(
            "imputed_multilateral_by_purpose returned no result.attrs['provenance']; "
            "cannot verify which CRS and Members' Total Use releases it read"
        )
    for name, provenance_key in _PROVENANCE_KEYS.items():
        label = _SOURCE_LABELS[name]
        release = provenance.get(provenance_key)
        if release is None or release == "supplied":
            raise ValueError(
                f"{label} release provenance is {release!r}, expected a "
                "release record; cannot verify which release was read"
            )
        release_id = release.get("release_id") if isinstance(release, Mapping) else None
        before = observed_before[name]
        if release_id != before:
            raise ValueError(
                f"{label} release provenance reports release_id {release_id!r}, "
                f"but {before!r} was observed: observe it again"
            )
        after = observed_after[name]
        if after != before:
            raise ValueError(
                f"{label} release moved from {before!r} to {after!r} after it "
                "was observed; refusing a result that may span two releases: "
                "observe it again"
            )


def _allocation_status_shares(result: pd.DataFrame) -> dict[str, dg.MetadataValue]:
    """One float metadata value per `allocation_status`'s share of total value."""
    totals = result.groupby("allocation_status", observed=True)[ODASchema.VALUE].sum()
    grand_total = totals.sum()
    shares = totals / grand_total if grand_total else totals * 0.0
    return {
        f"value_share_status_{status}": dg.MetadataValue.float(float(share))
        for status, share in shares.items()
    }


def _unallocated_value_share_by_year(result: pd.DataFrame) -> dg.MetadataValue:
    """Unallocated value's share of that year's total value, one row per year."""
    year_totals = result.groupby(ODASchema.YEAR)[ODASchema.VALUE].sum()
    unallocated_totals = (
        result.loc[result["allocation_status"] == "unallocated"]
        .groupby(ODASchema.YEAR)[ODASchema.VALUE]
        .sum()
    )
    share_by_year = (unallocated_totals / year_totals).reindex(
        year_totals.index, fill_value=0.0
    )
    return dg.MetadataValue.table(
        records=[
            dg.TableRecord(
                data={"year": int(year), "unallocated_value_share": float(share)}
            )
            for year, share in share_by_year.items()
        ],
        schema=dg.TableSchema(
            columns=[
                dg.TableColumn(name="year", type="int"),
                dg.TableColumn(name="unallocated_value_share", type="float"),
            ]
        ),
    )


def build_quality_metadata(
    result: pd.DataFrame, *, provenance: Mapping[str, object]
) -> dict[str, dg.MetadataValue]:
    """Build materialization metadata from a result frame and its provenance.

    Sourced from `imputation_quality_report`, a per-year computation over
    `result`, and `provenance` itself. Nothing here blocks the run:
    `imputation_quality_report`'s `duplicates` and `crosswalk_misses` are
    already enforced upstream (money conservation, `on_unmapped="raise"`),
    and its `negatives`/`single_purpose_concentration` are legitimate output
    shapes oda_data keeps deliberately, so they are reported as metadata.

    Args:
        result: `imputed_multilateral_by_purpose` output.
        provenance: `result.attrs["provenance"]`, already verified by
            `verify_release_provenance`.

    Returns:
        dict: Metadata values keyed by name.
    """
    report = imputation_quality_report(
        result, concentration_threshold=_CONCENTRATION_THRESHOLD
    )
    negatives = report["negatives"]
    single_purpose = report["single_purpose_concentration"]
    assert isinstance(negatives, pd.DataFrame)
    assert isinstance(single_purpose, pd.DataFrame)

    crs_release = provenance["crs_release"]
    multisystem_release = provenance["multisystem_release"]
    assert isinstance(crs_release, dict)
    assert isinstance(multisystem_release, dict)

    return {
        **_allocation_status_shares(result),
        "unallocated_value_share_by_year": _unallocated_value_share_by_year(result),
        "negative_row_count": dg.MetadataValue.int(len(negatives)),
        "negative_value_sum": dg.MetadataValue.float(
            float(negatives[ODASchema.VALUE].sum())
        ),
        "single_purpose_concentration_flagged_count": dg.MetadataValue.int(
            int(single_purpose["flagged"].sum())
        ),
        "crosswalk_vintage": dg.MetadataValue.text(
            str(provenance["crosswalk_vintage"])
        ),
        "package_version": dg.MetadataValue.text(str(provenance["package_version"])),
        "crs_release_id": dg.MetadataValue.text(str(crs_release.get("release_id"))),
        "multisystem_release_id": dg.MetadataValue.text(
            str(multisystem_release.get("release_id"))
        ),
    }


@dg.observable_source_asset(
    group_name="one_imputed_multilateral_sectors",
    automation_condition=dg.AutomationCondition.on_cron("@daily"),
)
def one_imputed_multilateral_sectors_source() -> dg.ObserveResult:
    """Observed CRS and Members' Total Use data versions the imputation depends on.

    `imputed_multilateral_by_purpose` reads CRS multilateral spending for the
    sector shares and Members' Total Use for the core contributions, so the
    pipeline needs to rerun whenever either source publishes or `oda_data`
    itself is upgraded.
    """
    (crs_url, crs_version), (multi_url, multi_version) = resolve_bulk_versions()
    oda_data_version = installed_version("oda-data")

    data_version = dg.DataVersion(
        ";".join([crs_url, crs_version, multi_url, multi_version, oda_data_version])
    )

    return dg.ObserveResult(
        metadata={
            "crs_url": dg.MetadataValue.text(crs_url),
            "crs_version": dg.MetadataValue.text(crs_version),
            "multisystem_url": dg.MetadataValue.text(multi_url),
            "multisystem_version": dg.MetadataValue.text(multi_version),
            "oda_data_version": dg.MetadataValue.text(oda_data_version),
        },
        data_version=data_version,
    )


def fetch_imputed_multilateral_sectors(
    directory: UPath, observation: Observation
) -> FetchedRelease:
    """Compute the imputation from the releases the observation saw and write it

    Computes `imputed_multilateral_by_purpose` for every provider and year
    `oda-data` has, gross disbursements at current USD prices, sector shares
    based on ODA and OOF CRS flows. This dataset aggregates over
    `channel_code`, without `financingChannel`. Every core dollar is
    allocated: `allocation_status` records whether a row's purpose shares
    came from the channel's own current-window CRS spending (`imputed`), a
    stale window (`stale_share`), a reviewed proxy channel (`proxy`), or
    could not be allocated at all (`unallocated`, with `recipient_code` and
    `purpose_code` null).

    The CRS and Members' Total Use releases the compute reads are verified
    against the releases the observation saw and those looked up after the
    compute; a disagreement, a republish since the observation, or an
    `oda-data` other than the observed one refuses the run rather than
    writing a result that may not match what was observed.
    """
    oda_data_version = installed_version("oda-data")
    if oda_data_version != observation.metadata["oda_data_version"]:
        raise ValueError(
            f"oda-data {oda_data_version} is installed, but "
            f"{observation.metadata['oda_data_version']} was observed: observe "
            "one_imputed_multilateral_sectors_source again"
        )
    observed_before = {
        "crs": observation.metadata["crs_version"],
        "multisystem": observation.metadata["multisystem_version"],
    }

    data = sector_imputations.imputed_multilateral_by_purpose(
        currency=CURRENCY, base_year=None, flow_types=FLOW_TYPES, refresh=True
    )
    provenance = data.attrs.get("provenance")

    (_, crs_after), (_, multi_after) = resolve_bulk_versions()
    observed_after = {"crs": crs_after, "multisystem": multi_after}

    verify_release_provenance(
        provenance=provenance,
        observed_before=observed_before,
        observed_after=observed_after,
    )
    release_dates = {
        "crs": parse_release_date(observed_before["crs"], label="CRS"),
        "multisystem": parse_release_date(
            observed_before["multisystem"], label="Members' Total Use"
        ),
    }

    currencies = set(data[ODASchema.CURRENCY].unique())
    prices = set(data[ODASchema.PRICES].unique())
    if not currencies <= {CURRENCY}:
        raise ValueError(
            "imputed_multilateral_by_purpose returned unexpected currencies "
            f"{currencies}, expected {{{CURRENCY!r}}}"
        )
    if not prices <= {"current"}:
        raise ValueError(
            "imputed_multilateral_by_purpose returned unexpected prices "
            f"{prices}, expected {{'current'}} for base_year=None"
        )

    data.to_parquet(directory / "imputed_multilateral_sectors.parquet", index=False)

    return FetchedRelease(
        release_dates=release_dates,
        metadata={
            "crs_version": observed_before["crs"],
            "multisystem_version": observed_before["multisystem"],
            **build_quality_metadata(data, provenance=provenance),
        },
    )


one_imputed_multilateral_sectors_landing = build_landing_asset(
    group="one_imputed_multilateral_sectors",
    source=one_imputed_multilateral_sectors_source,
    dataset="imputed_multilateral_sectors",
    title="imputed multilateral spending by purpose",
    files=["imputed_multilateral_sectors.parquet"],
    fetch=fetch_imputed_multilateral_sectors,
)


one_imputed_multilateral_sectors_raw_tables = build_raw_table_assets(
    group="one_imputed_multilateral_sectors",
    title="imputed multilateral spending by purpose",
    tables=[
        RawTableSpec(
            asset="one_imputed_multilateral_sectors_raw_table",
            file_stem="imputed_multilateral_sectors",
            table="raw_imputed_multilateral_sectors",
        )
    ],
    metadata_table="raw_imputed_multilateral_sectors_metadata",
    release_names=("crs", "multisystem"),
)


IMPUTED_MULTILATERAL_SECTORS_EXPORT_SPEC = ExportSpec(
    import_name="one_imputed_multilateral_sectors",
    provenance_dcid="ONEImputedMultilateralSectors",
    provenance_model=(
        "one_imputed_multilateral_sectors.dc_imputed_multilateral_sectors_provenance"
    ),
    conversion_recipe=ConversionRecipe.DAC,
    statvar_model=(
        "one_imputed_multilateral_sectors.dc_imputed_multilateral_sectors_statvars"
    ),
    observation_inputs=(
        ObservationInput(
            model=(
                "one_imputed_multilateral_sectors."
                "dc_imputed_multilateral_sectors_observations"
            ),
            property_to_column={
                "variable": "variable",
                "date": "date",
                "value": "value",
                "unit": "unit",
                # measurementMethod is one of add_input_file's recognised
                # column_mappings keys. Left out, the model's measurement_method
                # column would be written into the CSV but never registered in
                # config.json. dcp-tools rejects a key it does not recognise with
                # extra_forbidden, so a misspelt key fails the export.
                "measurementMethod": "measurement_method",
                "custom:financingProvider": "financing_provider",
                "custom:financingRecipient": "financing_recipient",
            },
        ),
    ),
    statvar_column_to_property={
        "name": "name",
        "netting_basis": "nettingBasis",
        "financial_flow_type": "financialFlowType",
        "type_of_flow": "crsTypeOfFlow",
        "aid_type": "dac2aAidType",
        "purpose_code": "crsPurpose",
    },
    docs_dir=DOCS_ROOT / "one" / "imputed_multilateral_sectors",
    constant_observation_properties={"observationPeriod": "P1Y"},
    provenance_column_to_property={
        "partial_observation_date": "partialObservationDate"
    },
)


one_imputed_multilateral_sectors_dc_export = build_dc_export_asset(
    spec=IMPUTED_MULTILATERAL_SECTORS_EXPORT_SPEC,
    group="one_imputed_multilateral_sectors",
    title="imputed multilateral sectors",
)


one_imputed_multilateral_sectors_dc_ingestion = build_dc_ingestion_asset(
    import_name=IMPUTED_MULTILATERAL_SECTORS_EXPORT_SPEC.import_name,
    group="one_imputed_multilateral_sectors",
    title="Imputed multilateral sectors",
    deps=[dfo_ontology_dc_ingestion, dfo_shared_dc_ingestion],
)
