from datetime import UTC, datetime

import dagster as dg
import pyarrow.parquet as pq
from dagster_duckdb import DuckDBResource
from oda_reader import bulk_download_dac2b
from oda_reader.dac2b import DAC2B_BULK_LABEL
from oda_reader.download.download_tools import (
    DAC2B_FLOW_URL,
    get_bulk_file_url_with_version,
)

from dfo_pipelines.data_commons.export import (
    DOCS_ROOT,
    ConversionRecipe,
    ExportSpec,
    ObservationInput,
    export_bundle,
)
from dfo_pipelines.defs.dfo.ontology.assets import dfo_ontology_dc_ingestion
from dfo_pipelines.defs.dfo.shared.assets import dfo_shared_dc_ingestion
from dfo_pipelines.extraction.metadata import write_extraction_metadata
from dfo_pipelines.extraction.oecd import fetch_bulk_file_release_date
from dfo_pipelines.orchestration.automation import (
    EAGER_ON_DEPS_OR_CODE_CHANGE_WHEN_CHECKS_PASS,
    EAGER_WHEN_CHECKS_PASS,
)
from dfo_pipelines.orchestration.dc_ingestion import build_dc_ingestion_asset
from dfo_pipelines.orchestration.raw_tables import build_raw_table_assets
from dfo_pipelines.orchestration.resources import UPathFileStoreResource


def resolve_dac2b_bulk_file() -> tuple[str, str]:
    """The URL and version token of the DAC2b bulk file

    Raises:
        ValueError: If the lookup returns no version token.
    """
    # bypass_annotation_cache defeats oda_reader's 7-day on-disk HTTP cache,
    # which would otherwise let a daily observation serve a version token up to
    # a week old. Refreshing that cache entry is also what lets the
    # bulk_download_dac2b call in oecd_dac2b_raw_file see the new version, since
    # it re-resolves the URL itself without a bypass.
    url, version = get_bulk_file_url_with_version(
        flow_url=DAC2B_FLOW_URL,
        label=DAC2B_BULK_LABEL,
        bypass_annotation_cache=True,
    )
    if version is None:
        raise ValueError(f"DAC2b version check: could not get version for {url}")
    return url, version


@dg.observable_source_asset(
    group_name="oecd_dac2b",
    automation_condition=dg.AutomationCondition.on_cron("@daily"),
)
def oecd_dac2b_source() -> dg.ObserveResult:
    """Observed OECD DAC2b data version.

    The source is observed daily via calls to the OECD API.
    """
    url, version = resolve_dac2b_bulk_file()

    data_version = dg.DataVersion(";".join([url, version]))

    return dg.ObserveResult(
        metadata={
            "dagster/uri": dg.MetadataValue.text(url),
            "version": dg.MetadataValue.text(version),
        },
        data_version=data_version,
    )


@dg.asset(
    deps=[oecd_dac2b_source],
    group_name="oecd_dac2b",
    kinds={"file", "parquet"},
    automation_condition=EAGER_WHEN_CHECKS_PASS,
    check_specs=[
        dg.AssetCheckSpec(
            name="file_not_empty", asset="oecd_dac2b_raw_file", blocking=True
        )
    ],
)
def oecd_dac2b_raw_file(
    context: dg.AssetExecutionContext, raw_file_store: UPathFileStoreResource
):
    """Raw OECD DAC2b bulk download file and metadata"""
    downloaded_at = datetime.now(UTC)
    directory = raw_file_store.build_date_partitioned_path(
        "oecd_dac2b/dac2b", partition_date=downloaded_at.date()
    )
    directory.mkdir(parents=True, exist_ok=True)
    file_path = directory / "data.parquet"
    tmp_path = directory / "table2b_data.parquet"
    url, version = resolve_dac2b_bulk_file()
    release_date = fetch_bulk_file_release_date(url)
    bulk_download_dac2b(save_to_path=str(directory), use_raw_cache=False)
    tmp_path.rename(file_path)
    rows = pq.read_metadata(file_path).num_rows
    file_size = file_path.stat().st_size
    write_extraction_metadata(
        directory, downloaded_at=downloaded_at, release_dates={"source": release_date}
    )
    context.log.info(f"Wrote OECD DAC2b data to {file_path}")
    return dg.MaterializeResult(
        value=str(file_path),
        metadata={
            "dagster/uri": dg.MetadataValue.text(str(file_path)),
            "file_size": dg.MetadataValue.int(file_size),
            "version": dg.MetadataValue.text(version),
            "dagster/row_count": dg.MetadataValue.int(rows),
            "source_release_date": dg.MetadataValue.text(release_date.isoformat()),
        },
        check_results=[
            dg.AssetCheckResult(check_name="file_not_empty", passed=(rows > 0))
        ],
    )


oecd_dac2b_raw_table, oecd_dac2b_raw_metadata = build_raw_table_assets(
    group="oecd_dac2b",
    table="raw_dac2b",
    title="OECD DAC2b",
    renames={
        "RECIPIENT": "recipient_code",
        "Recipient": "recipient_name",
        "DONOR": "donor_code",
        "Donor": "donor_name",
        "PART": "part_code",
        "Part": "part_name",
    },
)


OECD_DAC2B_EXPORT_SPEC = ExportSpec(
    import_name="oecd_dac2b",
    provenance_dcid="OECDDAC2B",
    provenance_model="oecd_dac2b.dc_dac2b_provenance",
    conversion_recipe=ConversionRecipe.DAC,
    statvar_model="oecd_dac2b.dc_dac2b_statvars",
    observation_inputs=(
        ObservationInput(
            model="oecd_dac2b.dc_dac2b_observations",
            property_to_column={
                "variable": "variable",
                "date": "date",
                "value": "value",
                "unit": "unit",
                "custom:financingProvider": "financing_provider",
                "custom:financingRecipient": "financing_recipient",
            },
        ),
    ),
    statvar_column_to_property={
        "name": "name",
        "financial_flow_type": "financialFlowType",
        "netting_basis": "nettingBasis",
        "dac_type_of_finance": "dacTypeOfFinance",
        "dac2b_aid_type": "dac2bAidType",
    },
    docs_dir=DOCS_ROOT / "oecd" / "dac2b",
    constant_observation_properties={"observationPeriod": "P1Y"},
    provenance_column_to_property={
        "partial_observation_date": "partialObservationDate"
    },
)


@dg.asset(
    group_name="oecd_dac2b",
    automation_condition=EAGER_ON_DEPS_OR_CODE_CHANGE_WHEN_CHECKS_PASS,
    code_version=OECD_DAC2B_EXPORT_SPEC.documentation_version,
    deps=[
        dg.AssetKey(["oecd_dac2b", "dc_dac2b_observations"]),
        dg.AssetKey(["oecd_dac2b", "dc_dac2b_provenance"]),
        dg.AssetKey(["oecd_dac2b", "dc_dac2b_statvars"]),
    ],
)
def oecd_dac2b_dc_export(
    context: dg.AssetExecutionContext,
    duckdb: DuckDBResource,
    export_file_store: UPathFileStoreResource,
) -> dg.MaterializeResult:
    """Data Commons export for OECD DAC2b"""
    out_dir = export_file_store.build_date_partitioned_path(
        OECD_DAC2B_EXPORT_SPEC.import_name
    )
    with duckdb.get_connection() as connection:
        export_metadata = export_bundle(
            OECD_DAC2B_EXPORT_SPEC,
            connection=connection,
            out_dir=out_dir,
            log=context.log,
        )
    return dg.MaterializeResult(
        value=str(out_dir),
        metadata={
            "dagster/uri": dg.MetadataValue.text(str(out_dir)),
            "observation_count": dg.MetadataValue.int(
                export_metadata.observation_count
            ),
            "statvar_count": dg.MetadataValue.int(export_metadata.statvar_count),
            "observation_file_count": dg.MetadataValue.int(
                export_metadata.observation_file_count
            ),
        },
    )


oecd_dac2b_dc_ingestion = build_dc_ingestion_asset(
    import_name=OECD_DAC2B_EXPORT_SPEC.import_name,
    group="oecd_dac2b",
    title="OECD DAC2b",
    deps=[dfo_ontology_dc_ingestion, dfo_shared_dc_ingestion],
)
