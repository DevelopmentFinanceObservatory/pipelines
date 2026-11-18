import dagster as dg
from upath import UPath

from dfo_pipelines.data_commons.export import (
    DOCS_ROOT,
    ConversionRecipe,
    ExportSpec,
    ObservationInput,
)
from dfo_pipelines.defs.dfo.ontology.assets import dfo_ontology_dc_ingestion
from dfo_pipelines.defs.dfo.shared.assets import dfo_shared_dc_ingestion
from dfo_pipelines.extraction.climate_finance import (
    EXPORT_TABLES,
    LATEST_FILE_NAME,
    MANIFEST_FILE_NAME,
    ExportManifest,
    LatestExport,
    sha256_bytes,
)
from dfo_pipelines.extraction.metadata import FetchedRelease
from dfo_pipelines.orchestration.automation import (
    EAGER_WHEN_CHECKS_PASS,
)
from dfo_pipelines.orchestration.dc_export import build_dc_export_asset
from dfo_pipelines.orchestration.dc_ingestion import build_dc_ingestion_asset
from dfo_pipelines.orchestration.landing import commit_landing_directory, landing_result
from dfo_pipelines.orchestration.raw_tables import RawTableSpec, build_raw_table_assets
from dfo_pipelines.orchestration.resources import (
    ClimateFinanceExportResource,
    UPathFileStoreResource,
)

SCHEMA = "one_climate_finance"


def read_manifest(directory: UPath, *, sha256: str) -> ExportManifest:
    """The manifest of the export in `directory`, once it has the sha256 `sha256`.

    Raises:
        ValueError: If the manifest has another sha256, or is malformed.
    """
    data = (directory / MANIFEST_FILE_NAME).read_bytes()
    if (actual := sha256_bytes(data)) != sha256:
        raise ValueError(
            f"{directory / MANIFEST_FILE_NAME} has sha256 {actual}, expected {sha256}"
        )
    return ExportManifest.model_validate_json(data)


@dg.observable_source_asset(
    group_name="one_climate_finance",
    automation_condition=dg.AutomationCondition.on_cron("@daily"),
)
def one_climate_finance_source(
    climate_finance_export: ClimateFinanceExportResource,
) -> dg.ObserveResult:
    """Observed export of the latest climate-finance release.

    The export root's `latest.json` names the release and the sha256 of its
    manifest, which the data version is made of. The manifest records the sha256
    of each raw table, so any change to the export moves the data version.

    Raises:
        RuntimeError: If `latest.json` or the manifest it names is missing,
            malformed, or the manifest has another sha256.
    """
    root = UPath(climate_finance_export.uri)
    try:
        latest = LatestExport.model_validate_json(
            (root / LATEST_FILE_NAME).read_bytes()
        )
        directory = root / latest.release
        read_manifest(directory, sha256=latest.manifest_sha256)
    except (OSError, ValueError) as error:
        raise RuntimeError(
            f"Could not read the climate finance export at {root}: {error}"
        ) from error
    return dg.ObserveResult(
        metadata={
            "dagster/uri": dg.MetadataValue.text(str(directory)),
            "version": dg.MetadataValue.text(latest.release),
            "sha256": dg.MetadataValue.text(latest.manifest_sha256),
        },
        data_version=dg.DataVersion(f"{latest.release};{latest.manifest_sha256}"),
    )


def latest_observation(context: dg.AssetExecutionContext) -> tuple[UPath, str]:
    """The export directory the source asset last observed, and the sha256 of its
    manifest.

    Raises:
        ValueError: If the source has no observation with a data version.
    """
    key = one_climate_finance_source.key
    record = context.instance.get_latest_data_version_record(key, is_source=True)
    observation = record.asset_observation if record else None
    if observation is None or observation.data_version is None:
        raise ValueError(f"{key.to_user_string()} has no observation; observe it first")
    metadata = observation.metadata
    return UPath(str(metadata["dagster/uri"].value)), str(metadata["sha256"].value)


@dg.asset(
    deps=[one_climate_finance_source],
    group_name="one_climate_finance",
    kinds={"file", "parquet"},
    automation_condition=EAGER_WHEN_CHECKS_PASS,
    check_specs=[
        dg.AssetCheckSpec(
            name=f"{stem}_not_empty",
            asset="one_climate_finance_landing",
            blocking=True,
        )
        for stem in EXPORT_TABLES
    ],
)
# The function has no return annotation because Dagster types an asset annotated
# `-> MaterializeResult` as returning Nothing, and the directory this returns is
# the raw tables' input.
def one_climate_finance_landing(
    context: dg.AssetExecutionContext, landing_file_store: UPathFileStoreResource
):
    """Climate finance tables from the observed release, as exported, one parquet
    file per raw table.

    `spending` holds the package's climate finance received, 2010 to 2024, and
    provided, 2013 to 2024, under ONE Data's and the OECD's methods and both
    attribution rules, by `bi_multi`. `crdf_commitments` holds the OECD's CRDF
    commitments, 2010 to 2024, and `btr_totals` the parties' Biennial Transparency
    Report totals. `scripts/export_climate_finance_release.py` extracts them.

    Raises:
        ValueError: If the export's manifest or a table differs from the sha256
            the observation or the manifest recorded.
    """
    export, manifest_sha256 = latest_observation(context)
    manifest = read_manifest(export, sha256=manifest_sha256)

    def write(directory: UPath) -> FetchedRelease:
        for stem in EXPORT_TABLES:
            source = export / f"{stem}.parquet"
            data = source.read_bytes()
            if (actual := sha256_bytes(data)) != (
                expected := manifest.tables[stem].sha256
            ):
                raise ValueError(
                    f"{source} has sha256 {actual}, its manifest records {expected}"
                )
            (directory / f"{stem}.parquet").write_bytes(data)
        return FetchedRelease(
            release_dates={"source": manifest.created_at.date()},
            metadata={},
            recorded={"version": manifest.release},
        )

    directory, extraction, _ = commit_landing_directory(
        context,
        landing_file_store=landing_file_store,
        path="one_climate_finance/climate_finance",
        write=write,
    )
    context.log.info(
        f"Copied climate finance release {extraction.recorded['version']} to {directory}"
    )
    rows = {stem: manifest.tables[stem].rows for stem in EXPORT_TABLES}
    file_size = sum(
        (directory / f"{stem}.parquet").stat().st_size for stem in EXPORT_TABLES
    )
    return landing_result(
        directory,
        metadata={
            "file_size": dg.MetadataValue.int(file_size),
            "dagster/row_count": dg.MetadataValue.int(sum(rows.values())),
            **{
                f"{stem}_row_count": dg.MetadataValue.int(count)
                for stem, count in rows.items()
            },
            # The returned directory's release, an earlier run's when deduplicated
            "version": dg.MetadataValue.text(extraction.recorded["version"]),
            "source_release_date": dg.MetadataValue.text(
                extraction.release_dates["source"].isoformat()
            ),
        },
        check_results=[
            dg.AssetCheckResult(check_name=f"{stem}_not_empty", passed=count > 0)
            for stem, count in rows.items()
        ],
    )


one_climate_finance_raw_tables = build_raw_table_assets(
    group=SCHEMA,
    title="climate finance",
    tables=[
        RawTableSpec(
            asset=f"one_climate_finance_raw_{stem}",
            file_stem=stem,
            table=f"raw_climate_finance_{stem}",
        )
        for stem in EXPORT_TABLES
    ],
    metadata_table="raw_climate_finance_metadata",
)


def _export_spec(
    *,
    dataset: str,
    provenance_dcid: str,
    maps_recipient: bool,
    source_model: str | None = None,
) -> ExportSpec:
    """The export of one of the three climate finance imports.

    `dataset` names the import and the dbt models it reads, `dc_<dataset>_*`.
    `maps_recipient` says whether its observations map a `financingRecipient`.
    `source_model` names the seed of the Source node the import defines, if any.
    """
    observation_columns = {
        "variable": "variable",
        "date": "date",
        "value": "value",
        "unit": "unit",
        "measurementMethod": "measurement_method",
        "custom:financingProvider": "financing_provider",
    }
    if maps_recipient:
        observation_columns["custom:financingRecipient"] = "financing_recipient"
    return ExportSpec(
        import_name=dataset,
        provenance_dcid=provenance_dcid,
        provenance_model=f"{SCHEMA}.dc_{dataset}_provenance",
        # Every series is in current US dollars at the OECD DAC exchange rates,
        # which is what the DAC recipe converts, as for the CRS and the imputed
        # multilateral sectors. The package builds its figures from OECD DAC
        # data, and converts each party's own-currency BTR amounts at those rates.
        conversion_recipe=ConversionRecipe.DAC,
        statvar_model=f"{SCHEMA}.dc_{dataset}_statvars",
        observation_inputs=(
            ObservationInput(
                model=f"{SCHEMA}.dc_{dataset}_observations",
                property_to_column=observation_columns,
            ),
        ),
        source_model=source_model,
        statvar_column_to_property={
            "name": "name",
            "netting_basis": "nettingBasis",
            "financial_flow_type": "financialFlowType",
            "financial_obligation_type": "financialObligationType",
            "climate_policy_objective": "climatePolicyObjective",
        },
        docs_dir=DOCS_ROOT / "one" / "climate_finance" / dataset,
        constant_observation_properties={"observationPeriod": "P1Y"},
    )


ONE_CLIMATE_FINANCE_EXPORT_SPEC = _export_spec(
    dataset="one_climate_finance",
    provenance_dcid="ONEClimateFinance",
    maps_recipient=True,
)
OECD_CRDF_EXPORT_SPEC = _export_spec(
    dataset="oecd_crdf",
    provenance_dcid="OECDClimateRelatedDevelopmentFinance",
    maps_recipient=True,
)
# The parties' totals cover every recipient, a scope the StatVar carries, so the
# observations name no recipient.
UNFCCC_BTR_EXPORT_SPEC = _export_spec(
    dataset="unfccc_btr",
    provenance_dcid="UNFCCCBiennialTransparencyReports",
    maps_recipient=False,
    source_model=f"{SCHEMA}.unfccc_source",
)


one_climate_finance_dc_export = build_dc_export_asset(
    spec=ONE_CLIMATE_FINANCE_EXPORT_SPEC,
    group="one_climate_finance",
    title="ONE Data Climate Finance",
)
oecd_crdf_dc_export = build_dc_export_asset(
    spec=OECD_CRDF_EXPORT_SPEC,
    group="one_climate_finance",
    title="the OECD's climate-related development finance",
)
unfccc_btr_dc_export = build_dc_export_asset(
    spec=UNFCCC_BTR_EXPORT_SPEC,
    group="one_climate_finance",
    title="the UNFCCC Biennial Transparency Reports",
)


one_climate_finance_dc_ingestion = build_dc_ingestion_asset(
    import_name=ONE_CLIMATE_FINANCE_EXPORT_SPEC.import_name,
    group="one_climate_finance",
    title="ONE Data Climate Finance",
    deps=[dfo_ontology_dc_ingestion, dfo_shared_dc_ingestion],
)
oecd_crdf_dc_ingestion = build_dc_ingestion_asset(
    import_name=OECD_CRDF_EXPORT_SPEC.import_name,
    group="one_climate_finance",
    title="OECD climate-related development finance",
    deps=[dfo_ontology_dc_ingestion, dfo_shared_dc_ingestion],
)
unfccc_btr_dc_ingestion = build_dc_ingestion_asset(
    import_name=UNFCCC_BTR_EXPORT_SPEC.import_name,
    group="one_climate_finance",
    title="UNFCCC Biennial Transparency Reports",
    deps=[dfo_ontology_dc_ingestion, dfo_shared_dc_ingestion],
)
