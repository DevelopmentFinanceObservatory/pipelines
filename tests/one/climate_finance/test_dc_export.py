import json
from datetime import date
from pathlib import Path
from typing import NamedTuple
from unittest.mock import patch

import dagster as dg
import pandas as pd
import pytest
from dagster_duckdb import DuckDBResource

from dfo_pipelines.data_commons.export import DOCUMENTATION_FILES, ExportSpec
from dfo_pipelines.definitions import defs
from dfo_pipelines.defs.one.climate_finance.assets import (
    OECD_CRDF_EXPORT_SPEC,
    ONE_CLIMATE_FINANCE_EXPORT_SPEC,
    UNFCCC_BTR_EXPORT_SPEC,
    oecd_crdf_dc_export,
    oecd_crdf_dc_ingestion,
    one_climate_finance_dc_export,
    one_climate_finance_dc_ingestion,
    unfccc_btr_dc_export,
    unfccc_btr_dc_ingestion,
)
from dfo_pipelines.defs.resources import KnowledgeGraphResource, UPathFileStoreResource

ASSETS_MODULE = "dfo_pipelines.defs.one.climate_finance.assets"

DISBURSEMENT_DCID = (
    "dcid:Amount_FinancialFlow_Gross_Disbursement_ClimateChangePolicyObjective_Nominal"
)
COMMITMENT_DCID = (
    "dcid:Amount_FinancialObligation_Commitment_"
    "MitigationOnlyClimateChangePolicyObjective_Nominal"
)
# One method per year, so a file loses its measurementMethod only if the mapping does.
MEASUREMENT_METHODS = {
    2021: "dcid:Estimated",
    2022: "dcid:FullValueMarkerWeightingJointMarkerAssignment",
}


class Export(NamedTuple):
    """One of the three exports, with what its bundle must carry"""

    spec: ExportSpec
    asset: dg.AssetsDefinition
    ingestion: dg.AssetsDefinition
    provenance_dcid: str
    source: str
    recipient: bool


UNFCCC_BTR = Export(
    UNFCCC_BTR_EXPORT_SPEC,
    unfccc_btr_dc_export,
    unfccc_btr_dc_ingestion,
    "dcid:provenance/UNFCCCBiennialTransparencyReports",
    "dcid:source/UNFCCC",
    recipient=False,
)
EXPORTS = [
    pytest.param(
        Export(
            ONE_CLIMATE_FINANCE_EXPORT_SPEC,
            one_climate_finance_dc_export,
            one_climate_finance_dc_ingestion,
            "dcid:provenance/ONEClimateFinance",
            "dcid:source/ONEData",
            recipient=True,
        ),
        id="one_climate_finance",
    ),
    pytest.param(
        Export(
            OECD_CRDF_EXPORT_SPEC,
            oecd_crdf_dc_export,
            oecd_crdf_dc_ingestion,
            "dcid:provenance/OECDClimateRelatedDevelopmentFinance",
            "dcid:dc/s/OrganisationForEconomicCo-operationAndDevelopmentOecd",
            recipient=True,
        ),
        id="oecd_crdf",
    ),
    pytest.param(UNFCCC_BTR, id="unfccc_btr"),
]


def _create_dc_models(duckdb: DuckDBResource, export: Export) -> None:
    """Create the models `export.spec` reads, holding a small fixture bundle"""
    spec = export.spec
    source = export.source.removeprefix("dcid:")
    observation_properties = (
        "financingProvider,financingRecipient"
        if export.recipient
        else "financingProvider"
    )
    recipient_column = (
        "'country/KEN' as financing_recipient," if export.recipient else ""
    )
    with duckdb.get_connection() as connection:
        connection.execute("create schema if not exists one_climate_finance")
        connection.execute(
            f"""
            create or replace table {spec.provenance_model} as
            select
                'Test provenance' as name,
                '{source}' as source,
                'https://example.org/data' as url,
                'https://example.org/data/release' as source_data_url,
                'https://example.org/terms' as license,
                '2026-11-09' as imported_source_release_date,
                '2021' as earliest_observation_date,
                '2022' as latest_observation_date,
                'Test description' as description,
                'Test attribution' as license_attribution
            """
        )
        connection.execute(
            f"""
            create or replace table {spec.statvar_model} as
            select * from (values
                (
                    '{DISBURSEMENT_DCID}', 'Test disbursements',
                    'dcid:FinancialFlow', 'dcid:amount', 'dcid:measuredValue',
                    'nettingBasis,financialFlowType,climatePolicyObjective',
                    'dcid:Gross', 'dcid:Disbursement', null,
                    'dcid:ClimateChangePolicyObjective', 'dcid:Nominal',
                    '{observation_properties}'
                ),
                (
                    '{COMMITMENT_DCID}', 'Test commitments',
                    'dcid:FinancialObligation', 'dcid:amount', 'dcid:measuredValue',
                    'financialObligationType,climatePolicyObjective',
                    null, null, 'dcid:Commitment',
                    'dcid:MitigationOnlyClimateChangePolicyObjective',
                    'dcid:Nominal', '{observation_properties}'
                )
            ) as statvars (
                dcid, name, population_type, measured_property, stat_type,
                constraint_properties, netting_basis, financial_flow_type,
                financial_obligation_type, climate_policy_objective,
                measurement_qualifier, observation_properties
            )
            """
        )
        connection.execute(
            f"""
            create or replace table {spec.observation_inputs[0].model} as
            select
                variable,
                year as date,
                'country/FRA' as financing_provider,
                {recipient_column}
                value::decimal(18, 2) as value,
                'dcid:CurrencyUSD' as unit,
                measurement_method
            from (values
                ('{DISBURSEMENT_DCID}', 2021, 1.5, '{MEASUREMENT_METHODS[2021]}'),
                ('{COMMITMENT_DCID}', 2021, 2.5, '{MEASUREMENT_METHODS[2021]}'),
                ('{DISBURSEMENT_DCID}', 2022, 3.5, '{MEASUREMENT_METHODS[2022]}')
            ) as observations (variable, year, value, measurement_method)
            """
        )
        if spec.source_model is not None:
            connection.execute(
                f"""
                create or replace table {spec.source_model} as
                select
                    '{source}' as dcid,
                    'Test source' as name,
                    'https://example.org/' as url
                """
            )


def _run_export(tmp_path: Path, export: Export) -> tuple[dg.MaterializeResult, Path]:
    """Materialize the export asset, returning the result and the bundle directory"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "processed"))
    _create_dc_models(duckdb, export)

    with patch("dfo_pipelines.defs.resources.date") as mock_date:
        mock_date.today.return_value = date(2026, 8, 3)
        out_dir = file_store.build_date_partitioned_path(export.spec.import_name)
        result = export.asset(
            context=dg.build_asset_context(),
            duckdb=duckdb,
            export_file_store=file_store,
        )

    assert isinstance(result, dg.MaterializeResult)
    return result, Path(str(out_dir))


@pytest.mark.parametrize("export", EXPORTS)
def test_dc_export_writes_a_bundle_to_a_date_partitioned_path(
    tmp_path: Path, export: Export
) -> None:
    """Test the export asset reports the bundle it wrote to today's partition"""
    result, out_dir = _run_export(tmp_path, export)

    assert out_dir == (
        tmp_path / "processed" / export.spec.import_name / "2026" / "08" / "03"
    )
    assert result.metadata == {
        "dagster/uri": dg.TextMetadataValue(str(out_dir)),
        "observation_count": dg.IntMetadataValue(3),
        "statvar_count": dg.IntMetadataValue(2),
        "observation_file_count": dg.IntMetadataValue(2),
    }


@pytest.mark.parametrize("export", EXPORTS)
def test_dc_export_writes_the_pipeline_documentation_onto_the_provenance_node(
    tmp_path: Path, export: Export
) -> None:
    """Test the Provenance node carries this import's transformations and changelog"""
    _, out_dir = _run_export(tmp_path, export)

    provenance = (out_dir / "provenance.mcf").read_text()
    for property_name, file_name in DOCUMENTATION_FILES.items():
        first_line = (export.spec.docs_dir / file_name).read_text().splitlines()[0]
        assert f'{property_name}: "{first_line}' in provenance


@pytest.mark.parametrize("export", EXPORTS)
def test_dc_export_writes_the_provenance_source_and_the_dollar_conversion_recipe(
    tmp_path: Path, export: Export
) -> None:
    """Test the Provenance node names its source and declares the DAC recipe"""
    _, out_dir = _run_export(tmp_path, export)

    provenance = (out_dir / "provenance.mcf").read_text()
    assert f"Node: {export.provenance_dcid}\n" in provenance
    assert f"source: {export.source}\n" in provenance
    assert "conversionRecipe: dcid:ConversionRecipeDAC\n" in provenance
    assert 'description: "Test description"\n' in provenance


@pytest.mark.parametrize("export", EXPORTS)
def test_dc_export_defines_a_source_node_only_when_the_spec_names_a_source_model(
    tmp_path: Path, export: Export
) -> None:
    """Test the bundle defines the Source its provenance names, when the import owns it"""
    _, out_dir = _run_export(tmp_path, export)

    provenance = (out_dir / "provenance.mcf").read_text()
    assert (f"Node: {export.source}\n" in provenance) == (
        export.spec.source_model is not None
    )


@pytest.mark.parametrize("export", EXPORTS)
def test_dc_export_writes_the_import_config(tmp_path: Path, export: Export) -> None:
    """Test the bundle's config registers its files, columns and observation period"""
    _, out_dir = _run_export(tmp_path, export)

    column_mappings = {
        "dcid:variableMeasured": "variable",
        "dcid:observationDate": "date",
        "dcid:value": "value",
        "dcid:unit": "unit",
        "dcid:measurementMethod": "measurement_method",
        "custom:financingProvider": "financing_provider",
    }
    if export.recipient:
        column_mappings["custom:financingRecipient"] = "financing_recipient"
    assert json.loads((out_dir / "config.json").read_text()) == {
        "importName": export.spec.import_name,
        "inputFiles": [
            {"pattern": "*.mcf", "provenance": export.provenance_dcid},
            {
                "pattern": "observations_*.csv",
                "provenance": export.provenance_dcid,
                "columnMappings": column_mappings,
                "observationProperties": {"observationPeriod": "P1Y"},
                "format": "variablePerRow",
            },
        ],
    }


@pytest.mark.parametrize("export", EXPORTS)
def test_dc_export_maps_the_statvar_dimensions_to_ontology_properties(
    tmp_path: Path, export: Export
) -> None:
    """Test the export names each StatVar dimension as its ontology property"""
    _, out_dir = _run_export(tmp_path, export)

    nodes = {
        node.splitlines()[0].removeprefix("Node: "): node
        for node in (out_dir / "custom_nodes.mcf").read_text().split("\n\n")
        if node.startswith("Node: ")
    }

    disbursements = nodes[DISBURSEMENT_DCID]
    assert 'name: "Test disbursements"' in disbursements
    assert "nettingBasis: dcid:Gross" in disbursements
    assert "financialFlowType: dcid:Disbursement" in disbursements
    assert "climatePolicyObjective: dcid:ClimateChangePolicyObjective" in disbursements
    commitments = nodes[COMMITMENT_DCID]
    assert "financialObligationType: dcid:Commitment" in commitments
    assert (
        "climatePolicyObjective: dcid:MitigationOnlyClimateChangePolicyObjective"
        in commitments
    )


@pytest.mark.parametrize("export", EXPORTS)
def test_dc_export_writes_the_measurement_method_to_every_observation_file(
    tmp_path: Path, export: Export
) -> None:
    """Test each observation file carries its measurement method under a column the
    config maps

    The mapping is the only place measurementMethod is wired to the model's column,
    and dcp-tools drops an unmapped column from the bundle without error, so the
    check reads the files the export wrote.
    """
    _, out_dir = _run_export(tmp_path, export)

    mapping = json.loads((out_dir / "config.json").read_text())["inputFiles"][1][
        "columnMappings"
    ]
    files = sorted(out_dir.glob("observations_*.csv"))
    assert len(files) == len(MEASUREMENT_METHODS)
    for year, method in MEASUREMENT_METHODS.items():
        written = pd.read_csv(out_dir / f"observations_{year}.csv")
        assert (written[mapping["dcid:measurementMethod"]] == method).all()


def test_unfccc_btr_export_names_no_recipient(tmp_path: Path) -> None:
    """Test the UNFCCC totals, which cover every recipient, map and carry none"""
    _, out_dir = _run_export(tmp_path, UNFCCC_BTR)

    config = (out_dir / "config.json").read_text()
    assert "financingRecipient" not in config
    for csv_file in out_dir.glob("observations_*.csv"):
        assert "financing_recipient" not in pd.read_csv(csv_file).columns
    nodes = (out_dir / "custom_nodes.mcf").read_text()
    assert "observationProperties: dcid:financingProvider\n" in nodes
    assert "financingRecipient" not in nodes


@pytest.mark.parametrize("export", EXPORTS)
def test_dc_export_depends_on_the_models_its_spec_reads(export: Export) -> None:
    """Test the export asset waits on the dbt models its spec names"""
    spec = export.spec
    read = {spec.provenance_model, spec.statvar_model, spec.observation_inputs[0].model}
    if spec.source_model is not None:
        read.add(spec.source_model)

    node = defs().resolve_asset_graph().get(export.asset.key)

    assert node.parent_keys == {dg.AssetKey(model.split(".")) for model in read}


@pytest.mark.parametrize("export", EXPORTS)
def test_dc_ingestion_ingests_the_bundle_the_export_wrote(
    tmp_path: Path, export: Export, knowledge_graph: KnowledgeGraphResource
) -> None:
    """Test each ingestion is handed its own export's directory under its import
    name, and that directory's config registers the import's provenance"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    _create_dc_models(duckdb, export)
    destination = f"gs://test-bucket/ingestion/input/{export.spec.import_name}"

    with patch(f"{ASSETS_MODULE}.ingest_bundle", return_value=destination) as ingest:
        result = dg.materialize(
            [export.asset, export.ingestion],
            resources={
                "duckdb": duckdb,
                "export_file_store": UPathFileStoreResource(
                    base_path=str(tmp_path / "processed")
                ),
                "knowledge_graph": knowledge_graph,
            },
        )

    assert result.success
    [exported] = result.asset_materializations_for_node(export.asset.key.path[-1])
    [ingestion] = result.asset_materializations_for_node(export.ingestion.key.path[-1])
    ingest.assert_called_once()
    args, kwargs = ingest.call_args
    assert str(args[0]) == exported.metadata["dagster/uri"].value
    assert kwargs["import_name"] == export.spec.import_name
    assert kwargs["settings"] == knowledge_graph.settings
    config = json.loads((Path(str(args[0])) / "config.json").read_text())
    assert {file["provenance"] for file in config["inputFiles"]} == {
        export.provenance_dcid
    }
    assert ingestion.metadata == {
        "dagster/uri": dg.TextMetadataValue(destination),
        "import_name": dg.TextMetadataValue(export.spec.import_name),
    }


@pytest.mark.parametrize("export", EXPORTS)
def test_dc_ingestion_is_manual_and_follows_the_export_and_the_shared_nodes(
    export: Export,
) -> None:
    """Test an ingestion carries no automation condition, since it rewrites the
    shared graph, and waits on its export, the ontology and the shared nodes"""
    node = defs().resolve_asset_graph().get(export.ingestion.key)

    assert node.automation_condition is None
    assert node.parent_keys == {
        export.asset.key,
        dg.AssetKey(["dfo_ontology_dc_ingestion"]),
        dg.AssetKey(["dfo_shared_dc_ingestion"]),
    }
