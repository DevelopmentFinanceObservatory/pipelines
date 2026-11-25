from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import dagster as dg
import pytest
from dagster_duckdb import DuckDBResource

from dfo_pipelines.data_commons.export import (
    DOCUMENTATION_FILES,
    ExportMetadata,
    ExportSpec,
    ObservationInput,
)
from dfo_pipelines.orchestration.automation import (
    EAGER_ON_DEPS_OR_CODE_CHANGE_WHEN_CHECKS_PASS,
)
from dfo_pipelines.orchestration.dc_export import build_dc_export_asset
from dfo_pipelines.resources.file_store import UPathFileStoreResource
from dfo_pipelines.resources.warehouse import DuckDBWarehouseResource

EXPORT_KEY = dg.AssetKey("example_dc_export")
RATE_DIAGNOSTIC = dg.AssetKey(["example", "int_example_rates"])


def _spec(docs_dir: Path, *, source_model: str | None) -> ExportSpec:
    """An export reading two observation models, with docs in `docs_dir`"""
    for file_name in DOCUMENTATION_FILES.values():
        (docs_dir / file_name).write_text(f"{file_name}\n")
    return ExportSpec(
        import_name="example",
        provenance_dcid="Example",
        provenance_model="example.dc_example_provenance",
        conversion_recipe=None,
        statvar_model="example.dc_example_statvars",
        observation_inputs=(
            ObservationInput(
                model="example.dc_example_observations",
                property_to_column={"variable": "variable"},
            ),
            ObservationInput(
                model="example.dc_example_flows",
                property_to_column={"variable": "variable"},
                file_stem="flows",
            ),
        ),
        source_model=source_model,
        statvar_column_to_property={"name": "name"},
        docs_dir=docs_dir,
    )


def _resources(tmp_path: Path) -> dict[str, dg.ConfigurableResource]:
    """A database and an export store under `tmp_path`"""
    return {
        "warehouse": DuckDBWarehouseResource(
            duckdb=DuckDBResource(database=str(tmp_path / "database.duckdb"))
        ),
        "export_file_store": UPathFileStoreResource(
            base_path=str(tmp_path / "processed")
        ),
    }


def _dep_keys(asset: dg.AssetsDefinition) -> set[dg.AssetKey]:
    return {dep.asset_key for dep in asset.specs_by_key[asset.key].deps}


def test_build_dc_export_asset_writes_the_bundle_to_a_directory_for_the_run(
    tmp_path: Path,
) -> None:
    """Test the export hands its spec and a directory named by the run's UTC instant
    and ID to `export_bundle`, returns the directory, and reports what the bundle holds"""
    spec = _spec(tmp_path, source_model=None)
    asset = build_dc_export_asset(spec=spec, group="example", title="Example")

    with (
        patch("dfo_pipelines.orchestration.dc_export.datetime") as mock_datetime,
        patch(
            "dfo_pipelines.orchestration.dc_export.export_bundle",
            return_value=ExportMetadata(
                observation_count=7, statvar_count=3, observation_file_count=2
            ),
        ) as export_bundle,
    ):
        mock_datetime.now.return_value = datetime(2026, 10, 1, 9, 30, 15, tzinfo=UTC)
        result = dg.materialize([asset], resources=_resources(tmp_path))

    assert result.success
    out_dir = tmp_path / "processed" / "example" / f"20261001T093015Z_{result.run_id}"
    export_bundle.assert_called_once()
    args, kwargs = export_bundle.call_args
    assert args == (spec,)
    assert kwargs["out_dir"] == out_dir
    assert result.output_for_node("example_dc_export") == str(out_dir)
    [materialization] = result.asset_materializations_for_node("example_dc_export")
    reported = {
        "dagster/uri": dg.TextMetadataValue(str(out_dir)),
        "observation_count": dg.IntMetadataValue(7),
        "statvar_count": dg.IntMetadataValue(3),
        "observation_file_count": dg.IntMetadataValue(2),
    }
    # The IO manager adds the `path` it stored the value at.
    assert materialization.metadata.items() >= reported.items()


def test_build_dc_export_asset_keeps_the_bundle_an_earlier_run_wrote(
    tmp_path: Path,
) -> None:
    """Test a second materialization writes a directory of its own and leaves the
    first run's bundle as it was"""
    spec = _spec(tmp_path, source_model=None)
    asset = build_dc_export_asset(spec=spec, group="example", title="Example")
    resources = _resources(tmp_path)
    runs = iter(["first", "second"])

    def write_bundle(spec: ExportSpec, *, out_dir: Path, **_: object) -> ExportMetadata:
        (out_dir / "config.json").write_text(next(runs))
        return ExportMetadata(
            observation_count=0, statvar_count=0, observation_file_count=0
        )

    with (
        patch("dfo_pipelines.orchestration.dc_export.datetime") as mock_datetime,
        patch(
            "dfo_pipelines.orchestration.dc_export.export_bundle",
            side_effect=write_bundle,
        ),
    ):
        mock_datetime.now.side_effect = [
            datetime(2026, 10, 1, 9, 30, 15, tzinfo=UTC),
            datetime(2026, 10, 1, 9, 30, 16, tzinfo=UTC),
        ]
        first = dg.materialize([asset], resources=resources)
        second = dg.materialize([asset], resources=resources)

    first_dir = Path(first.output_for_node("example_dc_export"))
    second_dir = Path(second.output_for_node("example_dc_export"))
    assert first_dir != second_dir
    assert (first_dir / "config.json").read_text() == "first"
    assert (second_dir / "config.json").read_text() == "second"


def test_build_dc_export_asset_removes_the_bundle_of_a_failed_run(
    tmp_path: Path,
) -> None:
    """Test an export that fails after writing part of its bundle fails the run and
    leaves no directory to ingest"""
    spec = _spec(tmp_path, source_model=None)
    asset = build_dc_export_asset(spec=spec, group="example", title="Example")

    def write_part_of_the_bundle(
        spec: ExportSpec, *, out_dir: Path, **_: object
    ) -> ExportMetadata:
        (out_dir / "config.json").write_text("{}")
        raise RuntimeError("export failed")

    with (
        patch(
            "dfo_pipelines.orchestration.dc_export.export_bundle",
            side_effect=write_part_of_the_bundle,
        ),
        pytest.raises(RuntimeError, match="export failed"),
    ):
        dg.materialize([asset], resources=_resources(tmp_path))

    assert list((tmp_path / "processed" / "example").iterdir()) == []


def test_build_dc_export_asset_is_named_and_versioned_by_its_spec(
    tmp_path: Path,
) -> None:
    """Test the export takes its name from the import, its description from the
    title, and reruns eagerly when its documentation changes"""
    spec = _spec(tmp_path, source_model=None)

    asset = build_dc_export_asset(spec=spec, group="example", title="Example")

    assert asset.key == EXPORT_KEY
    assert asset.group_names_by_key[EXPORT_KEY] == "example"
    assert asset.descriptions_by_key[EXPORT_KEY] == "Data Commons export for Example"
    assert asset.code_versions_by_key[EXPORT_KEY] == spec.documentation_version
    assert (
        asset.automation_conditions_by_key[EXPORT_KEY]
        == EAGER_ON_DEPS_OR_CODE_CHANGE_WHEN_CHECKS_PASS
    )


def test_build_dc_export_asset_depends_on_every_model_its_spec_reads(
    tmp_path: Path,
) -> None:
    """Test the export waits on the provenance, StatVar, every observation and the
    Source models, then on the extra deps it is given"""
    spec = _spec(tmp_path, source_model="example.example_source")

    asset = build_dc_export_asset(
        spec=spec, group="example", title="Example", deps=[RATE_DIAGNOSTIC]
    )

    assert _dep_keys(asset) == {
        dg.AssetKey(["example", "dc_example_provenance"]),
        dg.AssetKey(["example", "dc_example_statvars"]),
        dg.AssetKey(["example", "dc_example_observations"]),
        dg.AssetKey(["example", "dc_example_flows"]),
        dg.AssetKey(["example", "example_source"]),
        RATE_DIAGNOSTIC,
    }


def test_build_dc_export_asset_without_a_source_model_depends_on_no_source(
    tmp_path: Path,
) -> None:
    """Test an export whose provenance's source already exists waits only on the
    models its spec names"""
    spec = _spec(tmp_path, source_model=None)

    asset = build_dc_export_asset(spec=spec, group="example", title="Example")

    assert _dep_keys(asset) == {
        dg.AssetKey(["example", "dc_example_provenance"]),
        dg.AssetKey(["example", "dc_example_statvars"]),
        dg.AssetKey(["example", "dc_example_observations"]),
        dg.AssetKey(["example", "dc_example_flows"]),
    }
