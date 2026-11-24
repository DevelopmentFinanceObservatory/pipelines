from collections.abc import Mapping, Sequence
from datetime import UTC, datetime

import dagster as dg
from dagster_duckdb import DuckDBResource
from upath import UPath

from dfo_pipelines.data_commons.export import ExportSpec, export_bundle
from dfo_pipelines.orchestration.automation import (
    EAGER_ON_DEPS_OR_CODE_CHANGE_WHEN_CHECKS_PASS,
)
from dfo_pipelines.orchestration.resources import UPathFileStoreResource


def export_directory_result(
    out_dir: UPath, counts: Mapping[str, int]
) -> dg.MaterializeResult:
    """Result of writing a bundle to `out_dir`, with each of its `counts`"""
    return dg.MaterializeResult(
        value=str(out_dir),
        metadata={
            "dagster/uri": dg.MetadataValue.text(str(out_dir)),
            **{name: dg.MetadataValue.int(count) for name, count in counts.items()},
        },
    )


def build_dc_export_asset(
    *,
    spec: ExportSpec,
    group: str,
    title: str,
    deps: Sequence[dg.AssetKey] = (),
) -> dg.AssetsDefinition:
    """The `<import_name>_dc_export` asset writing its spec's bundle"""
    models = [
        spec.provenance_model,
        spec.statvar_model,
        *(observations.model for observations in spec.observation_inputs),
    ]
    if spec.source_model is not None:
        models.append(spec.source_model)

    @dg.asset(
        name=f"{spec.import_name}_dc_export",
        description=f"Data Commons export for {title}",
        group_name=group,
        automation_condition=EAGER_ON_DEPS_OR_CODE_CHANGE_WHEN_CHECKS_PASS,
        code_version=spec.documentation_version,
        deps=[*(dg.AssetKey(model.split(".")) for model in models), *deps],
    )
    def dc_export(
        context: dg.AssetExecutionContext,
        duckdb: DuckDBResource,
        export_file_store: UPathFileStoreResource,
    ) -> dg.MaterializeResult:
        with export_file_store.write_once_directory(
            spec.import_name, created_at=datetime.now(UTC), log=context.log
        ) as out_dir:
            with duckdb.get_connection() as connection:
                export_metadata = export_bundle(
                    spec, connection=connection, out_dir=out_dir, log=context.log
                )
            return export_directory_result(
                out_dir,
                {
                    "observation_count": export_metadata.observation_count,
                    "statvar_count": export_metadata.statvar_count,
                    "observation_file_count": export_metadata.observation_file_count,
                },
            )

    return dc_export
