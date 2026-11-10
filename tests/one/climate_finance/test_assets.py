import json
import shutil
from datetime import UTC, date, datetime
from pathlib import Path

import dagster as dg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dagster_duckdb import DuckDBResource
from upath import UPath

from dfo_pipelines.defs.one.climate_finance.assets import (
    one_climate_finance_raw_file,
    one_climate_finance_raw_metadata,
    one_climate_finance_raw_tables,
    one_climate_finance_source,
)
from dfo_pipelines.defs.resources import (
    ClimateFinanceExportResource,
    UPathFileStoreResource,
)
from dfo_pipelines.extraction.climate_finance import (
    ExportedTable,
    ExportManifest,
    LatestExport,
    sha256_bytes,
)
from dfo_pipelines.extraction.metadata import (
    read_extraction_metadata,
    write_extraction_metadata,
)
from tests.helpers import meta
from tests.one.climate_finance.conftest import EXPORT

NAME = "2026a-rc9"
STEMS = ["spending", "crdf_commitments", "btr_totals"]


@pytest.fixture
def export(tmp_path: Path) -> Path:
    """A copy of the cut's export, which a test may change"""
    root = tmp_path / "export"
    shutil.copytree(EXPORT, root)
    return root


def _republish(root: Path, *, tables: dict[str, bytes]) -> None:
    """Replace tables of the export in `root` and rewrite its manifest and
    latest.json, as a new export does"""
    directory = root / NAME
    manifest = ExportManifest.model_validate_json(
        (directory / "manifest.json").read_bytes()
    )
    entries = dict(manifest.tables)
    for stem, data in tables.items():
        (directory / f"{stem}.parquet").write_bytes(data)
        rows = pq.read_metadata(directory / f"{stem}.parquet").num_rows
        entries[stem] = ExportedTable(sha256=sha256_bytes(data), rows=rows)
    data = manifest.model_copy(update={"tables": entries}).model_dump_json().encode()
    (directory / "manifest.json").write_bytes(data)
    latest = LatestExport(release=NAME, manifest_sha256=sha256_bytes(data))
    (root / "latest.json").write_text(latest.model_dump_json())


def _empty_table(path: Path) -> bytes:
    """A parquet file with the schema of the one at `path` and no rows"""
    empty = path.parent / "empty.parquet"
    pq.write_table(pq.read_table(path).slice(0, 0), empty)
    return empty.read_bytes()


def _observe(root: Path) -> dg.ObserveResult:
    observe = one_climate_finance_source.observe_fn
    assert observe is not None
    result = observe(climate_finance_export=ClimateFinanceExportResource(uri=str(root)))
    assert isinstance(result, dg.ObserveResult)
    return result


def _record_observation(instance: dg.DagsterInstance, root: Path) -> None:
    """Observe the source and record the observation, as the daily tick does"""
    result = _observe(root)
    assert result.data_version is not None
    instance.report_runless_asset_event(
        dg.AssetObservation(
            asset_key=one_climate_finance_source.key,
            metadata=result.metadata,
            tags={"dagster/data_version": result.data_version.value},
        )
    )


def _materialize(instance: dg.DagsterInstance, tmp: Path) -> dg.MaterializeResult:
    result = one_climate_finance_raw_file(
        context=dg.build_asset_context(instance=instance),
        raw_file_store=UPathFileStoreResource(base_path=str(tmp / "raw")),
    )
    assert isinstance(result, dg.MaterializeResult)
    return result


def test_source_observes_the_export_latest_json_names(export: Path) -> None:
    """Test the source records the release and its manifest's sha256, and versions
    the data by both"""
    latest = json.loads((export / "latest.json").read_text())

    result = _observe(export)

    assert result.metadata == {
        "dagster/uri": dg.MetadataValue.text(str(UPath(export) / NAME)),
        "version": dg.MetadataValue.text(NAME),
        "sha256": dg.MetadataValue.text(latest["manifest_sha256"]),
    }
    assert result.data_version == dg.DataVersion(f"{NAME};{latest['manifest_sha256']}")


def test_source_data_version_moves_with_a_new_export(export: Path) -> None:
    """Test a new export of the same release moves the data version"""
    before = _observe(export)
    btr_totals = (export / NAME / "btr_totals.parquet").read_bytes()
    _republish(export, tables={"spending": btr_totals})

    assert _observe(export).data_version != before.data_version


@pytest.mark.parametrize(
    "file, content",
    [
        ("latest.json", None),
        ("latest.json", "not json"),
        (f"{NAME}/manifest.json", None),
        # Valid, but not the manifest latest.json names, as mid-export.
        (f"{NAME}/manifest.json", (EXPORT / NAME / "manifest.json").read_text() + " "),
    ],
    ids=[
        "no latest.json",
        "malformed latest.json",
        "no manifest",
        "manifest other than latest.json names",
    ],
)
def test_source_refuses_an_export_it_cannot_read(
    export: Path, file: str, content: str | None
) -> None:
    """Test the source fails loudly, with no fallback, on a missing, malformed or
    half-written export"""
    if content is None:
        (export / file).unlink()
    else:
        (export / file).write_text(content)
    with pytest.raises(RuntimeError, match="Could not read the climate finance export"):
        _observe(export)


def test_raw_file_copies_the_observed_export(export: Path, tmp_path: Path) -> None:
    """Test the raw file copies every table, as exported, with its checks, row
    counts and the release's name and creation date"""
    instance = dg.DagsterInstance.ephemeral()
    _record_observation(instance, export)
    manifest = ExportManifest.model_validate_json(
        (export / NAME / "manifest.json").read_bytes()
    )

    result = _materialize(instance, tmp_path)

    directory = Path(str(result.value))
    for stem in STEMS:
        copied = (directory / f"{stem}.parquet").read_bytes()
        assert copied == (export / NAME / f"{stem}.parquet").read_bytes()
        row_count = meta(result, f"{stem}_row_count", dg.IntMetadataValue).value
        assert row_count == manifest.tables[stem].rows
    assert {check.check_name: check.passed for check in result.check_results} == {
        f"{stem}_not_empty": True for stem in STEMS
    }
    assert meta(result, "dagster/row_count", dg.IntMetadataValue).value == sum(
        table.rows for table in manifest.tables.values()
    )
    assert meta(result, "version", dg.TextMetadataValue).value == NAME
    assert (
        meta(result, "source_release_date", dg.TextMetadataValue).value == "2026-11-10"
    )
    recorded = read_extraction_metadata(UPath(directory), release_names=("source",))
    assert recorded.release_dates["source"].isoformat() == "2026-11-10"


@pytest.mark.parametrize("stem", STEMS)
def test_raw_file_check_fails_on_a_table_with_no_rows(
    export: Path, tmp_path: Path, stem: str
) -> None:
    """Test each table's blocking check fails when its export has no rows"""
    empty = _empty_table(export / NAME / f"{stem}.parquet")
    _republish(export, tables={stem: empty})
    instance = dg.DagsterInstance.ephemeral()
    _record_observation(instance, export)

    result = _materialize(instance, tmp_path)

    failed = {check.check_name for check in result.check_results if not check.passed}
    assert failed == {f"{stem}_not_empty"}


def test_raw_file_refuses_an_export_replaced_after_the_observation(
    export: Path, tmp_path: Path
) -> None:
    """Test the raw file refuses a manifest other than the one observed"""
    instance = dg.DagsterInstance.ephemeral()
    _record_observation(instance, export)
    empty = _empty_table(export / NAME / "btr_totals.parquet")
    _republish(export, tables={"btr_totals": empty})

    with pytest.raises(ValueError, match=r"manifest\.json has sha256"):
        _materialize(instance, tmp_path)


def test_raw_file_refuses_a_table_its_manifest_does_not_record(
    export: Path, tmp_path: Path
) -> None:
    """Test the raw file refuses a table whose sha256 differs from the manifest's"""
    instance = dg.DagsterInstance.ephemeral()
    _record_observation(instance, export)
    shutil.copy(
        export / NAME / "btr_totals.parquet", export / NAME / "spending.parquet"
    )

    with pytest.raises(ValueError, match=r"spending\.parquet has sha256"):
        _materialize(instance, tmp_path)


def test_raw_file_refuses_to_run_before_an_observation(tmp_path: Path) -> None:
    """Test the raw file fails when the source was never observed"""
    with pytest.raises(ValueError, match="has no observation"):
        _materialize(dg.DagsterInstance.ephemeral(), tmp_path)


# Each raw file stem and the table it loads into.
RAW_TABLES = {
    "spending": "one_climate_finance.raw_climate_finance_spending",
    "crdf_commitments": "one_climate_finance.raw_climate_finance_crdf_commitments",
    "btr_totals": "one_climate_finance.raw_climate_finance_btr_totals",
}


def _raw_directory(tmp: Path, **tables: pa.Table) -> Path:
    """A raw file directory holding the test export's tables, with `tables` in
    place of those stems"""
    directory = tmp / "raw_file"
    directory.mkdir(parents=True)
    for stem in RAW_TABLES:
        exported = EXPORT / NAME / f"{stem}.parquet"
        pq.write_table(
            tables.get(stem, pq.read_table(exported)), directory / f"{stem}.parquet"
        )
    return directory


def _load_tables(duckdb: DuckDBResource, directory: Path) -> dg.ExecuteInProcessResult:
    """Run the raw tables in a run whose raw file returned `directory`"""

    @dg.asset(name="one_climate_finance_raw_file")
    def raw_file() -> str:
        return str(directory)

    return dg.materialize(
        [raw_file, one_climate_finance_raw_tables],
        resources={"duckdb": duckdb},
        raise_on_error=False,
    )


def test_raw_tables_replace_each_table_with_its_raw_file(tmp_path: Path) -> None:
    """Test each raw file loads, as written, into its own table, replacing what an
    earlier load left there"""
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))
    exported = {
        stem: pq.read_table(EXPORT / NAME / f"{stem}.parquet") for stem in RAW_TABLES
    }
    earlier = {stem: table.slice(0, 2) for stem, table in exported.items()}
    _load_tables(duckdb, _raw_directory(tmp_path / "earlier", **earlier))

    result = _load_tables(duckdb, _raw_directory(tmp_path))

    assert result.success
    with duckdb.get_connection() as connection:
        for stem, table in RAW_TABLES.items():
            loaded = connection.sql(f"select * from {table}").to_arrow_table()
            assert loaded.to_pylist() == exported[stem].to_pylist()
    assert {
        materialization.asset_key: materialization.metadata
        for materialization in result.asset_materializations_for_node(
            "one_climate_finance_raw_tables"
        )
    } == {
        dg.AssetKey(f"one_climate_finance_raw_{stem}"): {
            "dagster/table_name": dg.TextMetadataValue(table),
            "dagster/row_count": dg.IntMetadataValue(exported[stem].num_rows),
        }
        for stem, table in RAW_TABLES.items()
    }
    assert all(check.passed for check in result.get_asset_check_evaluations())


@pytest.mark.parametrize("stem", list(RAW_TABLES))
def test_raw_tables_check_fails_on_an_empty_file(tmp_path: Path, stem: str) -> None:
    """Test the blocking check of the table loaded from an empty file fails, and
    only that one"""
    empty = pq.read_table(EXPORT / NAME / f"{stem}.parquet").slice(0, 0)
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    result = _load_tables(duckdb, _raw_directory(tmp_path, **{stem: empty}))

    failed = {
        check.asset_key
        for check in result.get_asset_check_evaluations()
        if check.check_name == "table_not_empty" and not check.passed
    }
    assert failed == {dg.AssetKey(f"one_climate_finance_raw_{stem}")}
    assert not result.success


def test_raw_metadata_loads_the_release_date(tmp_path: Path) -> None:
    """Test the metadata table holds the download time and the release's date"""
    directory = UPath(_raw_directory(tmp_path))
    downloaded_at = datetime(2026, 11, 10, 12, tzinfo=UTC)
    write_extraction_metadata(
        directory,
        downloaded_at=downloaded_at,
        release_dates={"source": date(2026, 11, 10)},
    )
    duckdb = DuckDBResource(database=str(tmp_path / "database.duckdb"))

    result = one_climate_finance_raw_metadata(
        duckdb=duckdb, one_climate_finance_raw_file=str(directory)
    )

    assert isinstance(result, dg.MaterializeResult)
    table = "one_climate_finance.raw_climate_finance_metadata"
    with duckdb.get_connection() as connection:
        rows = connection.sql(
            f"select downloaded_at, source_release_date from {table}"
        ).fetchall()
    assert rows == [(downloaded_at, date(2026, 11, 10))]
    assert result.metadata == {
        "dagster/table_name": dg.TextMetadataValue(table),
        "dagster/row_count": dg.IntMetadataValue(1),
    }
