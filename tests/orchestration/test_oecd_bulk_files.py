import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import Mock, patch

import dagster as dg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dfo_pipelines.extraction.metadata import METADATA_FILE_NAME
from dfo_pipelines.orchestration.oecd_bulk_files import build_oecd_bulk_file_assets
from dfo_pipelines.orchestration.resources import UPathFileStoreResource
from tests.helpers import AutomationHarness

FLOW_URL = "https://sdmx.oecd.org/public/rest/dataflow/OECD.DCD.FSD/DSD_EXAMPLE"
LABEL = "Example full dataset"
DOWNLOADED_NAME = "example_data.parquet"

# Stand-in for the resolved bulk-download URL. The lookup is mocked, so the URL
# is never fetched, and it is joined into the data version the tests pin.
BULK_FILE_URL = "https://webfs-dcd.oecd.org/data/example/Example.parquet"
VERSION = "v20260801"

DOWNLOADED_AT = datetime(2026, 8, 3, 23, 47, 5, tzinfo=UTC)
RELEASE_DATE = date(2026, 7, 7)


def write_download(save_to_path: str, use_raw_cache: bool) -> None:
    """Write a small table where oda_reader's bulk download writes its file"""
    pq.write_table(pa.table({"DONOR": [1, 2, 3]}), Path(save_to_path) / DOWNLOADED_NAME)


def build_example(
    *, download: Mock, release_date: Mock
) -> tuple[dg.SourceAsset, dg.AssetsDefinition]:
    return build_oecd_bulk_file_assets(
        group="example",
        dataset="things",
        title="Example",
        flow_url=FLOW_URL,
        label=LABEL,
        download=download,
        downloaded_name=DOWNLOADED_NAME,
        release_date=release_date,
    )


@contextmanager
def patch_bulk_lookup(version: str = VERSION) -> Iterator[Mock]:
    """Answer the bulk file lookup the source and the raw file make"""
    with patch(
        "dfo_pipelines.extraction.oecd.get_bulk_file_url_with_version",
        autospec=True,
        return_value=(BULK_FILE_URL, version),
    ) as lookup:
        yield lookup


def materialize_raw_file(
    raw_file: dg.AssetsDefinition, tmp_path: Path
) -> dg.MaterializeResult:
    """Run the raw file asset's body against a file store under `tmp_path`"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path / "raw"))
    with patch("dfo_pipelines.orchestration.raw_files.datetime") as mock_datetime:
        mock_datetime.now.return_value = DOWNLOADED_AT
        result = raw_file(context=dg.build_asset_context(), raw_file_store=file_store)
    assert isinstance(result, dg.MaterializeResult)
    return result


def test_source_observes_the_bulk_file_url_and_version() -> None:
    """Test the observation records the labelled file's URL and version, and joins
    them into the data version"""
    source, _ = build_example(download=Mock(), release_date=Mock())
    assert source.observe_fn is not None

    with patch_bulk_lookup() as lookup:
        result = source.observe_fn()

    lookup.assert_called_once_with(
        flow_url=FLOW_URL, label=LABEL, bypass_annotation_cache=True
    )
    assert isinstance(result, dg.ObserveResult)
    assert result.data_version == dg.DataVersion(f"{BULK_FILE_URL};{VERSION}")
    assert result.metadata == {
        "dagster/uri": dg.TextMetadataValue(BULK_FILE_URL),
        "version": dg.TextMetadataValue(VERSION),
    }


def test_raw_file_writes_the_download_as_published(tmp_path: Path) -> None:
    """Test the downloaded file becomes the raw file unchanged, dated by the
    release-date lookup, with the version the download was resolved at"""
    download = Mock(side_effect=write_download)
    release_date = Mock(return_value=RELEASE_DATE)
    _, raw_file = build_example(download=download, release_date=release_date)

    with patch_bulk_lookup() as lookup:
        result = materialize_raw_file(raw_file, tmp_path)

    directory = tmp_path / "raw" / "example" / "things" / "2026" / "08" / "03"
    lookup.assert_called_once_with(
        flow_url=FLOW_URL, label=LABEL, bypass_annotation_cache=True
    )
    release_date.assert_called_once_with(BULK_FILE_URL, VERSION)
    download.assert_called_once_with(save_to_path=str(directory), use_raw_cache=False)
    assert sorted(path.name for path in directory.iterdir()) == [
        "data.parquet",
        METADATA_FILE_NAME,
    ]
    assert pq.read_table(directory / "data.parquet").to_pydict() == {"DONOR": [1, 2, 3]}
    assert json.loads((directory / METADATA_FILE_NAME).read_text()) == {
        "downloaded_at": "2026-08-03T23:47:05+00:00",
        "source_release_date": "2026-07-07",
    }
    assert result.metadata is not None
    assert (
        result.metadata.items()
        >= {
            "version": dg.TextMetadataValue(VERSION),
            "source_release_date": dg.TextMetadataValue("2026-07-07"),
        }.items()
    )


def test_raw_file_resolves_the_bulk_file_before_downloading(tmp_path: Path) -> None:
    """Test the cache-bypassing lookup runs before the download, which re-resolves
    the URL through the cache that lookup refreshes"""

    with patch_bulk_lookup() as lookup:

        def download(save_to_path: str, use_raw_cache: bool) -> None:
            lookup.assert_called_once()
            write_download(save_to_path, use_raw_cache)

        _, raw_file = build_example(
            download=Mock(side_effect=download),
            release_date=Mock(return_value=RELEASE_DATE),
        )
        materialize_raw_file(raw_file, tmp_path)


def test_raw_file_does_not_download_when_the_release_cannot_be_dated(
    tmp_path: Path,
) -> None:
    """Test a release-date lookup that fails stops the run before the download"""
    download = Mock(side_effect=write_download)
    _, raw_file = build_example(
        download=download,
        release_date=Mock(side_effect=ValueError("no release date")),
    )

    with patch_bulk_lookup(), pytest.raises(ValueError, match="no release date"):
        materialize_raw_file(raw_file, tmp_path)

    download.assert_not_called()


class _ExampleAutomation(AutomationHarness):
    """Drives the built source and raw file against an ephemeral instance"""

    def __init__(self, tmp_path: Path) -> None:
        source, raw_file = build_example(
            download=Mock(side_effect=write_download),
            release_date=Mock(return_value=RELEASE_DATE),
        )
        super().__init__(tmp_path, source=source, raw_assets=[raw_file])

    def _start_mocks(self) -> None:
        self._stack.enter_context(patch_bulk_lookup())


def test_source_observation_records_an_observation_event(tmp_path: Path) -> None:
    """Test observing the source records one observation against the source"""
    with _ExampleAutomation(tmp_path) as automation:
        result = automation.observe()

    assert [
        entry.asset_key
        for entry in result.all_events
        if entry.event_type_value == "ASSET_OBSERVATION"
    ] == [dg.AssetKey("example_source")]


def test_source_is_requested_once_per_daily_cron_tick(tmp_path: Path) -> None:
    """Test the source's condition requests one observation per UTC day

    The 13:00 tick pins the cadence, since a shorter cron would request an
    observation an hour after the last one. The 00:01 tick shows the condition
    requests an observation once the daily tick has passed, and 00:02 shows it
    is not requested again before the next one.
    """
    with _ExampleAutomation(tmp_path) as automation:
        # A baseline tick. Every condition is reset on its initial evaluation,
        # so the first tick requests nothing whatever the time is.
        automation.source_tick(at=datetime(2026, 8, 25, 12, 0, tzinfo=UTC))

        assert automation.source_tick(at=datetime(2026, 8, 25, 13, 0, tzinfo=UTC)) == 0
        assert automation.source_tick(at=datetime(2026, 8, 26, 0, 1, tzinfo=UTC)) == 1
        assert automation.source_tick(at=datetime(2026, 8, 26, 0, 2, tzinfo=UTC)) == 0
