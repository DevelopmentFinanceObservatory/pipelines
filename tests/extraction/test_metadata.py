import hashlib
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from upath import UPath

from dfo_pipelines.extraction.metadata import (
    METADATA_FILE_NAME,
    ExtractionMetadata,
    FetchedRelease,
    commit_extraction_metadata,
    file_hashes,
    read_extraction_metadata,
)
from tests.helpers import mark_committed

DOWNLOADED_AT = datetime(2026, 8, 3, 23, 47, 5, tzinfo=UTC)
RELEASE_DATES = {"crs": date(2026, 8, 3), "multisystem": date(2026, 7, 15)}
LANDING_ASSET = "example_landing"


def test_extraction_metadata_round_trips(tmp_path: Path) -> None:
    """Test what was recorded reads back equal"""
    mark_committed(tmp_path, downloaded_at=DOWNLOADED_AT, release_dates=RELEASE_DATES)
    assert read_extraction_metadata(
        UPath(tmp_path), release_names=RELEASE_DATES.keys(), landing_asset=LANDING_ASSET
    ) == ExtractionMetadata(downloaded_at=DOWNLOADED_AT, release_dates=RELEASE_DATES)


def test_file_hashes_key_each_file_by_its_path_in_the_directory(
    tmp_path: Path,
) -> None:
    """Test files in subdirectories are hashed under their relative POSIX path, and
    the extraction metadata is left out"""
    (tmp_path / "skills" / "debt").mkdir(parents=True)
    (tmp_path / "skills" / "debt" / "SKILL.md").write_text("debt")
    (tmp_path / "top.txt").write_text("top")
    (tmp_path / METADATA_FILE_NAME).write_text("{}")

    assert file_hashes(UPath(tmp_path)) == {
        "skills/debt/SKILL.md": hashlib.sha256(b"debt").hexdigest(),
        "top.txt": hashlib.sha256(b"top").hexdigest(),
    }


def test_committed_extraction_metadata_reads_back_its_recorded_values(
    tmp_path: Path,
) -> None:
    """Test values recorded beside the release dates read back, and the run and file
    hashes are not among them"""
    commit_extraction_metadata(
        UPath(tmp_path),
        downloaded_at=DOWNLOADED_AT,
        run_id="run",
        fetched=FetchedRelease(
            release_dates={}, metadata={}, recorded={"commit": "abc123"}
        ),
        files={"SKILL.md": "0" * 64},
    )

    assert read_extraction_metadata(
        UPath(tmp_path), release_names=(), landing_asset=LANDING_ASSET
    ) == ExtractionMetadata(
        downloaded_at=DOWNLOADED_AT, release_dates={}, recorded={"commit": "abc123"}
    )


@pytest.mark.parametrize(
    "name", ["downloaded_at", "run_id", "files", "source_release_date"]
)
def test_commit_extraction_metadata_refuses_a_recorded_value_named_as_its_own_key(
    tmp_path: Path, name: str
) -> None:
    """Test a recorded value cannot overwrite an entry the extraction metadata
    writes itself, and nothing is written"""
    with pytest.raises(ValueError, match=f"Recorded values \\['{name}'\\] clash"):
        commit_extraction_metadata(
            UPath(tmp_path),
            downloaded_at=DOWNLOADED_AT,
            run_id="run",
            fetched=FetchedRelease(
                release_dates={"source": date(2026, 8, 3)},
                metadata={},
                recorded={name: "overwritten"},
            ),
            files={},
        )

    assert not (tmp_path / METADATA_FILE_NAME).exists()


def test_read_extraction_metadata_raises_when_nothing_was_recorded(
    tmp_path: Path,
) -> None:
    """Test a directory with no record fails as incomplete, naming the asset that
    writes it"""
    with pytest.raises(
        FileNotFoundError,
        match=f"No {METADATA_FILE_NAME} .* materialize {LANDING_ASSET}",
    ):
        read_extraction_metadata(
            UPath(tmp_path), release_names=("source",), landing_asset=LANDING_ASSET
        )


def test_read_extraction_metadata_raises_when_a_release_name_is_missing(
    tmp_path: Path,
) -> None:
    """Test a name recorded elsewhere but not requested here is refused"""
    mark_committed(tmp_path, downloaded_at=DOWNLOADED_AT, release_dates=RELEASE_DATES)
    with pytest.raises(ValueError, match="crs_release_date"):
        read_extraction_metadata(
            UPath(tmp_path), release_names=("multisystem",), landing_asset=LANDING_ASSET
        )


def test_read_extraction_metadata_raises_when_a_release_name_is_unexpected(
    tmp_path: Path,
) -> None:
    """Test a requested name that was never recorded is refused"""
    mark_committed(tmp_path, downloaded_at=DOWNLOADED_AT, release_dates=RELEASE_DATES)
    with pytest.raises(ValueError, match="unmapped_release_date"):
        read_extraction_metadata(
            UPath(tmp_path),
            release_names=(*RELEASE_DATES.keys(), "unmapped"),
            landing_asset=LANDING_ASSET,
        )
