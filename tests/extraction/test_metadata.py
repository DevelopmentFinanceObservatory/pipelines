import hashlib
from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from upath import UPath

from dfo_pipelines.extraction.metadata import (
    METADATA_FILE_NAME,
    ExtractionMetadata,
    file_hashes,
    read_extraction_metadata,
    write_extraction_metadata,
)

DOWNLOADED_AT = datetime(2026, 8, 3, 23, 47, 5, tzinfo=UTC)
RELEASE_DATES = {"crs": date(2026, 8, 3), "multisystem": date(2026, 7, 15)}
RUN_ID = "example-run"
FILES = {"things.parquet": "0" * 64}


def test_file_hashes_names_each_file_by_its_relative_path(tmp_path: Path) -> None:
    """Test every file under the directory, nested ones included, is hashed by its
    POSIX path in sorted order, and the extraction metadata is left out"""
    (tmp_path / "skills" / "debt").mkdir(parents=True)
    (tmp_path / "skills" / "debt" / "SKILL.md").write_bytes(b"debt")
    (tmp_path / "notes.txt").write_bytes(b"notes")
    (tmp_path / METADATA_FILE_NAME).write_text("{}")

    hashes = file_hashes(UPath(tmp_path))

    assert list(hashes.items()) == [
        ("notes.txt", hashlib.sha256(b"notes").hexdigest()),
        ("skills/debt/SKILL.md", hashlib.sha256(b"debt").hexdigest()),
    ]


def test_extraction_metadata_round_trips(tmp_path: Path) -> None:
    """Test what was recorded reads back equal"""
    write_extraction_metadata(
        UPath(tmp_path),
        downloaded_at=DOWNLOADED_AT,
        run_id=RUN_ID,
        release_dates=RELEASE_DATES,
        files=FILES,
    )
    assert read_extraction_metadata(
        UPath(tmp_path), release_names=RELEASE_DATES.keys()
    ) == ExtractionMetadata(
        downloaded_at=DOWNLOADED_AT, release_dates=RELEASE_DATES, files=FILES
    )


def test_write_extraction_metadata_refuses_an_empty_mapping(tmp_path: Path) -> None:
    """Test an extraction cannot record zero release dates"""
    with pytest.raises(ValueError, match="release_dates"):
        write_extraction_metadata(
            UPath(tmp_path),
            downloaded_at=DOWNLOADED_AT,
            run_id=RUN_ID,
            release_dates={},
            files=FILES,
        )


def test_read_extraction_metadata_raises_when_nothing_was_recorded(
    tmp_path: Path,
) -> None:
    """Test a directory with no record fails rather than returning a default"""
    with pytest.raises(FileNotFoundError, match=METADATA_FILE_NAME):
        read_extraction_metadata(UPath(tmp_path), release_names=("source",))


def test_read_extraction_metadata_raises_when_a_release_name_is_missing(
    tmp_path: Path,
) -> None:
    """Test a name recorded elsewhere but not requested here is refused"""
    write_extraction_metadata(
        UPath(tmp_path),
        downloaded_at=DOWNLOADED_AT,
        run_id=RUN_ID,
        release_dates=RELEASE_DATES,
        files=FILES,
    )
    with pytest.raises(ValueError, match="crs_release_date"):
        read_extraction_metadata(UPath(tmp_path), release_names=("multisystem",))


def test_read_extraction_metadata_raises_when_a_release_name_is_unexpected(
    tmp_path: Path,
) -> None:
    """Test a requested name that was never recorded is refused"""
    write_extraction_metadata(
        UPath(tmp_path),
        downloaded_at=DOWNLOADED_AT,
        run_id=RUN_ID,
        release_dates=RELEASE_DATES,
        files=FILES,
    )
    with pytest.raises(ValueError, match="unmapped_release_date"):
        read_extraction_metadata(
            UPath(tmp_path), release_names=(*RELEASE_DATES.keys(), "unmapped")
        )
