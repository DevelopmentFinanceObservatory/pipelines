from datetime import UTC, date, datetime
from pathlib import Path

import pytest
from upath import UPath

from dfo_pipelines.extraction.metadata import (
    METADATA_FILE_NAME,
    read_extraction_metadata,
    write_extraction_metadata,
)

DOWNLOADED_AT = datetime(2026, 8, 3, 23, 47, 5, tzinfo=UTC)
RELEASE_DATES = {"crs": date(2026, 8, 3), "multisystem": date(2026, 7, 15)}
RUN_ID = "example-run"


def test_extraction_metadata_reads_back_as_a_row_in_the_order_of_the_releases(
    tmp_path: Path,
) -> None:
    """Test what was recorded reads back as the download instant, then each release
    date column in the order the releases are named"""
    write_extraction_metadata(
        UPath(tmp_path),
        downloaded_at=DOWNLOADED_AT,
        run_id=RUN_ID,
        release_dates=RELEASE_DATES,
    )
    row = read_extraction_metadata(
        UPath(tmp_path), release_names=("multisystem", "crs")
    )

    assert list(row.items()) == [
        ("downloaded_at", DOWNLOADED_AT),
        ("multisystem_release_date", RELEASE_DATES["multisystem"]),
        ("crs_release_date", RELEASE_DATES["crs"]),
    ]


def test_write_extraction_metadata_refuses_an_empty_mapping(tmp_path: Path) -> None:
    """Test an extraction cannot record zero release dates"""
    with pytest.raises(ValueError, match="release_dates"):
        write_extraction_metadata(
            UPath(tmp_path),
            downloaded_at=DOWNLOADED_AT,
            run_id=RUN_ID,
            release_dates={},
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
    )
    with pytest.raises(ValueError, match="unmapped_release_date"):
        read_extraction_metadata(
            UPath(tmp_path), release_names=(*RELEASE_DATES.keys(), "unmapped")
        )
