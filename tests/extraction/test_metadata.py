from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pytest
from upath import UPath

from dfo_pipelines.extraction.metadata import (
    METADATA_FILE_NAME,
    ExtractionMetadata,
    load_extraction_metadata,
    read_extraction_metadata,
    write_extraction_metadata,
)

DOWNLOADED_AT = datetime(2026, 8, 3, 23, 47, 5, tzinfo=UTC)
RELEASE_DATES = {"crs": date(2026, 8, 3), "multisystem": date(2026, 7, 15)}


def test_extraction_metadata_round_trips(tmp_path: Path) -> None:
    """Test what was recorded reads back equal"""
    write_extraction_metadata(
        UPath(tmp_path), downloaded_at=DOWNLOADED_AT, release_dates=RELEASE_DATES
    )
    assert read_extraction_metadata(
        UPath(tmp_path), release_names=RELEASE_DATES.keys()
    ) == ExtractionMetadata(downloaded_at=DOWNLOADED_AT, release_dates=RELEASE_DATES)


def test_write_extraction_metadata_refuses_an_empty_mapping(tmp_path: Path) -> None:
    """Test an extraction cannot record zero release dates"""
    with pytest.raises(ValueError, match="release_dates"):
        write_extraction_metadata(
            UPath(tmp_path), downloaded_at=DOWNLOADED_AT, release_dates={}
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
        UPath(tmp_path), downloaded_at=DOWNLOADED_AT, release_dates=RELEASE_DATES
    )
    with pytest.raises(ValueError, match="crs_release_date"):
        read_extraction_metadata(UPath(tmp_path), release_names=("multisystem",))


def test_read_extraction_metadata_raises_when_a_release_name_is_unexpected(
    tmp_path: Path,
) -> None:
    """Test a requested name that was never recorded is refused"""
    write_extraction_metadata(
        UPath(tmp_path), downloaded_at=DOWNLOADED_AT, release_dates=RELEASE_DATES
    )
    with pytest.raises(ValueError, match="unmapped_release_date"):
        read_extraction_metadata(
            UPath(tmp_path), release_names=(*RELEASE_DATES.keys(), "unmapped")
        )


def test_load_extraction_metadata_replaces_the_previous_row(tmp_path: Path) -> None:
    """Test a rerun leaves one row, holding what it recorded"""
    connection = duckdb.connect()
    write_extraction_metadata(
        UPath(tmp_path), downloaded_at=DOWNLOADED_AT, release_dates=RELEASE_DATES
    )
    load_extraction_metadata(
        connection,
        directory=UPath(tmp_path),
        schema="raw",
        table="metadata",
        release_names=("crs", "multisystem"),
    )

    later = datetime(2026, 9, 8, 11, 2, tzinfo=UTC)
    later_release_dates = {"crs": date(2026, 9, 7), "multisystem": date(2026, 9, 1)}
    write_extraction_metadata(
        UPath(tmp_path), downloaded_at=later, release_dates=later_release_dates
    )
    rows = load_extraction_metadata(
        connection,
        directory=UPath(tmp_path),
        schema="raw",
        table="metadata",
        release_names=("crs", "multisystem"),
    )

    assert rows == 1
    assert connection.sql(
        "select downloaded_at, crs_release_date, multisystem_release_date "
        "from raw.metadata"
    ).fetchall() == [
        (later, later_release_dates["crs"], later_release_dates["multisystem"])
    ]


def test_load_extraction_metadata_creates_columns_in_the_given_order(
    tmp_path: Path,
) -> None:
    """Test the release date columns follow the order release_names is given in"""
    connection = duckdb.connect()
    write_extraction_metadata(
        UPath(tmp_path), downloaded_at=DOWNLOADED_AT, release_dates=RELEASE_DATES
    )
    load_extraction_metadata(
        connection,
        directory=UPath(tmp_path),
        schema="raw",
        table="metadata",
        release_names=("multisystem", "crs"),
    )

    assert [row[0] for row in connection.sql("describe raw.metadata").fetchall()] == [
        "downloaded_at",
        "multisystem_release_date",
        "crs_release_date",
    ]


def test_load_extraction_metadata_raises_when_nothing_was_recorded(
    tmp_path: Path,
) -> None:
    """Test an unrecorded extraction fails rather than loading an empty table"""
    connection = duckdb.connect()

    with pytest.raises(FileNotFoundError, match=METADATA_FILE_NAME):
        load_extraction_metadata(
            connection,
            directory=UPath(tmp_path),
            schema="raw",
            table="metadata",
            release_names=("source",),
        )


def test_load_extraction_metadata_raises_on_an_invalid_release_name(
    tmp_path: Path,
) -> None:
    """Test a release name that is not a safe SQL identifier is refused"""
    connection = duckdb.connect()
    write_extraction_metadata(
        UPath(tmp_path),
        downloaded_at=DOWNLOADED_AT,
        release_dates={"Crs Source": date(2026, 8, 3)},
    )

    with pytest.raises(ValueError, match="Crs Source"):
        load_extraction_metadata(
            connection,
            directory=UPath(tmp_path),
            schema="raw",
            table="metadata",
            release_names=("Crs Source",),
        )
