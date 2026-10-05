"""Record what an extraction fetched, stored alongside the files it fetched"""

import json
import re
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import date, datetime

from duckdb import DuckDBPyConnection
from upath import UPath

METADATA_FILE_NAME = "metadata.json"

_RELEASE_NAME_PATTERN = re.compile(r"[a-z][a-z0-9_]*")


@dataclass(frozen=True, kw_only=True)
class ExtractionMetadata:
    """What an extraction recorded about the files it wrote.

    Attributes:
        downloaded_at: Instant the source files were fetched.
        release_dates: Date each named source released the fetched data,
            keyed by source name.
    """

    downloaded_at: datetime
    release_dates: Mapping[str, date]


def write_extraction_metadata(
    directory: UPath, *, downloaded_at: datetime, release_dates: Mapping[str, date]
) -> None:
    """Record extraction metadata in `directory`.

    Args:
        directory: Directory holding the files the extraction wrote.
        downloaded_at: Instant the files were fetched.
        release_dates: Date each named source released the fetched data,
            keyed by source name. Written as `<name>_release_date`.

    Raises:
        ValueError: If `release_dates` is empty.
    """
    if not release_dates:
        raise ValueError("release_dates must name at least one source")
    payload = {"downloaded_at": downloaded_at.isoformat()}
    for name, release_date in release_dates.items():
        payload[f"{name}_release_date"] = release_date.isoformat()
    (directory / METADATA_FILE_NAME).write_text(json.dumps(payload))


def read_extraction_metadata(
    directory: UPath, *, release_names: Collection[str]
) -> ExtractionMetadata:
    """Read the extraction recorded in `directory`.

    Args:
        directory: Directory holding the files the extraction wrote.
        release_names: Names the recorded `<name>_release_date` keys must
            match.

    Returns:
        ExtractionMetadata: The extraction recorded there.

    Raises:
        FileNotFoundError: If the directory holds no record.
        ValueError: If a recorded `*_release_date` key is missing or
            unexpected relative to `release_names`.
    """
    path = directory / METADATA_FILE_NAME
    if not path.exists():
        raise FileNotFoundError(
            f"No {METADATA_FILE_NAME} in {directory}; "
            "rerun the extraction that writes it"
        )
    recorded = json.loads(path.read_text())
    recorded_keys = {key for key in recorded if key.endswith("_release_date")}
    expected_keys = {f"{name}_release_date" for name in release_names}
    missing = expected_keys - recorded_keys
    unexpected = recorded_keys - expected_keys
    if missing or unexpected:
        raise ValueError(
            f"{path} release date keys do not match {sorted(expected_keys)}: "
            f"missing {sorted(missing)}, unexpected {sorted(unexpected)}"
        )
    return ExtractionMetadata(
        downloaded_at=datetime.fromisoformat(recorded["downloaded_at"]),
        release_dates={
            name: date.fromisoformat(recorded[f"{name}_release_date"])
            for name in release_names
        },
    )


def load_extraction_metadata(
    connection: DuckDBPyConnection,
    *,
    directory: UPath,
    schema: str,
    table: str,
    release_names: Collection[str],
) -> int:
    """Load the extraction metadata recorded in `directory` into `schema`.`table`.

    The table is replaced rather than appended to, showing the download
    date for the data that is currently loaded in the raw table.

    Args:
        connection: Connection to the database the table is written to.
        directory: Directory holding the files the extraction wrote.
        schema: Schema to write into, created if it does not exist.
        table: Table to write, replaced if it does.
        release_names: Names the recorded `<name>_release_date` keys must
            match. Also the order the `<name>_release_date` columns are
            written in.

    Returns:
        int: Count of rows written.

    Raises:
        FileNotFoundError: If the directory holds no record.
        ValueError: If a recorded release date key does not match
            `release_names`, or a name is not a valid identifier.
    """
    for name in release_names:
        if not _RELEASE_NAME_PATTERN.fullmatch(name):
            raise ValueError(
                f"Invalid release name {name!r}: must match "
                f"{_RELEASE_NAME_PATTERN.pattern!r}"
            )
    metadata = read_extraction_metadata(directory, release_names=release_names)
    connection.execute(f"create schema if not exists {schema}")
    release_columns = ", ".join(
        f"cast(? as date) as {name}_release_date" for name in release_names
    )
    connection.execute(
        f"create or replace table {schema}.{table} as "
        f"select cast(? as timestamptz) as downloaded_at, {release_columns}",
        [
            metadata.downloaded_at,
            *(metadata.release_dates[name] for name in release_names),
        ],
    )
    return connection.sql(f"select count(*) from {schema}.{table}").fetchall()[0][0]
