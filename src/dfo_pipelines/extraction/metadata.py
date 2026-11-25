"""Record what an extraction fetched, stored alongside the files it fetched"""

import json
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, NamedTuple

from upath import UPath

METADATA_FILE_NAME = "metadata.json"


class FetchedRelease(NamedTuple):
    """What a fetch found: the release dates and the source's own metadata

    Attributes:
        release_dates: Date each named source released the fetched data, keyed by
            source name.
        metadata: The source's own materialization metadata.
    """

    release_dates: Mapping[str, date]
    metadata: Mapping[str, Any]


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

    def as_row(self) -> dict[str, Any]:
        """The metadata as a table row: `downloaded_at`, then a `<name>_release_date`
        column for each release date, in their order"""
        return {
            "downloaded_at": self.downloaded_at,
            **{
                f"{name}_release_date": release_date
                for name, release_date in self.release_dates.items()
            },
        }


def write_extraction_metadata(
    directory: UPath,
    *,
    downloaded_at: datetime,
    run_id: str,
    release_dates: Mapping[str, date],
) -> None:
    """Record extraction metadata in `directory`.

    Args:
        directory: Directory holding the files the extraction wrote.
        downloaded_at: Instant the files were fetched.
        run_id: Run that wrote the directory.
        release_dates: Date each named source released the fetched data,
            keyed by source name. Written as `<name>_release_date`.

    Raises:
        ValueError: If `release_dates` is empty.
    """
    if not release_dates:
        raise ValueError("release_dates must name at least one source")
    payload = {"downloaded_at": downloaded_at.isoformat(), "run_id": run_id}
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
