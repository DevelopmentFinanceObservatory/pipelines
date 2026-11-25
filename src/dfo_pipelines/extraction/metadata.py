"""Record what an extraction fetched, stored alongside the files it fetched"""

import json
from collections.abc import Collection, Mapping
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
) -> dict[str, Any]:
    """Read the extraction recorded in `directory`, as its metadata table's row.

    Args:
        directory: Directory holding the files the extraction wrote.
        release_names: Names the recorded `<name>_release_date` keys must
            match.

    Returns:
        dict[str, Any]: `downloaded_at`, then each `<name>_release_date`, in the
            order of `release_names`.

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
    return {
        "downloaded_at": datetime.fromisoformat(recorded["downloaded_at"]),
        **{
            f"{name}_release_date": date.fromisoformat(recorded[f"{name}_release_date"])
            for name in release_names
        },
    }
