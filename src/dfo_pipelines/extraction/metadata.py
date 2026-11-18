"""Record what an extraction fetched, stored alongside the files it fetched"""

import hashlib
import json
from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime
from types import MappingProxyType
from typing import Any, NamedTuple

from upath import UPath

METADATA_FILE_NAME = "metadata.json"

_HASH_CHUNK_SIZE = 2**20

# Entries of the extraction metadata other than release dates and recorded values
_MARKER_KEYS = frozenset({"downloaded_at", "run_id", "files"})


class FetchedRelease(NamedTuple):
    """What a fetch found: the release dates and the source's own metadata

    Attributes:
        release_dates: Date each named source released the fetched data, keyed by
            source name.
        metadata: The source's own materialization metadata.
        recorded: Values the extraction metadata records beside the release dates,
            such as the commit a repository was fetched at.
    """

    release_dates: Mapping[str, date]
    metadata: Mapping[str, Any]
    recorded: Mapping[str, str] = MappingProxyType({})


@dataclass(frozen=True, kw_only=True)
class ExtractionMetadata:
    """What an extraction recorded about the files it wrote.

    Attributes:
        downloaded_at: Instant the source files were fetched.
        release_dates: Date each named source released the fetched data,
            keyed by source name.
        recorded: Values recorded beside the release dates, such as a commit.
    """

    downloaded_at: datetime
    release_dates: Mapping[str, date]
    recorded: Mapping[str, str] = field(default_factory=dict)


def file_hashes(directory: UPath) -> dict[str, str]:
    """sha256 of each file under `directory` by POSIX path relative to it, the
    extraction metadata excluded"""
    hashes = {}
    for path in directory.rglob("*"):
        relative_path = path.relative_to(directory).as_posix()
        if relative_path == METADATA_FILE_NAME or not path.is_file():
            continue
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(_HASH_CHUNK_SIZE), b""):
                digest.update(chunk)
        hashes[relative_path] = digest.hexdigest()
    return dict(sorted(hashes.items()))


def _release_date_entries(release_dates: Mapping[str, date]) -> dict[str, str]:
    return {
        f"{name}_release_date": release_date.isoformat()
        for name, release_date in release_dates.items()
    }


def commit_extraction_metadata(
    directory: UPath,
    *,
    downloaded_at: datetime,
    run_id: str,
    fetched: FetchedRelease,
    files: Mapping[str, str],
) -> None:
    """Record extraction metadata in `directory`, marking it complete

    Written once every file is in place, since a directory without it is never read.

    Args:
        directory: Directory holding the files the extraction wrote.
        downloaded_at: Instant the files were fetched.
        run_id: Run that wrote the directory.
        fetched: The release dates, written as `<name>_release_date`, and the
            recorded values, written under their own names.
        files: sha256 of each file in the directory, from `file_hashes`.

    Raises:
        ValueError: If a recorded value's name is one of the extraction
            metadata's own keys.
    """
    clashing = sorted(
        name
        for name in fetched.recorded
        if name in _MARKER_KEYS or name.endswith("_release_date")
    )
    if clashing:
        raise ValueError(
            f"Recorded values {clashing} clash with the extraction metadata's own keys"
        )
    payload = {
        "downloaded_at": downloaded_at.isoformat(),
        "run_id": run_id,
        **_release_date_entries(fetched.release_dates),
        **fetched.recorded,
        "files": dict(files),
    }
    (directory / METADATA_FILE_NAME).write_text(json.dumps(payload))


def holds_same_download(
    directory: UPath, *, files: Mapping[str, str], release_dates: Mapping[str, date]
) -> bool:
    """Whether `directory` recorded the same file hashes and release dates

    False when it holds no readable extraction metadata or recorded no file hashes.
    """
    path = directory / METADATA_FILE_NAME
    if not path.exists():
        return False
    try:
        recorded = json.loads(path.read_text())
    except ValueError:
        return False
    if not isinstance(recorded, dict) or "files" not in recorded:
        return False
    recorded_release_dates = {
        key: value for key, value in recorded.items() if key.endswith("_release_date")
    }
    return recorded["files"] == files and recorded_release_dates == (
        _release_date_entries(release_dates)
    )


def read_extraction_metadata(
    directory: UPath, *, release_names: Collection[str], landing_asset: str
) -> ExtractionMetadata:
    """Read the extraction recorded in `directory`.

    Args:
        directory: Directory holding the files the extraction wrote.
        release_names: Names the recorded `<name>_release_date` keys must
            match.
        landing_asset: Asset that writes the directory, named in the error when the
            directory was never completed.

    Returns:
        ExtractionMetadata: The extraction recorded there.

    Raises:
        FileNotFoundError: If the directory holds no record, so its write never
            completed.
        ValueError: If a recorded `*_release_date` key is missing or
            unexpected relative to `release_names`.
    """
    path = directory / METADATA_FILE_NAME
    if not path.exists():
        raise FileNotFoundError(
            f"No {METADATA_FILE_NAME} in {directory}, so the run writing it did "
            f"not complete; materialize {landing_asset}"
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
        recorded={
            key: value
            for key, value in recorded.items()
            if key not in _MARKER_KEYS and not key.endswith("_release_date")
        },
    )
