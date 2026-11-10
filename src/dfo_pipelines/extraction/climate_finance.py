"""The layout of a climate finance export, which the export script writes and the
climate finance pipeline reads.

An export root holds one directory per release, named for it, with the raw tables
as `<table>.parquet` and `manifest.json` (`ExportManifest`), and `latest.json`
(`LatestExport`), which names the newest release and its manifest's sha256. The
script writes `latest.json` last, so a reader that checks the manifest against it
never takes a half-written export.
"""

import hashlib
from datetime import datetime

from pydantic import BaseModel, ConfigDict

# The raw tables of an export, by the stem of their parquet file.
RAW_TABLES = ("spending", "crdf_commitments", "btr_totals")
LATEST_FILE_NAME = "latest.json"
MANIFEST_FILE_NAME = "manifest.json"


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ExportedTable(_Frozen):
    """One raw table of an export: the sha256 of its parquet file and its rows."""

    sha256: str
    rows: int


class ExportManifest(_Frozen):
    """What one export holds and what it was made from.

    Attributes:
        release: The release's name, such as `2026a-rc9`.
        created_at: When the release was built, as its `release.json` records.
        release_json_sha256: The sha256 of the release's `release.json`, which
            records the sha256 of every release table.
        pack_sha256: The sha256 of the pack the release was unpacked from, or
            None when it was exported from a directory.
        package_commit: The git commit of the climate-finance package that ran the
            extracts, or None when it was not installed from git.
        package_source_sha256: The sha256 of that package's source files, equal to
            the digest the release records of the code that built it.
        exported_at: When the export was written.
        tables: Each raw table by the stem of its parquet file.
    """

    release: str
    created_at: datetime
    release_json_sha256: str
    pack_sha256: str | None
    package_commit: str | None
    package_source_sha256: str
    exported_at: datetime
    tables: dict[str, ExportedTable]


class LatestExport(_Frozen):
    """The contents of `latest.json`: the newest export's release and the sha256
    of its `manifest.json`."""

    release: str
    manifest_sha256: str


def sha256_bytes(data: bytes) -> str:
    """The hex sha256 of `data`."""
    return hashlib.sha256(data).hexdigest()
