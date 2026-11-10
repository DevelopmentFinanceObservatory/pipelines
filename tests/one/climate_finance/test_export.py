"""Tests of scripts/export_climate_finance_release.py.

They need the climate-finance package, which CI does not install, and skip without
it. Run them from the repository root with the package at the commit that built
rc9:

    uv run --with "climate-finance @ git+https://github.com/ONEcampaign/climate-finance-package@efc1ddde3bf99d3e8e6ad787aaf916211d5049c9" \\
        pytest tests/one/climate_finance
"""

import json
import shutil
import tarfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from upath import UPath

from dfo_pipelines.extraction.climate_finance import (
    RAW_TABLES,
    ExportManifest,
    LatestExport,
    sha256_bytes,
)
from tests.one.climate_finance.conftest import EXPORT, RELEASE

cf = pytest.importorskip("climate_finance")
pl = pytest.importorskip("polars")
export_script = pytest.importorskip("scripts.export_climate_finance_release")

NAME = "2026a-rc9"


@pytest.fixture
def extracted(monkeypatch: pytest.MonkeyPatch, raw_tables: dict[str, Any]) -> None:
    """Hand the export the raw tables extracted once per session"""
    extracts = {
        stem: lambda release, stem=stem: raw_tables[stem] for stem in raw_tables
    }
    monkeypatch.setattr(export_script, "EXTRACTS", extracts)


def _manifest(root: Path) -> ExportManifest:
    return ExportManifest.model_validate_json(
        (root / NAME / "manifest.json").read_bytes()
    )


def _edited_release(tmp_path: Path, edit: Callable[[dict[str, Any]], None]) -> Path:
    """A copy of the cut whose release.json `edit` has changed"""
    release = tmp_path / "release"
    shutil.copytree(RELEASE, release)
    manifest = json.loads((release / "release.json").read_text())
    edit(manifest)
    (release / "release.json").write_text(json.dumps(manifest))
    return release


def _interrupt_export(monkeypatch: pytest.MonkeyPatch, root: Path) -> None:
    """Export the cut to `root`, failing as latest.json is written"""

    def fail(self: Any, **kwargs: Any) -> str:
        raise OSError("pointer write failed")

    with monkeypatch.context() as patch:
        patch.setattr(export_script.LatestExport, "model_dump_json", fail)
        with pytest.raises(OSError, match="pointer write failed"):
            export_script.export(RELEASE, to=UPath(root))


def _record_another_release_json(directory: Path) -> None:
    path = directory / "manifest.json"
    path.write_text(
        json.dumps({**json.loads(path.read_text()), "release_json_sha256": "0" * 64})
    )


def _files(root: Path) -> dict[str, bytes]:
    return {
        str(path.relative_to(root)): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def test_extracts_cover_every_raw_table() -> None:
    """Test the script extracts the tables the pipeline reads, in its order"""
    assert tuple(export_script.EXTRACTS) == RAW_TABLES


def test_package_source_sha256_matches_the_digest_the_release_records() -> None:
    """Test the installed package hashes to the digest of the code that built rc9"""
    manifest = json.loads((RELEASE / "release.json").read_text())
    assert (
        export_script.package_source_sha256()
        == manifest["build"]["package_source_sha256"]
    )


@pytest.mark.usefixtures("extracted")
def test_export_writes_tables_and_manifest_then_points_latest_at_them(
    tmp_path: Path, raw_tables: dict[str, Any]
) -> None:
    """Test the export writes each table, a manifest of their sha256 and the
    release, and a latest.json naming the manifest's sha256"""
    root = tmp_path / "export"

    export_script.export(RELEASE, to=UPath(root))

    manifest_bytes = (root / NAME / "manifest.json").read_bytes()
    latest = LatestExport.model_validate_json((root / "latest.json").read_bytes())
    assert latest == LatestExport(
        release=NAME, manifest_sha256=sha256_bytes(manifest_bytes)
    )
    manifest = _manifest(root)
    release_json = (RELEASE / "release.json").read_bytes()
    assert manifest.release == NAME
    assert manifest.created_at.isoformat() == "2026-11-09T23:24:57.683173+00:00"
    assert manifest.release_json_sha256 == sha256_bytes(release_json)
    assert manifest.pack_sha256 is None
    assert manifest.package_source_sha256 == export_script.package_source_sha256()
    for stem, frame in raw_tables.items():
        data = (root / NAME / f"{stem}.parquet").read_bytes()
        assert manifest.tables[stem].sha256 == sha256_bytes(data)
        assert manifest.tables[stem].rows == frame.height
        assert pl.read_parquet(root / NAME / f"{stem}.parquet").equals(frame)


@pytest.mark.usefixtures("extracted")
def test_export_records_the_pack_it_unpacked(tmp_path: Path) -> None:
    """Test an export from a .tar.zst pack records the pack's sha256"""
    pack = tmp_path / f"climate-finance-{NAME}.tar.zst"
    with (
        pa.output_stream(str(pack), compression="zstd") as stream,
        tarfile.open(fileobj=stream, mode="w|") as tar,
    ):
        tar.add(RELEASE, arcname=f"climate-finance-{NAME}")

    export_script.export(pack, to=UPath(tmp_path / "export"))

    assert _manifest(tmp_path / "export").pack_sha256 == sha256_bytes(pack.read_bytes())


def test_export_refuses_a_release_another_package_built(tmp_path: Path) -> None:
    """Test a release whose recorded package source differs from the installed
    package's is refused before anything is written"""
    release = _edited_release(
        tmp_path, lambda m: m["build"].update(package_source_sha256="0" * 64)
    )

    with pytest.raises(ValueError, match="install the package at the commit"):
        export_script.export(release, to=UPath(tmp_path / "export"))
    assert not (tmp_path / "export").exists()


@pytest.mark.parametrize(
    ("edit", "mismatch"),
    [
        (
            lambda m: m["settings"].update({"years.crs_last_complete": 2023}),
            "years.crs_last_complete is 2023, expected 2024",
        ),
        (
            lambda m: m["years"].update(crdf_recipient=[2000, 2025]),
            "crdf_recipient end is 2025, expected 2024",
        ),
        (
            lambda m: m["years"].update(crdf_provider=[2013, 2023]),
            "crdf_provider end is 2023, expected 2024",
        ),
        (
            lambda m: m["years"].pop("crdf_provider"),
            "crdf_provider end is None, expected 2024",
        ),
        (
            lambda m: m["settings"].pop("years.crs_last_complete"),
            "years.crs_last_complete is None, expected 2024",
        ),
    ],
    ids=[
        "crs-last-complete",
        "crdf-recipient",
        "crdf-provider",
        "missing-crdf-provider",
        "missing-crs-last-complete",
    ],
)
def test_export_refuses_a_release_whose_years_differ_from_the_limits(
    tmp_path: Path, edit: Callable[[dict[str, Any]], None], mismatch: str
) -> None:
    """Test a release whose recorded coverage differs from RECEIVED_YEARS and
    PROVIDED_YEARS in any of the three fields, or lacks one, is refused naming
    the mismatch, and nothing is written"""
    release = _edited_release(tmp_path, edit)

    with pytest.raises(ValueError, match=mismatch):
        export_script.export(release, to=UPath(tmp_path / "export"))
    assert not (tmp_path / "export").exists()


@pytest.mark.usefixtures("extracted")
def test_export_retry_publishes_an_interrupted_export_unchanged(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Test a retry after latest.json failed to write publishes the pointer and
    leaves the tables and manifest as the first attempt wrote them"""
    root = tmp_path / "export"
    _interrupt_export(monkeypatch, root)
    interrupted = _files(root)
    assert "latest.json" not in interrupted

    export_script.export(RELEASE, to=UPath(root))

    published = _files(root)
    assert (
        published.pop("latest.json")
        == (
            LatestExport(
                release=NAME,
                manifest_sha256=sha256_bytes(published[f"{NAME}/manifest.json"]),
            ).model_dump_json(indent=2)
            + "\n"
        ).encode()
    )
    assert published == interrupted


@pytest.mark.usefixtures("extracted")
def test_export_retry_after_publishing_changes_nothing(tmp_path: Path) -> None:
    """Test a second export of a release that latest.json already names succeeds
    and rewrites no file"""
    root = tmp_path / "export"
    export_script.export(RELEASE, to=UPath(root))
    published = _files(root)

    export_script.export(RELEASE, to=UPath(root))

    assert _files(root) == published


@pytest.mark.usefixtures("extracted")
@pytest.mark.parametrize(
    ("tamper", "match"),
    [
        (
            lambda directory: (directory / "btr_totals.parquet").write_bytes(b"x"),
            "differs from the sha256",
        ),
        (
            lambda directory: (directory / "btr_totals.parquet").unlink(),
            "is missing or differs",
        ),
        (_record_another_release_json, "made from another release.json"),
    ],
    ids=["altered-table", "missing-table", "other-release-json"],
)
def test_export_retry_refuses_an_export_that_is_not_the_release(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    tamper: Callable[[Path], object],
    match: str,
) -> None:
    """Test a retry refuses an export whose table was altered or removed or whose
    manifest records another release.json, and writes nothing"""
    root = tmp_path / "export"
    _interrupt_export(monkeypatch, root)
    tamper(root / NAME)
    tampered = _files(root)

    with pytest.raises(ValueError, match=match):
        export_script.export(RELEASE, to=UPath(root))
    assert _files(root) == tampered


@pytest.mark.usefixtures("extracted")
@pytest.mark.parametrize(
    ("built", "refused"),
    [("2026-11-09T00:00:00Z", False), ("2026-11-10T14:28:44.071479Z", True)],
    ids=["older", "same-time"],
)
def test_export_refuses_a_release_no_newer_than_latest(
    tmp_path: Path, built: str, refused: bool
) -> None:
    """Test latest.json moves only to a release built after the one it names,
    and a refused release writes nothing"""
    root = tmp_path / "export"
    (root / "2026b").mkdir(parents=True)
    current = ExportManifest(
        release="2026b",
        created_at=built,
        release_json_sha256="0" * 64,
        pack_sha256=None,
        package_commit=None,
        package_source_sha256="0" * 64,
        exported_at=built,
        tables={},
    )
    manifest_bytes = current.model_dump_json().encode()
    (root / "2026b" / "manifest.json").write_bytes(manifest_bytes)
    latest_json = LatestExport(
        release="2026b", manifest_sha256=sha256_bytes(manifest_bytes)
    ).model_dump_json()
    (root / "latest.json").write_text(latest_json)

    if refused:
        with pytest.raises(ValueError, match="is no newer than 2026b"):
            export_script.export(RELEASE, to=UPath(root))
        assert (root / "latest.json").read_text() == latest_json
        assert not (root / NAME).exists()
    else:
        export_script.export(RELEASE, to=UPath(root))
        latest = LatestExport.model_validate_json((root / "latest.json").read_bytes())
        assert latest.release == NAME


def test_export_points_latest_at_nothing_until_every_table_is_written(
    monkeypatch: pytest.MonkeyPatch, raw_tables: dict[str, Any], tmp_path: Path
) -> None:
    """Test an export that fails partway leaves no latest.json"""

    def fail(release: Any) -> Any:
        raise RuntimeError("extract failed")

    extracts = {
        stem: lambda release, stem=stem: raw_tables[stem] for stem in raw_tables
    }
    monkeypatch.setattr(export_script, "EXTRACTS", {**extracts, "btr_totals": fail})
    with pytest.raises(RuntimeError, match="extract failed"):
        export_script.export(RELEASE, to=UPath(tmp_path / "export"))
    assert not (tmp_path / "export" / "latest.json").exists()


@pytest.mark.usefixtures("extracted")
def test_committed_export_is_the_export_of_the_cut(tmp_path: Path) -> None:
    """Test the export the asset tests read is what the script writes for the cut,
    so it changes with the extracts. cut_climate_finance_fixture.py rewrites it"""
    export_script.export(RELEASE, to=UPath(tmp_path / "export"))

    fresh, committed = _manifest(tmp_path / "export"), _manifest(EXPORT)
    unstamped = {"exported_at", "package_commit"}
    assert fresh.model_dump(exclude=unstamped) == committed.model_dump(
        exclude=unstamped
    )
