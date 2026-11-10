"""Export the climate finance pipeline's raw tables from a climate-finance release.

The climate-finance package is private, so the pipeline cannot install it. This
script runs where it is installed, opens a release, extracts the three raw tables
the pipeline reads, and writes them with a manifest to an export root, local or
`gs://`, in the layout `dfo_pipelines.extraction.climate_finance` describes. The
pipeline's source asset observes the root's `latest.json`.

Run it with the package at the commit that built the release:

    uv run --with "climate-finance @ git+https://github.com/ONEcampaign/climate-finance-package@<commit>" \\
        python scripts/export_climate_finance_release.py \\
        --release <release directory or .tar.zst pack> --to <export root>
"""

import argparse
import hashlib
import io
import json
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.metadata import distribution
from pathlib import Path
from typing import Literal

import climate_finance as cf
import polars as pl
from climate_finance.release import unpack_release
from climate_finance.schema import sha256_file
from upath import UPath

from dfo_pipelines.extraction.climate_finance import (
    LATEST_FILE_NAME,
    MANIFEST_FILE_NAME,
    ExportedTable,
    ExportManifest,
    LatestExport,
    sha256_bytes,
)

type Family = Literal["received", "provided"]
type Flow = Literal["commitments", "disbursements"]
type MethodName = Literal["ONE", "OECD"]
type Attribution = Literal["contributions", "outflows"]

METHODS: dict[MethodName, cf.Method] = {"ONE": cf.methods.ONE, "OECD": cf.methods.OECD}
FLOWS: tuple[Flow, ...] = ("commitments", "disbursements")

# Received starts with the first year of adaptation markers, provided with the
# first year of the OECD's Table 8 outflow shares. Both end at the release's last
# complete year. `check_covers_limits` refuses a release whose recorded coverage
# differs, so these limits and the year ranges in docs/one/climate_finance/ change
# together.
RECEIVED_YEARS = range(2010, 2025)
PROVIDED_YEARS = range(2013, 2025)

SPENDING_COLUMNS = (
    "year",
    "provider_code",
    "recipient_code",
    "bi_multi",
    "method",
    "attribution",
    "value",
    "adaptation",
    "mitigation",
    "cross_cutting",
    "unsplit",
    "no_adaptation_marker",
)
CRDF_COMMITMENTS_COLUMNS = (
    "year",
    "provider_code",
    "recipient_code",
    "bi_multi",
    "psi_flag",
    "repeat_of",
    "climate_usd",
    "adaptation_usd",
    "mitigation_usd",
    "overlap_usd",
)


@dataclass(frozen=True, kw_only=True)
class SpendingQuery:
    """One `Release.spending` call and the family its rows belong to."""

    family: Family
    flow: Flow
    method: MethodName
    attribution: Attribution

    @property
    def view(self) -> Literal["recipient", "provider"]:
        return "recipient" if self.family == "received" else "provider"

    @property
    def years(self) -> range:
        return RECEIVED_YEARS if self.family == "received" else PROVIDED_YEARS


def spending_queries() -> list[SpendingQuery]:
    """Received under each flow and method, attributed by spending share, and
    provided under each flow, method and attribution rule."""
    received = [
        SpendingQuery(
            family="received", flow=flow, method=method, attribution="contributions"
        )
        for flow in FLOWS
        for method in METHODS
    ]
    provided = [
        SpendingQuery(family="provided", flow=flow, method=method, attribution=rule)
        for flow in FLOWS
        for method in METHODS
        for rule in ("contributions", "outflows")
    ]
    return [*received, *provided]


def spending(release: cf.Release) -> pl.DataFrame:
    """Every query of `spending_queries`, one row per family, flow, method,
    attribution rule, year, provider, recipient and `bi_multi`.

    Rows and amounts are the package's, in USD at current prices, with no EBRD
    aggregate disbursement estimates. Received is the recipient view: bilateral
    flows and multilaterals' own outflows. Provided is the provider view, whose
    core contributions to multilaterals carry `bi_multi` 2. Rows with no provider
    (attributed value no developed contributor takes) and with no recipient (core
    contributions with no outflows to spread over) stay in.

    Raises:
        ValueError: when a result's `method` or `attribution` column differs from
            the query.
    """
    frames = []
    for query in spending_queries():
        table = release.spending(
            flow=query.flow,
            years=query.years,
            view=query.view,
            method=METHODS[query.method],
            attribution=query.attribution,
            currency="USD",
            prices="current",
            aggregate_estimates=False,
        ).table
        for column, expected in (
            ("method", query.method),
            ("attribution", query.attribution),
        ):
            if held := set(table[column].unique()) - {expected}:
                raise ValueError(
                    f"{query}: result {column} holds {sorted(held)}, "
                    f"expected {expected!r}"
                )
        frames.append(
            table.select(
                pl.lit(query.family).alias("family"),
                pl.lit(query.flow).alias("flow"),
                *SPENDING_COLUMNS,
            )
        )
    return pl.concat(frames)


def crdf_commitments(release: cf.Release) -> pl.DataFrame:
    """The release's `commitments` table, the OECD's recipient-perspective CRDF
    commitments as the package repairs them, every row of the received years."""
    return (
        release.table("commitments")
        .filter(pl.col("year").is_in(list(RECEIVED_YEARS)))
        .select(CRDF_COMMITMENTS_COLUMNS)
        .collect()
    )


def btr_totals(release: cf.Release) -> pl.DataFrame:
    """The release's `btr_totals` table, the bilateral climate finance each party
    reports in its Biennial Transparency Report, unchanged."""
    return release.table("btr_totals").collect()


# Each raw table's extract by the stem of its parquet file, in `RAW_TABLES` order.
EXTRACTS: dict[str, Callable[[cf.Release], pl.DataFrame]] = {
    "spending": spending,
    "crdf_commitments": crdf_commitments,
    "btr_totals": btr_totals,
}


def package_source_sha256() -> str:
    """The sha256 of the installed climate_finance package's files.

    The same rule as `climate_finance.build.provenance.source_sha256`, which needs
    the package's build extra: the path and sha256 of every file, compiled bytecode
    and hidden files left out. A release records the digest of the code that built
    it as `build.package_source_sha256`.
    """
    root = Path(cf.__file__).parent
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        skipped = any(
            part == "__pycache__" or part.startswith(".") for part in relative.parts
        )
        if path.is_file() and not skipped:
            digest.update(f"{relative.as_posix()}\0{sha256_file(path)}\n".encode())
    return digest.hexdigest()


def package_commit() -> str | None:
    """The git commit the installed package was built from, if installed from git."""
    direct_url = distribution("climate-finance").read_text("direct_url.json")
    if not direct_url:
        return None
    return json.loads(direct_url).get("vcs_info", {}).get("commit_id")


def check_built_by_installed_package(release: cf.Release) -> None:
    """Refuse a release that another version of the package built.

    The package weighs a release's rows when the extracts ask for them, so the
    figures are the release's only under the code that built it.

    Raises:
        ValueError: If the release's `build.package_source_sha256` differs from
            the installed package's.
    """
    built_by = release.manifest.build.package_source_sha256
    if built_by != (installed := package_source_sha256()):
        raise ValueError(
            f"Release {release.manifest.release} was built by climate_finance "
            f"source {built_by}, but the installed package's source is {installed}; "
            "install the package at the commit that built the release"
        )


def check_covers_limits(release: cf.Release) -> None:
    """Refuse a release whose years differ from the limits the extracts apply.

    The spending queries and the CRDF filter stop at `RECEIVED_YEARS` and
    `PROVIDED_YEARS`, so a release that goes further would publish without its
    last years, and one that stops short would publish less than the docs claim.

    Raises:
        ValueError: If the release's `years.crs_last_complete` setting or the end
            of its `crdf_recipient` year range differs from the last received
            year, or the end of its `crdf_provider` year range from the last
            provided year, or if it lacks any of the three.
    """
    manifest = release.manifest
    years = manifest.years
    recorded = {
        "settings years.crs_last_complete": (
            manifest.settings.get("years.crs_last_complete"),
            RECEIVED_YEARS[-1],
        ),
        "years crdf_recipient end": (
            years.get("crdf_recipient", (None, None))[1],
            RECEIVED_YEARS[-1],
        ),
        "years crdf_provider end": (
            years.get("crdf_provider", (None, None))[1],
            PROVIDED_YEARS[-1],
        ),
    }
    if mismatches := [
        f"{name} is {found}, expected {limit}"
        for name, (found, limit) in recorded.items()
        if found != limit
    ]:
        raise ValueError(
            f"Release {manifest.release} does not cover the export's years: "
            f"{'; '.join(mismatches)}; RECEIVED_YEARS and PROVIDED_YEARS and the "
            "year ranges in docs/one/climate_finance/ change together"
        )


def read_latest(to: UPath) -> LatestExport | None:
    """The `to/latest.json` pointer, or `None` where none is published."""
    if not (to / LATEST_FILE_NAME).exists():
        return None
    return LatestExport.model_validate_json((to / LATEST_FILE_NAME).read_bytes())


def check_newer_than_latest(release: cf.Release, *, to: UPath) -> None:
    """Refuse a release no newer than the one `to/latest.json` names.

    The pipeline publishes whatever `latest.json` names, so pointing it at an
    older release would republish older figures. Releases are ordered by when
    they were built, as each manifest's `created_at` records.

    Raises:
        ValueError: If `to/latest.json` names a release built at the same time
            as `release` or later.
    """
    if (latest := read_latest(to)) is None:
        return
    current = ExportManifest.model_validate_json(
        (to / latest.release / MANIFEST_FILE_NAME).read_bytes()
    )
    if release.manifest.created_at <= current.created_at:
        raise ValueError(
            f"Release {release.manifest.release}, built {release.manifest.created_at}, "
            f"is no newer than {current.release}, built {current.created_at}, which "
            f"{to / LATEST_FILE_NAME} names; edit latest.json by hand to roll back"
        )


def is_published(manifest_bytes: bytes, *, release: str, to: UPath) -> bool:
    """Whether `to/latest.json` names `release` with the sha256 of the manifest."""
    latest = read_latest(to)
    return latest == LatestExport(
        release=release, manifest_sha256=sha256_bytes(manifest_bytes)
    )


def write_export(
    release: cf.Release, *, directory: UPath, pack_sha256: str | None
) -> tuple[ExportManifest, bytes]:
    """Write the raw tables of `release` and then their manifest to `directory`,
    and return the manifest with the bytes written."""
    directory.mkdir(parents=True, exist_ok=True)
    tables = {}
    for stem, extract in EXTRACTS.items():
        frame = extract(release)
        buffer = io.BytesIO()
        frame.write_parquet(buffer)
        data = buffer.getvalue()
        (directory / f"{stem}.parquet").write_bytes(data)
        tables[stem] = ExportedTable(sha256=sha256_bytes(data), rows=frame.height)

    manifest = ExportManifest(
        release=release.manifest.release,
        created_at=release.manifest.created_at,
        release_json_sha256=sha256_file(release.path / "release.json"),
        pack_sha256=pack_sha256,
        package_commit=package_commit(),
        package_source_sha256=package_source_sha256(),
        exported_at=datetime.now(UTC),
        tables=tables,
    )
    manifest_bytes = (manifest.model_dump_json(indent=2) + "\n").encode()
    (directory / MANIFEST_FILE_NAME).write_bytes(manifest_bytes)
    return manifest, manifest_bytes


def read_export(
    release: cf.Release, *, directory: UPath
) -> tuple[ExportManifest, bytes] | None:
    """The manifest in `directory` and its bytes, or `None` where there is none,
    once it and its tables are shown to be this release's.

    Raises:
        ValueError: If the manifest records another `release.json` than the
            release's, or a table file it lists is missing or differs from its
            recorded sha256.
    """
    if not (directory / MANIFEST_FILE_NAME).exists():
        return None
    manifest_bytes = (directory / MANIFEST_FILE_NAME).read_bytes()
    manifest = ExportManifest.model_validate_json(manifest_bytes)
    if manifest.release_json_sha256 != sha256_file(release.path / "release.json"):
        raise ValueError(
            f"{directory} holds an export of {manifest.release} made from another "
            "release.json; delete it to export again"
        )
    for stem, table in manifest.tables.items():
        path = directory / f"{stem}.parquet"
        if not path.exists() or sha256_bytes(path.read_bytes()) != table.sha256:
            raise ValueError(
                f"{path} is missing or differs from the sha256 the manifest "
                "records; delete the export to export again"
            )
    return manifest, manifest_bytes


def export_release(
    release: cf.Release, *, to: UPath, pack_sha256: str | None = None
) -> ExportManifest:
    """Write the raw tables of `release` and their manifest to `to/<release>/`,
    then point `to/latest.json` at them.

    Where `to/<release>/` already holds a manifest, as after an export cut short
    before `latest.json`, the tables and manifest are checked against the release
    and left as they are, and only `latest.json` is written, where it does not
    already name them.

    Raises:
        ValueError: If the installed package did not build the release, the
            release's years differ from `RECEIVED_YEARS` and `PROVIDED_YEARS`,
            `to/latest.json` names a release no older than it, or an existing
            export in `to/<release>/` is not of this release (another
            `release.json`, or a table file missing or altered).
    """
    check_built_by_installed_package(release)
    check_covers_limits(release)
    name = release.manifest.release
    directory = to / name
    existing = read_export(release, directory=directory)
    if existing and is_published(existing[1], release=name, to=to):
        return existing[0]
    check_newer_than_latest(release, to=to)
    manifest, manifest_bytes = existing or write_export(
        release, directory=directory, pack_sha256=pack_sha256
    )
    latest = LatestExport(release=name, manifest_sha256=sha256_bytes(manifest_bytes))
    (to / LATEST_FILE_NAME).write_text(latest.model_dump_json(indent=2) + "\n")
    return manifest


def export(release: Path, *, to: UPath) -> ExportManifest:
    """Open the release directory or `.tar.zst` pack at `release` and export it."""
    if release.is_dir():
        return export_release(cf.load(path=release), to=to)
    with tempfile.TemporaryDirectory() as unpacked:
        root = unpack_release(release, into=Path(unpacked), source=release)
        return export_release(
            cf.load(path=root), to=to, pack_sha256=sha256_file(release)
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--release",
        type=Path,
        required=True,
        help="a release directory, such as a build's assemble/, or a .tar.zst pack",
    )
    parser.add_argument(
        "--to", required=True, help="the export root, a local directory or gs:// URI"
    )
    arguments = parser.parse_args()
    manifest = export(arguments.release, to=UPath(arguments.to))
    rows = ", ".join(f"{stem} {table.rows}" for stem, table in manifest.tables.items())
    print(f"Exported {manifest.release} to {arguments.to}: {rows}")


if __name__ == "__main__":
    main()
