"""Cut the climate finance test release from a built climate-finance release, and
export it as the pipeline's test export.

Writes `tests/one/climate_finance/fixtures/release`, the cut, and
`tests/one/climate_finance/fixtures/export`, its export by
`export_climate_finance_release.py`, which the pipeline's tests read in place of the
export in GCS.

The cut keeps 2021 and 2022 for these providers:

- Denmark (3), a developed provider whose core contributions count under both
  attribution rules, with CRDF repeat and memorandum rows and value with no
  adaptation marker.
- Luxembourg (22), a developed provider whose BTR totals report committed and
  disbursed finance, three funding sources, and rows with no status.
- Indonesia (738), whose BTR totals have no face value, and no other rows.
- IDA (905), a multilateral whose outflows the contributions spread over.
- The provider 1045, whose Table 8 share no developed provider in the release
  contributed to, so its attributed outflows have no provider.
- The Gates Foundation (1601), a private foundation the CRDF commitments hold.

Core contributions to every other channel find no outflows in the cut, so they
stay without a recipient.

Tables with a provider keep those providers' rows, and tables with a year those
years'. `commitments` also keeps the earlier commitments the kept disbursements
link to, whose markers they take. `objectives` keeps the rows of the records the
other tables keep, `developed_providers` every row, and `reflows` none, since only
net flows read it. As in a published pack, the row-level BTR tables move from
`tables` to `withheld_tables`. `release.json` keeps the source's other fields, with
each table's sha256 and row count rewritten, so the package opens the cut as it
opens the source.

Run from the repository root with the source release directory, such as a build's
`assemble/`, and the package at the commit that built it:

    uv run --with "climate-finance @ git+https://github.com/ONEcampaign/climate-finance-package@<commit>" \\
        python scripts/cut_climate_finance_fixture.py <release directory>
"""

import hashlib
import json
import shutil
import sys
from pathlib import Path

import climate_finance as cf
import polars as pl
from export_climate_finance_release import export_release
from upath import UPath

PROVIDERS = (3, 22, 738, 905, 1045, 1601)
YEARS = (2021, 2022)
WITHHELD_TABLES = dict.fromkeys(
    ("btr", "btr_links", "btr_multilateral"),
    "Left out of the published release, as the package's packs leave it out.",
)
FIXTURES = Path(__file__).parents[1] / "tests" / "one" / "climate_finance" / "fixtures"
RELEASE = FIXTURES / "release"
EXPORT = FIXTURES / "export"


def _cut(name: str, frame: pl.DataFrame) -> pl.DataFrame:
    if name == "reflows":
        return frame.clear()
    if "provider_code" in frame.columns and name != "developed_providers":
        frame = frame.filter(pl.col("provider_code").is_in(PROVIDERS))
    if "year" in frame.columns:
        frame = frame.filter(pl.col("year").is_in(YEARS))
    return frame


def cut_release(source: Path) -> None:
    """Write the cut of the release in `source` to `fixtures/release`."""
    manifest = json.loads((source / "release.json").read_text())
    names = sorted(set(manifest["tables"]) - set(WITHHELD_TABLES))
    frames = {
        name: _cut(name, pl.read_parquet(source / f"{name}.parquet"))
        for name in names
        if name != "objectives"
    }
    # Disbursements take their markers from the commitments they link to, which
    # are often of earlier years.
    frames["commitments"] = pl.concat(
        [
            frames["commitments"],
            pl.read_parquet(source / "commitments.parquet").filter(
                ~pl.col("year").is_in(YEARS),
                pl.col("row_id").is_in(frames["disbursements"]["row_id"].implode()),
            ),
        ]
    )
    record_ids = pl.concat(
        [
            frame["record_id"].cast(pl.String)
            for frame in frames.values()
            if "record_id" in frame.columns
        ]
    ).unique()
    frames["objectives"] = pl.read_parquet(source / "objectives.parquet").filter(
        pl.col("record_id").is_in(record_ids.implode())
    )

    shutil.rmtree(RELEASE, ignore_errors=True)
    RELEASE.mkdir(parents=True)
    tables = {}
    for name in sorted(frames):
        path = RELEASE / f"{name}.parquet"
        frames[name].write_parquet(path)
        tables[name] = {
            **manifest["tables"][name],
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "rows": frames[name].height,
        }
    manifest["tables"] = tables
    manifest["withheld_tables"] = WITHHELD_TABLES
    (RELEASE / "release.json").write_text(json.dumps(manifest, indent=2) + "\n")


def export_cut() -> None:
    """Write the export of the cut to `fixtures/export`."""
    shutil.rmtree(EXPORT, ignore_errors=True)
    export_release(cf.load(path=RELEASE), to=UPath(EXPORT))


if __name__ == "__main__":
    cut_release(Path(sys.argv[1]))
    export_cut()
