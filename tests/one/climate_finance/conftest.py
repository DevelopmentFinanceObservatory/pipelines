from pathlib import Path
from typing import Any

import pytest

FIXTURES = Path(__file__).parent / "fixtures"
# A cut of release 2026a-rc9 and its export, both written by
# scripts/cut_climate_finance_fixture.py.
RELEASE = FIXTURES / "release"
EXPORT = FIXTURES / "export"


@pytest.fixture(scope="session")
def release() -> Any:
    """The cut, opened by the climate-finance package, which CI does not install"""
    cf = pytest.importorskip("climate_finance")
    return cf.load(path=RELEASE)


@pytest.fixture(scope="session")
def raw_tables(release: Any) -> dict[str, Any]:
    """Every raw table extracted from the cut, once per session. The package's
    provided disbursements under the outflows rule take about 15 s on any release,
    however small."""
    cf = pytest.importorskip("climate_finance")
    from scripts.export_climate_finance_release import EXTRACTS

    with pytest.warns(cf.errors.CoverageWarning):
        return {stem: extract(release) for stem, extract in EXTRACTS.items()}
