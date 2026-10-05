from datetime import date
from pathlib import Path
from unittest.mock import patch

from dfo_pipelines.defs.resources import UPathFileStoreResource


def test_upath_file_store_build_date_partitioned_path(tmp_path: Path) -> None:
    """Test the built path joins the base path, the given path and the date"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path))
    path = file_store.build_date_partitioned_path(
        "oecd_dac_crs/dac_crs", partition_date=date(2026, 8, 3)
    )
    assert str(path) == f"{tmp_path}/oecd_dac_crs/dac_crs/2026/08/03"


def test_upath_file_store_build_date_partitioned_path_defaults_to_today(
    tmp_path: Path,
) -> None:
    """Test the partition defaults to today when no date is given"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path))
    with patch("dfo_pipelines.defs.resources.date") as mock_date:
        mock_date.today.return_value = date(2026, 8, 3)
        path = file_store.build_date_partitioned_path("oecd_dac_crs/dac_crs")

    assert str(path) == f"{tmp_path}/oecd_dac_crs/dac_crs/2026/08/03"
