import logging
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

from dfo_pipelines.resources.file_store import UPathFileStoreResource


def test_upath_file_store_names_a_write_once_directory_by_its_utc_instant_and_run(
    tmp_path: Path,
) -> None:
    """Test a directory created at an instant given in another zone is named by
    that instant in UTC, then the run"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path))
    created_at = datetime(2026, 8, 4, 1, 47, 5, tzinfo=timezone(timedelta(hours=2)))

    with file_store.write_once_directory(
        "oecd_dac1/dac1",
        created_at=created_at,
        run_id="run-a",
        log=logging.getLogger(__name__),
    ) as directory:
        pass

    assert str(directory) == f"{tmp_path}/oecd_dac1/dac1/20260803T234705Z_run-a"


def test_upath_file_store_writes_runs_in_the_same_second_to_separate_directories(
    tmp_path: Path,
) -> None:
    """Test two runs at one instant get different directories, and the second leaves
    the first's files alone"""
    file_store = UPathFileStoreResource(base_path=str(tmp_path))
    created_at = datetime(2026, 8, 3, 23, 47, 5, tzinfo=UTC)
    log = logging.getLogger(__name__)

    with file_store.write_once_directory(
        "dac1", created_at=created_at, run_id="run-a", log=log
    ) as first:
        (first / "earlier.txt").write_text("earlier")
    with file_store.write_once_directory(
        "dac1", created_at=created_at, run_id="run-b", log=log
    ) as second:
        pass

    assert first != second
    assert [path.name for path in first.iterdir()] == ["earlier.txt"]
