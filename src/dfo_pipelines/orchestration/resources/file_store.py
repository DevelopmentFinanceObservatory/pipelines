import logging
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime

from upath import UPath

from dfo_pipelines.orchestration.resources.base import StrictConfigurableResource


class UPathFileStoreResource(StrictConfigurableResource):
    """File store for UPath backends"""

    base_path: str

    @contextmanager
    def write_once_directory(
        self, path: str, *, created_at: datetime, run_id: str, log: logging.Logger
    ) -> Iterator[UPath]:
        """Create `<base_path>/<path>/<created_at in UTC>_<run_id>` for the block to
        write, and remove it if the block raises

        The run ID keeps runs that start in the same second apart.
        """
        directory = (
            UPath(self.base_path)
            / path
            / f"{created_at.astimezone(UTC).strftime('%Y%m%dT%H%M%SZ')}_{run_id}"
        )
        directory.mkdir(parents=True)
        try:
            yield directory
        except BaseException:
            # Raise the failure that stopped the write, not one from cleaning up.
            try:
                directory.rmdir(recursive=True)
            except Exception as error:
                log.warning(f"Could not remove {directory}: {error}")
            raise
