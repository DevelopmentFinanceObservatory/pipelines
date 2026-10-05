"""Every pipeline's Data Commons export, for code that resolves a provenance.

Each pipeline defines its `ExportSpec` beside its assets; this tuple gathers them
so a consumer can look up the provenance an export registers. A test checks every
module-level `ExportSpec` under `dfo_pipelines.defs` is listed here.
"""

from dfo_pipelines.data_commons.export import ExportSpec
from dfo_pipelines.defs.imf.weo.assets import IMF_WEO_EXPORT_SPEC
from dfo_pipelines.defs.oecd.dac1.assets import OECD_DAC1_EXPORT_SPEC
from dfo_pipelines.defs.oecd.dac_crs.assets import CRS_EXPORT_SPEC
from dfo_pipelines.defs.one.conversion_tables.assets import (
    DAC_EXPORT_SPEC,
    IMF_EXPORT_SPEC,
)
from dfo_pipelines.defs.one.imputed_multilateral_sectors.assets import (
    IMPUTED_MULTILATERAL_SECTORS_EXPORT_SPEC,
)
from dfo_pipelines.defs.world_bank.ids.assets import IDS_EXPORT_SPEC

EXPORT_SPECS: tuple[ExportSpec, ...] = (
    IMF_WEO_EXPORT_SPEC,
    CRS_EXPORT_SPEC,
    OECD_DAC1_EXPORT_SPEC,
    DAC_EXPORT_SPEC,
    IMF_EXPORT_SPEC,
    IMPUTED_MULTILATERAL_SECTORS_EXPORT_SPEC,
    IDS_EXPORT_SPEC,
)
