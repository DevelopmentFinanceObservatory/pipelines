from pathlib import Path

import dagster as dg

from dfo_pipelines.defs.dfo.shared.assets import (
    dfo_shared_dc_export,
)
from dfo_pipelines.resources.file_store import UPathFileStoreResource
from tests.helpers import exported_directory, meta, warehouse_with_models

LICENSE_TYPES_SQL = """
create or replace table dfo_shared.license_types as
select * from (values
    (
        'dcid:TestLicense',
        'Test License',
        'Data available under the Test License.',
        'https://example.org/terms'
    )
) as license_types (dcid, name, description, url)
"""

PROPERTIES_SQL = """
create or replace table dfo_shared.properties as
select * from (values
    (
        'dcid:testProperty',
        'test property',
        'A property for the test.',
        'dcid:Provenance',
        'schema:Text'
    )
) as properties (dcid, name, description, domain, range)
"""


# The seeds the export reads, with one row in each
SEEDS_SQL = (
    "create schema if not exists dfo_shared",
    LICENSE_TYPES_SQL,
    PROPERTIES_SQL,
)


def test_dc_export_writes_a_node_for_each_seed_row(tmp_path: Path) -> None:
    """Test each seed row becomes a licence type or property node under the import's provenance"""
    result = dfo_shared_dc_export(
        context=dg.build_asset_context(),
        warehouse=warehouse_with_models(tmp_path, *SEEDS_SQL),
        export_file_store=UPathFileStoreResource(base_path=str(tmp_path / "processed")),
    )

    assert isinstance(result, dg.MaterializeResult)
    out_dir = exported_directory(result)
    assert (out_dir / "custom_nodes.mcf").read_text().strip() == (
        "Node: dcid:TestLicense\n"
        'name: "Test License"\n'
        "typeOf: dcid:LicenseTypeEnum\n"
        'description: "Data available under the Test License."\n'
        'url: "https://example.org/terms"\n'
        "\n"
        "Node: dcid:testProperty\n"
        'name: "test property"\n'
        "typeOf: dcid:Property\n"
        'description: "A property for the test."\n'
        "domainIncludes: dcid:Provenance\n"
        "rangeIncludes: schema:Text\n"
        "isProvisional: schema:True"
    )
    provenance = (out_dir / "provenance.mcf").read_text()
    assert "Node: dcid:provenance/DFOShared\n" in provenance
    assert meta(result, "license_type_count", dg.IntMetadataValue).value == 1
    assert meta(result, "property_count", dg.IntMetadataValue).value == 1
