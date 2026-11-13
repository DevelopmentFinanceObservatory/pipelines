from pathlib import Path

from dfo_pipelines.data_commons.export import ExportSpec, ObservationInput

# A StatVar table shaped like the CRS and DAC models: the fixed columns, the two
# list columns, one column per constraint property, a name, and a bookkeeping
# column the export ignores. The second row leaves its purpose code null and its
# denominator an empty string.
STATVARS_SQL = """
create or replace table checked.statvars as
select * from (values
    (
        'dcid:Variable1', 'Variable One', 'dcid:FinancialFlow', 'dcid:amount',
        'dcid:measuredValue', 'dcid:Nominal', null,
        'nettingBasis,crsPurpose', 'dcid:Gross', 'dcid:CRSPurposeCode12220',
        'financingProvider,financingRecipient', true
    ),
    (
        'dcid:Variable2', 'Variable Two', 'dcid:FinancialFlow', 'dcid:amount',
        'dcid:measuredValue', 'dcid:Nominal', '',
        'nettingBasis,crsPurpose', 'dcid:Net', null,
        null, false
    )
) as statvars (
    dcid, name, population_type, measured_property, stat_type,
    measurement_qualifier, measurement_denominator, constraint_properties,
    netting_basis, purpose_code, observation_properties, has_provider
)
"""

SPEC = ExportSpec(
    import_name="checked",
    provenance_dcid="Provenance1",
    provenance_model="checked.provenance",
    conversion_recipe=None,
    statvar_model="checked.statvars",
    observation_inputs=(
        ObservationInput(model="checked.observations", property_to_column={}),
    ),
    statvar_column_to_property={
        "name": "name",
        "netting_basis": "nettingBasis",
        "purpose_code": "crsPurpose",
        "has_provider": "hasProvider",
    },
    statvar_ignore_columns=("has_provider",),
    docs_dir=Path("unused"),
)
