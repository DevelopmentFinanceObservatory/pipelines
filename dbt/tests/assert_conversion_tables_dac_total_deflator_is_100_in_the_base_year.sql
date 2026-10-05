{#-
    The DAC publishes constant prices of one base year, so its own total deflator,
    100 x current / constant ODA for DAC Countries, Total, equals 100 in that year.
    This reads the deflator as DAC1 publishes it, before int_conversion_tables_dac_deflators
    rebases it, so it fails when the DAC moves its base year and the
    conversion_tables_deflator_base_year var lags: the published total then reads
    some other value in the declared year. A test on the rebased table would pass
    by construction.

    DAC1 rounds amounts to hundredths of a million, which moves the total's
    deflator by under 1e-5, so 0.01 is a tolerance rounding never reaches and any
    change of base year exceeds. A missing base-year row fails too.
-#}
with base_year_total as (
    select deflator
    from {{ ref("int_conversion_tables_dac_deflators_unrebased") }}
    where
        entity = 'dcid:DACCountriesTotal'
        and year = {{ var("conversion_tables_deflator_base_year") }}
)

select 'no DAC Countries, Total deflator in the base year' as failure
where not exists (select 1 from base_year_total)

union all

select
    'DAC Countries, Total deflator is ' || cast(deflator as varchar)
    || ' in the base year' as failure
from base_year_total
where abs(deflator - 100) > 0.01
