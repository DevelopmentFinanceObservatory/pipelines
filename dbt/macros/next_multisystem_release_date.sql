{% macro next_multisystem_release_date(release_date) -%}
    {#-
        Members' Total Use of the Multilateral System has no April preliminary
        release the way CRS does: it derives from CRS activity-level microdata
        that is only final once CRS's December release lands, so it publishes
        once a year, around December, alongside that CRS final release. This
        is that December, rolling into December of the following year once it
        has been published.
    -#}
    case
        when extract(month from {{ release_date }}) < 12
            then cast(extract(year from {{ release_date }}) as varchar) || '-12'
        else cast(extract(year from {{ release_date }}) + 1 as varchar) || '-12'
    end
{%- endmacro %}
