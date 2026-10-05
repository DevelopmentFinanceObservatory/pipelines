{% test supersedes_interim_mappings(model, exported, interim) %}
{#
    Mappings the export now publishes that are still curated by hand.

    Reads its two inputs rather than the model it is declared on, because the
    model has already dropped the rows it reports. It is declared on that model
    so a refreshed export is what evaluates it, and so it reaches Dagster as a
    check there: the seed is static between deployments, and a test declared on
    a source does not become one at all.
#}
select
    interim.list,
    interim.code,
    interim.dcid
from {{ exported }} as exported
inner join {{ interim }} as interim
    on
        exported.list = interim.list
        and exported.property = interim.property
        and exported.code = interim.code
{% endtest %}
