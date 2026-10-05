{#
    The ontology is the authority on any code it publishes, so a mapping curated
    in a seed is dropped once the export carries the same one. The interim seed
    holds mappings only until the ontology adopts their nodes; the ids seed holds
    codes the ontology leaves to the pipeline, so its rows are permanent but wait
    dormant while the export still carries them.
#}
with exported as (
    select
        list,
        code,
        dcid,
        label,
        property
    from {{ source("dfo_ontology", "raw_code_mapping") }}
),

curated as (
    select
        list,
        code,
        dcid,
        label,
        property
    from {{ ref("interim_code_mapping") }}

    union all

    select
        list,
        code,
        dcid,
        label,
        property
    from {{ ref("ids_code_mapping") }}
),

not_carried_by_the_export as (
    select
        curated.list,
        curated.code,
        curated.dcid,
        curated.label,
        curated.property
    from curated
    left join exported
        on
            curated.list = exported.list
            and curated.property = exported.property
            and curated.code = exported.code
    where exported.code is null
)

select * from exported

union all

select * from not_carried_by_the_export
