{#-
    The OECD's commitments are a second facet of ONE Data's received
    commitment StatVars, and each import defines the StatVars its observations
    use. An ingest writes each import's copy of a node under its own
    provenance, so the two copies must match byte for byte, or one StatVar
    would carry two names or two populations. Any OECD StatVar row that is not
    also a ONE Data StatVar row fails.
-#}
select * from {{ ref("dc_oecd_crdf_statvars") }}
except
select * from {{ ref("dc_one_climate_finance_statvars") }}
