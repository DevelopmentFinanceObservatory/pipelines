## What this dataset measures

A multilateral organisation pools a provider's core contributions and spends them across sectors and
recipients on its own account. This dataset attributes that spending back to the original provider, in
proportion to its share of each multilateral's core resources, and breaks it down by recipient and by
sector. It answers "how much of a provider's money reached this sector in this recipient country, once its
contributions to multilaterals are spread the way those multilaterals spent them", a question the CRS
alone cannot answer. The CRS records a provider's payment to the multilateral, not what the multilateral
did with it.

This is a different question from a bilateral relationship where the provider is itself a multilateral
organisation, such as IDA disbursing directly to Kenya. That spending is already a CRS row under IDA's own
provider code and is not part of this dataset.

## Download the source data

The pipeline downloads two OECD bulk releases: the Creditor Reporting System (CRS), which supplies the
sector mix each multilateral's spending followed, and Members' Total Use of the Multilateral System, which
supplies each provider's core contributions. Both are read through the `oda-data` package, and both are
fingerprinted by the `-vYYYYMMDD` republish token the OECD attaches to each bulk file. `resolve_bulk_versions`
in `src/dfo_pipelines/defs/one/imputed_multilateral_sectors/assets.py` reads both tokens, and
`one_imputed_multilateral_sectors_source` observes them daily so the pipeline reruns when either changes.

`one_imputed_multilateral_sectors_raw_file` calls `imputed_multilateral_by_purpose` with `refresh=True`, so
every materialization re-reads both releases rather than serving a cached copy. `resolve_bulk_versions` is
called again immediately before and immediately after that compute, and `verify_release_provenance` checks
the compute's own `result.attrs["provenance"]` against the token observed before it, then checks that same
token against the one observed after. The run refuses to write, leaving no parquet file and no metadata,
when `oda-data`'s recorded release disagrees with the token observed before the compute, or when either
source's token moves between the two observations: `oda-data` resolves its own release identity through a
7-day HTTP annotation cache, so it can otherwise report a release that no longer matches what was read.

## Exclude the partial preliminary year

Before each full release, the OECD publishes preliminary data for the year that release covers, as reporters
submit it. Until then, that year is partial, so the pipeline excludes it. Support for partial years is in
development. Partial years will be marked incomplete, so they don't lead to incorrect or confusing analyses.

## Compute the imputed spending

For each provider, `oda-data`'s `imputed_multilateral_by_purpose` function takes the provider's core
contributions to each multilateral and spreads them across sectors and recipients in the same proportions
that multilateral's own CRS-reported spending followed over its current three-year window (`period_length`,
default 3). Those shares are computed over CRS spending flagged Official Development Assistance (ODA) or
Other Official Flows (OOF) combined (`flow_types=("ODA", "OOF")`), because several multilateral channels,
among them IBRD, EBRD, IFC and IDB Invest, report only OOF to the CRS; an ODA-only share basis would leave
those channels with no CRS spending to compute a share from. `flow_types=("ODA", "OOF")` affects only the
CRS spending the shares are computed from, not the core-contribution amounts read from Members' Total Use.
Every value is a gross disbursement at current US dollar prices.

An observation is dated to the year the provider made the contribution, not to the years the underlying
sector mix was drawn from. Because that sector mix comes from the three years up to and including the
contribution year, a value published against year n can change if the CRS is later revised for year n-1 or
n-2, even though the contribution itself did not change. Re-running the pipeline against a revised CRS
vintage updates the affected years' values, and nothing in the observation itself records that it moved.

Coverage starts in 2011, the first year Members' Total Use of the Multilateral System reports, even though
the CRS itself runs back to 1973. Every observation is a gross disbursement, so this dataset will not
reconcile against headline grant-equivalent ODA figures, which measure the same flows on a different basis.

## Resolve which channel the money passed through

Each multilateral's CRS spending resolves to a DAC channel code through a crosswalk `oda-data` ships and
maintains under review, keyed on the exact `(provider_code, agency_code)` pair the CRS reports. A
`(provider_code, agency_code)` pair carrying nonzero value with no reviewed row in that crosswalk raises
`UnmappedChannelError` inside `imputed_multilateral_by_purpose`, and nothing in this pipeline catches it: a
new OECD channel appearing in the CRS before it has been reviewed and added to the crosswalk fails the run
loudly.

This dataset ships without a channel dimension. `int_imputed_multilateral_sectors_aggregated.sql` sums over
`channel_code` once entities are resolved, so a user cannot see which multilateral a given amount travelled
through, only the total across all of them for a given provider, recipient, sector and year.

## Allocate every core contribution

Every core contribution `oda-data` reads from Members' Total Use follows exactly one allocation route. A
contribution spread across recipients and purposes gives many output rows, every one of them tagged with
the same `allocation_status` and together summing to the contribution:

- `imputed`: the channel's own CRS spending in its current three-year window produced the share.
- `stale_share`: the channel reported no CRS spending in its current window, so its most recent window with
  data, within `max_share_age` (default 5) years, is used instead.
- `proxy`: a reviewed proxy channel's shares stand in for the channel's own; for example, the Multilateral
  Debt Relief Initiative uses IDA's shares. A fixed purpose code carries no recipient of its own, so a
  fixed-purpose proxy row, for example UN peacekeeping core contributions under purpose code 15230, carries
  a null `recipient_code` instead.
- `unallocated`: no usable share exists, at any age, from the channel itself or a reviewed proxy; both
  `recipient_code` and `purpose_code` are null.

`stg_imputed_multilateral_sectors.sql` keeps rows for all four allocation statuses, so a provider's total
across this dataset always equals its Members' Total Use core contributions. A null `recipient_code`, from a
fixed-purpose proxy row or an `unallocated` row, is published against DAC recipient code 9998, "Bilateral,
unallocated/unspecified"; an `unallocated` row's null `purpose_code` is published against CRS purpose code
998, "Unallocated/unspecified". The `unallocated` share of that year's total core money, reported in
`one_imputed_multilateral_sectors_raw_file`'s materialization metadata under
`unallocated_value_share_by_year`, is a coverage figure over the published values.

[`oda-data` pull request #167](https://github.com/ONEcampaign/oda_data_package/pull/167) measures this
split on CRS release v20260803 and Members' Total Use release v20260710, across 2015-2024: 89.15% of core
money is imputed, 4.04% proxy, 0.21% stale share, and 6.59% unallocated. This pipeline reports those same
four shares for each run in `one_imputed_multilateral_sectors_raw_file`'s materialization metadata.

## Resolve providers and recipients, and select dimensions

Provider and recipient codes are standardised to Data Commons IDs used across the platform, so data can be
compared across datasets. Every figure is reported against the year, the provider, the recipient and the
purpose code. Netting basis, flow type and flow category are constants across the whole dataset (gross
disbursement, ODA) rather than dimensions read from the source, because this is the only combination
`oda-data`'s imputation method produces.

Each series is a `FinancialFlow` StatVar measuring `attributedAmount`, the part of a flow's amount credited
to a provider whose resources it draws on, where the flow cannot be traced to that provider directly. Here
the flow is the multilateral's own spending and the resources are the provider's core contributions. The
StatVar is constrained by `dac2aAidType` `DAC2aAidType106`, which marks this dataset's series,
so its Data Commons IDs differ from those of a CRS series for the same purpose. The IDs order the
constraints `nettingBasis`, `financialFlowType`, `crsTypeOfFlow`, `dac2aAidType`, `crsPurpose`. Every
observation carries `measurementMethod` `Modelled`, since each value is computed from shares.

## Aggregate totals

Activities sharing the same year, provider, recipient and purpose code are summed, across every channel
they passed through. Every figure is reported in US dollars at current prices.

## Cite each source's own release

`dc_imputed_multilateral_sectors_provenance.sql` cites CRS and Members' Total Use separately, each with its
own release year, since the two publish months apart; the leading "ONE (year)" citation uses the later of
the two years. `next_source_release_date` is the earlier of CRS's own next scheduled release, one of April,
June, September or December, and Members' Total Use's next annual release. Members' Total Use derives from
the same CRS activity-level microdata that only becomes final at CRS's December release, so it carries no
April preliminary counterpart the way CRS does, and the pipeline models its own next release as arriving
every December. `next_oecd_release_date` and `next_multisystem_release_date`, in `dbt/macros/`, each compute
their own source's next date from its own last observed release date, and
`dc_imputed_multilateral_sectors_provenance.sql` takes the earlier (`least()`) of the two.

## Combining this dataset with the CRS

A provider total built from both this dataset and the CRS counts each core contribution once.
`stg_dac_crs.sql` excludes CRS rows flagged `bi_multi = 2`, the OECD's own flag for a donor's core
contribution to a multilateral organisation, and keeps rows where the flag is null. Members' Total Use
reports the same contributions, and this dataset spreads them across sectors and recipients, so `oecd_dac_crs`
carries a provider's bilateral spending while this dataset carries the redistributed core spending, with no
overlap between the two.

The B02 family of cooperation-modality codes (`B02`, `B021`, `B022`, `B02a`, `B02b`) is only partly mapped in
`code_mapping`: `B02a` and `B02b` have no ontology mapping, so a CRS row reported under either code has no
cooperation-modality segment in its Data Commons ID. The `bi_multi = 2` exclusion removes any double count
between the two datasets, and the unmapped codes are a completeness gap in the cooperation-modality
dimension of the CRS series.

This dataset restates each provider's own contributions rather than adding new recipient-side information.
The question "how much reached this recipient, from all providers combined" is a recipient-perspective
total, and the CRS alone answers it.
