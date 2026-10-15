## Download the source data

The complete CRS file is downloaded from the OECD each time the pipeline runs, and
replaces the previous copy, so the data reflects the OECD's latest release.

## Exclude the partial preliminary year

Before each full release, the OECD publishes preliminary data for the year that release
covers, as reporters submit it. Until then, that year is partial, so the pipeline
excludes it.

## Exclude donor core contributions to multilaterals

The OECD instructs that donor core contributions to multilateral organisations,
reported as ordinary CRS rows flagged `bi_multi = 2`, are excluded to compute
bilateral flows. These contributions are already reported in Members' Total Use of
the Multilateral System, so keeping them would double count.

## Standardise providers and recipients

Provider and recipient codes are standardised to Data Commons IDs used across the
platform, so data can be compared across datasets.

## Select dimensions

Every figure is reported against the year, the provider and the recipient, and four
dimensions from the CRS: type of flow, co-operation modality, type of finance and
purpose code. The CRS describes each activity in far more detail than this, including
project titles, descriptions, channels and policy markers, none of which is carried
through.

## Aggregate totals

Activities sharing the same year, provider, recipient and dimensions are summed.
Commitments, disbursements, net disbursements and grant equivalents are each
aggregated as separate measures. Every figure is reported in US dollars at current
prices.
