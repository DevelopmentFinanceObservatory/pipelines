## Download the source data

The complete CRS file is downloaded from the OECD each time the pipeline runs, and
replaces the previous copy, so the data reflects the OECD's latest release.

## Mark the partial preliminary year

Before each full release, the OECD publishes preliminary data for the year that release
covers, as reporters submit it. Until then, that year is partial. The pipeline publishes
it and marks it. The import's provenance names the year in `partialObservationDate`, in
the same format as `latestObservationDate`. The mark stays until the pipeline holds a
file dated on or after 1 December of the following year, the December release that
completes the year. The property is then left off.

Before publishing, the pipeline checks the mark against the data. Among the three years
ending at the newest, the marked year must have fewer than half as many donors reporting
as the best-covered year, and every other year must have at least half. When the
calendar and the data disagree, the build fails.

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
