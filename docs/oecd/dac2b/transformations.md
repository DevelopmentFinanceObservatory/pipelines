## Download the source data

The OECD publishes DAC2B in April, June, September and December. The complete table is
downloaded from the OECD each time it is republished, and replaces the previous copy,
so the data reflects the OECD's latest release.

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

## Standardise providers and recipients

Provider and recipient codes are standardised to Data Commons IDs used across the
platform, so data can be compared across datasets. The totals DAC2B reports for groups
of providers, such as all DAC countries, and for groups of recipients, such as regions
and income groups, are included as providers and recipients of their own. Where two
provider codes name the same organisation and report the same figures, the figures are
counted once.

## Select figures

Only figures in current US dollars are included, not constant US dollars. Only flows to
recipients on Part I of the DAC List are included: the developing countries, and the
multilateral organisations DAC2B reports as recipients, such as development banks. Flows
to Part II recipients, the countries in transition, are excluded.

## Convert to whole units

DAC2B reports amounts in millions, which are converted to whole units.

## Select dimensions

Each figure is reported by year, provider, recipient and type of flow, such as other
official flows, officially supported export credits or interest received. Figures are
marked as gross or net, and as grants or non-grants where the type of flow is wholly one
or the other.
