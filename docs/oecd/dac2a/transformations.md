## Download the source data

The OECD publishes DAC2A in April, June, September and December. The complete table is
downloaded from the OECD each time it is republished, and replaces the previous copy,
so the data reflects the OECD's latest release.

## Mark the partial year

The OECD publishes data for a year as reporters submit it, and its final release in
December of the following year covers every reporter. Until then, only some donors
have reported, so the year is partial. The pipeline publishes it and marks it. The
import's provenance names the year in `partialObservationDate`, in the same format as
`latestObservationDate`. The mark stays until the pipeline holds a file dated on or
after 1 December of the following year. The property is then left off.

Before publishing, the pipeline checks the mark against the data. Among the three years
ending at the newest, the marked year must have fewer than half as many donors reporting
as the best-covered year, and every other year must have at least half. When the
calendar and the data disagree, the build fails.

## Standardise providers and recipients

Provider and recipient codes are standardised to Data Commons IDs used across the
platform, so data can be compared across datasets. The totals DAC2A reports for groups
of providers, such as all DAC countries, and for groups of recipients, such as regions
and income groups, are included as providers and recipients of their own. Where two
provider codes name the same organisation and report the same figures, the figures are
counted once. Where two recipient codes name the same organisation, such as IDA and
IDA's Multilateral Debt Relief Initiative, their flows are added.

## Select figures

Only figures in current US dollars are included, not constant US dollars. Only flows to
recipients on Part I of the DAC List are included: the developing countries, and the
multilateral organisations DAC2A reports as recipients, such as development banks and UN
agencies. Flows to Part II recipients, the countries in transition, are excluded.

## Convert to whole units

DAC2A reports amounts in millions, which are converted to whole units. ODA per person
and ODA as a share of GNI are kept as reported.

## Select dimensions

Each figure is reported by year, provider, recipient and type of aid, such as grants,
repayments, interest received, or debt forgiven or rescheduled. DAC2A is a table of
disbursements, so every figure, repayments and interest included, is recorded as a
disbursement. Figures are marked as gross or net, except where DAC2A does not say which,
and as grants or non-grants where the type of aid is wholly one or the other. Imputed
multilateral ODA, the OECD's estimate of each provider's share of multilateral
organisations' ODA, is marked as modelled. ODA as a share of GNI records the recipient's
GNI as its denominator, and ODA per person records the recipient's population.
