## What this dataset measures

ONE Data's climate finance package estimates how much climate finance each provider committed and paid
out, by recipient and year. The OECD publishes climate finance mostly as commitments. The package links
each payment in the OECD's Creditor Reporting System (CRS) to the commitment it pays, by project
identifiers or statistically by project numbers, titles and dates, and gives the payment that
commitment's climate share.

The dataset holds two kinds of figures, both as commitments and as gross disbursements.

- Received figures, measured as `amount`, are each provider's own climate finance by recipient, from
  2010 to 2024. Providers are the official providers that report to the CRS, DAC members and others
  such as Türkiye and the United Arab Emirates, along with EU Institutions, development banks and
  climate funds, and a bank's lending counts under the bank.
- Provided figures, measured as `attributedAmount`, credit each donor with its share of what development
  banks and climate funds commit and pay out, by recipient, from 2013 to 2024.

Two rules work out a donor's share of an institution's climate finance. Spending share multiplies the donor's core contributions to the institution by the institution's climate share, taken
from its commitments over the year and the year before, or from the OECD where the institution reports
no CRS data. Funding share uses the shares the OECD attributes to developed countries in its count
towards the USD 100bn goal. Value the OECD credits to no developed country is left out.

## Weighting the Rio markers

Donors mark each project with a Rio marker for adaptation and one for mitigation, as principal when the
objective is the main aim, significant when it is one aim among others. A marker says whether a project
serves an objective, and a weighting turns it into the share of the project counted as climate finance.
Every figure comes under two weightings.

- ONE Data's marker-score weighting uses the coefficients each donor applies when it
  reports to the UNFCCC, else the shares its Biennial Transparency Report implies, else 40% of a
  significant project and 100% of a principal one. A project marked for both objectives counts under
  its highest marker, or as cross-cutting when the two markers are equal.
- Full value, the OECD's practice, counts the whole of every marked project. A project marked for both
  objectives counts as cross-cutting.

Under both weightings, development banks' projects count at the climate share the banks report.

Each figure's `measurementMethod` names its weighting and, for provided figures, its attribution rule,
so every method is a facet of the same series:

- received, ONE Data's weighting: `MarkerScoreWeightingHighestMarkerAssignment`
- received, full value: `FullValueMarkerWeightingJointMarkerAssignment`
- provided, ONE Data's weighting: `MarkerScoreWeightingHighestMarkerAssignmentSpendingShareAttribution`
  or `MarkerScoreWeightingHighestMarkerAssignmentFundingShareAttribution`
- provided, full value: `FullValueMarkerWeightingJointMarkerAssignmentSpendingShareAttribution` or
  `FullValueMarkerWeightingJointMarkerAssignmentFundingShareAttribution`

## Splitting by objective

Each figure is published as a total and in four parts that add up to it under both weightings, each a
value of `climatePolicyObjective`:

- `ClimateChangePolicyObjective`, the total
- `AdaptationOnlyClimateChangePolicyObjective`, adaptation only
- `MitigationOnlyClimateChangePolicyObjective`, mitigation only
- `CrossCuttingClimateChangePolicyObjective`, adaptation and mitigation
- `UnspecifiedClimateChangePolicyObjective`, objective unspecified

Full value counts a project marked for both objectives once, as cross-cutting. The unspecified part
holds value whose adaptation marker is missing and attributed finance the package does not split by
objective. It is published only where it is not zero once rounded to the cent.

## Recipients

The recipient "developing countries, unspecified" (`DACBilateralUnallocated`) holds finance the package
records for developing countries in general. On provided figures it also holds core contributions to
institutions with no spending of their own to spread them over.

## Values and what is left out

Figures are the package's central figure, in current US dollars. The dataset leaves out:

- the package's estimated payments for IFC, IDB Invest, the Development Bank of Latin America and the
  Caribbean, and the New Development Bank, which report climate commitments but no payments, so these
  banks show commitments without disbursements
- the package's estimate of the climate share of the EBRD's aggregate disbursements
- the range around each figure that shows how much of it rests on uncertain links between payments and
  commitments
- net flows and grant equivalents

## Adding figures up

One provider's received and provided figures together give its climate finance. Adding the two kinds
together across providers counts multilateral finance twice, because a development bank's received figures hold
the same money that provided figures credit to the donors that fund it.

Under ONE Data's weighting, member states' provided figures include their share of what EU Institutions
spend, which EU Institutions' received figures also count. Adding EU Institutions to its member states
counts that money twice.
