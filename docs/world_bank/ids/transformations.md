## Download the source data

The World Bank publishes IDS as six bulk files, split by debtor name. All six are
downloaded each time the pipeline runs, and replace the previous copy, so the data
reflects the World Bank's latest release.

## Exclude series outside the debt statistics

Non-debt statistics are excluded, including population, balance of payments, investment
and reserves. GNI is kept, because some debt ratios are shares of it. Exports of goods,
services and primary income, and the three debt ratios that are shares of it, are
excluded until the ontology can describe them. Debt forgiveness grants, the stock-flow
reconciliation residual, the annual changes in debt stocks and interest arrears, and the
present value of debt are also excluded.

## Omit zeros where no lending took place

IDS reports a value for every debtor, creditor and series, including pairs with no
lending between them, so most figures are zero. Zeros are kept on world totals, and
within or after a period of lending between a debtor and a creditor. They are omitted
where a creditor has not lent to a debtor, for the years before lending began, and for
averages in years with no new commitments, since there is nothing to average.

## Standardise debtors and creditors

Debtor and creditor codes are standardised to Data Commons IDs used across the
platform, so data can be compared across datasets. Where a series names a single lender
in its code, such as debt owed to the IBRD, that lender becomes the creditor.

## Select dimensions

Every figure is reported against the year, the debtor and the creditor, and the
dimensions the World Bank encodes in each series code, such as type of flow, maturity
and creditor type. GNI is the exception: it describes the debtor economy, so it is
reported against the year and the economy alone. Ratios record what they are a share of:
GNI or total external debt stocks. Every amount is reported in US dollars at current
prices.

## Mark projections as estimated

For the years after the last reported year, the World Bank projects debt service and
disbursements on commitments already made. These values are marked as estimated, so
they are kept apart from reported values.
