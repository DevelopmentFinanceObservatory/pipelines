## Download the source data

The IMF publishes the World Economic Outlook in April and October. Each new release
is downloaded from the IMF's data API and replaces the previous copy, so the data
reflects the IMF's latest release.

## Select indicators

WEO covers national accounts, prices, employment, the balance of payments, government
finance and commodity prices. The indicators included cover gross domestic product
(GDP), gross national savings, gross capital formation and the output gap.

## Standardise countries and country groups

Country and country group codes are standardised to Data Commons IDs used across the
platform, so data can be compared across datasets. Country groups include the world,
regions and the euro area.

## Convert to whole units

WEO reports some amounts in billions, which are converted to whole units.

## Select dimensions

Every figure is reported against the year and the country. GDP in a country's own
currency and in US dollars is published as one variable in two units. Where a
country's own currency is the US dollar, the two are the same figure, so only the
US-dollar one is published. GDP at purchasing power parity is a separate variable.
Shares record what they are a share of, GDP or potential GDP, and per-capita figures
are divided by population.

## Mark projections as estimated

WEO includes the IMF's projections for the years after the last year the IMF reports
for each series. These values are marked as estimated, so they are kept apart from
reported values. The purchasing power parity series take the last reported year of GDP
in US dollars. Country groups other than the euro area have no last reported year, so
their values are marked as estimated from the year the release is published. The output
gap is marked as modelled in every year, since potential output is never observed.
