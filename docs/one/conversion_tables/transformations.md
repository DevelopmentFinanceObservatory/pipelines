## What these tables hold

The graph stores every monetary observation once, in the currency its source reports and at current
prices. The ONE conversion tables hold what it takes to convert those values to another currency or to
constant prices: an exchange rate for each currency and a US-dollar deflator for each country. There are
two tables, each its own import under its own provenance.

- ONE Conversion Tables (DAC) serves OECD data such as the CRS. Its deflators and the rates of the DAC
  donors' currencies come from the OECD DAC1 table, and every other currency's rate comes from the IMF
  World Economic Outlook (WEO).
- ONE Conversion Tables (IMF) serves IMF and World Bank data. Both its rates and its deflators come from
  the WEO.

Each table is computed by its own models and checked by its own tests, so a DAC1 problem, such as a new
donor with no node or a failing DAC check, stops the DAC table and leaves the IMF table to publish. The DAC
table reads the IMF table's rates to fill the currencies DAC1 lacks, so a failure in those rates stops
both.

Each table has two StatVars. The exchange rate is in units of a currency per US dollar, as an annual
average, and each observation is about the currency's node, such as `CurrencyEUR`. The US dollar is 1 in
every year. The deflator is about a country, about EU Institutions, or, in the DAC table, about
`DACCountriesTotal`, and each series equals 100 in the base year the provenance declares as
`deflatorBaseYear`. A value V in currency S, year t, converts to currency T at constant prices of base
year b as V × (D[R, b] / D[R, t]) × X[T, b] / X[S, t], where X is the rate, D the deflator and R the
entity whose prices apply.

## Read the sources

The DAC table reads total net official development assistance (ODA) from DAC1, aid type 1010 and net
disbursements 1140, in current US dollars, in constant US dollars of the DAC base year and in national
currency. The same aid type and flow in each price basis means the ratio of any two is a price or
currency ratio. DAC1 group totals, which carry codes of 20000 and above, are
dropped, except DAC Countries, Total, which becomes `DACCountriesTotal`. Every other donor resolves to its
node through the ontology's DAC code, and a donor with no node fails the build.

Both tables read GDP in national currency (NGDP) and GDP in US dollars (NGDPD) from the WEO, and the
IMF table also reads the GDP deflator (NGDP_D), each flagged as reported or projected by the WEO's last
actual year for that country and series. The WEO staging model stages NGDP_D beside the indicators the
WEO import publishes, and the import leaves it out.

## Compute each area's implied rate

Every area's own rate is computed first. A DAC1 donor's is its national-currency ODA over its US-dollar
ODA. A WEO country's is its national-currency GDP over its US-dollar GDP, and is a projection when either
GDP figure is.

Each area takes the currency the IMF's primary domestic currency map gives its country. The map is
present-tense, so one currency applies to every year. EU Institutions, which have no country, take the
euro from the `currency_rate_reference` seed. A DAC1 donor with no currency, such as DAC Countries,
Total, has no rate.

## Choose one rate per currency

Several areas can share a currency, so `currency_rate_reference` names the area whose rate is the
currency's rate. In the DAC table the euro comes from EU Institutions, which report in euros (ECU before
1999) in every year. In the IMF table it comes from Germany, because the WEO publishes no euro-area GDP in
euros. A currency's issuer supplies it where there is one, as the United States does for the US dollar.
For the CFA francs and the Eastern Caribbean dollar, which no country issues, the seed names a member. A
currency one area uses takes that area's rate. A shared currency with no reference fails the build.

## Check the members agree

Every other member of a shared currency is compared with the reference in the member's last actual year.
Earlier years can sit in a legacy currency, such as a euro member's before it joined, and projected years
follow each country's own forecast, so the latest reported year is the one the present-tense map speaks
for. Members must agree within a relative 1e-6. DAC1's bulk file carries national-currency amounts to six
decimals and real members agree within 1e-8, so 1e-6 leaves headroom while catching a wrong currency. A
member that disagrees beyond the tolerance fails the build. The seed lists the members left out and why:
Andorra and Nauru, whose WEO GDP ratios differ from Germany's and Australia's; Tuvalu, whose GDP the WEO
publishes to the nearest thousand, too coarse for 1e-6; and Bulgaria, which DAC1 reports in lev through 2025
although the map gives the euro it adopted in 2026.

## Fill the DAC table's other currencies

DAC1 publishes rates for its donors' currencies only. The DAC table takes the IMF table's rate for every
other currency, so aid to Kenya converts to Kenyan shillings.

The fill is per currency. A currency DAC1 reports keeps DAC1's rates alone and has no rate in the years
DAC1 lacks: the years after DAC1's latest, which include every WEO projection year, and the years before
its donor began reporting. No currency's series mixes the two sources.

From 1999 each donor's own currency reproduces the OECD's national-currency amounts. Before 1999 the euro
rate is EU Institutions' ECU rate, while donors' DAC1 euro amounts are their legacy currencies converted
at the fixed euro conversion rates, so a donor's pre-1999 euro amounts differ from its US-dollar amounts
converted at the table's euro rate.

The CFP franc and the Caribbean guilder have no source row in either table.

## Check the rates against each other

DAC1 and the WEO compute each currency's rate from independent figures. For every currency-year both
report, the DAC1 rate must lie within a factor of two of the WEO's reported rate. The two differ by
averaging method and fiscal years by far less than a power of ten, so a ratio outside the band means one side
read its amounts at the wrong scale. The US dollar must equal 1 in both. The band guards the rates DAC1
supplies, so it gates the DAC table alone. The DAC member agreement check gates the DAC table alone,
and the IMF member agreement check gates both tables, because the DAC table fills from the IMF rates.

## Compute the US-dollar deflators

A US-dollar deflator moves with domestic prices and the exchange rate together, so converting a
current-dollar value at it gives constant dollars of the base year.

- DAC: 100 × current-dollar ODA / constant-dollar ODA, the deflator the DAC itself applies, for every
  donor and for DAC Countries, Total.
- IMF: NGDP_D × NGDPD / NGDP for each country, which turns the WEO's national-currency deflator into
  US-dollar terms. A value is a projection when any of the three figures is.

## Rebase to the standard base year

Each entity's deflator series is divided by its own value in the base year, so every series equals 100
there. A rebase leaves every ratio of two years, and so every converted value, unchanged. An entity with
no deflator in the base year is left out, because the DAC never rebases a donor that has stopped
reporting and such a series has no known base. The DAC recipe deflates those donors' rows by
`DACCountriesTotal`.

The base year is a pipeline parameter, set to the base year of the DAC's published constant prices. A
test reads the DAC Countries, Total deflator as DAC1 publishes it, before rebasing, and fails the build
unless it equals 100 in the parameter year. When the DAC moves its base year and the parameter lags, the
build fails.

## Mark projections

A rate or deflator computed from an IMF projection carries `measurementMethod: IMFProjection`, in the
same file as the reported values. DAC1 values are all reported.
