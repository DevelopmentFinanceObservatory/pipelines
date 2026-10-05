## 2026-10-01 First release

### Added

- ONE Conversion Tables (DAC) and ONE Conversion Tables (IMF), each with an exchange rate per US dollar
  about each currency's node and a US-dollar deflator about each country, rebased to equal 100 in the
  declared base year, 2024. The DAC table takes its deflators and donor-currency rates from DAC1 and
  every other currency's rate from the WEO. The IMF table takes both from the WEO. Both tables mark
  values computed from IMF projections with `measurementMethod: IMFProjection`.
- The DAC table fills WEO rates per currency, so a currency DAC1 reports has no rate in the years DAC1
  lacks. Before 1999 its euro rate is EU Institutions' ECU rate, while donors' DAC1 euro amounts are
  legacy currencies at the fixed conversion rates, so each donor's own currency reproduces the OECD's
  national-currency amounts from 1999.
