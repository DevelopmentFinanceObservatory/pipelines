## 2026-09-21 First release

### Added

- Dataset added to the graph: imputed multilateral spending by sector and recipient, computed from
  `oda-data`'s `imputed_multilateral_by_purpose`. Channels resolve through `oda-data`'s reviewed
  provider/agency crosswalk; a core contribution with no usable share from its own channel falls back to a
  stale window, a reviewed proxy channel, or an explicit `unallocated` status published against DAC
  recipient 9998 and CRS purpose 998. Amounts are gross disbursements at current US dollar prices; both
  source releases (CRS and Members' Total Use) are cited with their own release year. The series are
  `FinancialFlow` StatVars measuring `attributedAmount`, constrained by `dac2aAidType`
  `DAC2aAidType106`, with measurement method `Modelled`.
