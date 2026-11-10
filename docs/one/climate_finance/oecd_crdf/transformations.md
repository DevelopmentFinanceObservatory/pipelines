## What this dataset holds

The OECD's Climate-related Development Finance (CRDF) commitments from the recipient perspective, by
provider, recipient and year, from 2010 to 2024, in current US dollars. Every project with a Rio
marker counts at full value, and development banks' projects at the climate share the banks report.
Each figure carries `measurementMethod` `FullValueMarkerWeightingJointMarkerAssignment` and is a facet
of the same commitment series as ONE Data's received figures.

The dataset keeps every row the climate-finance package ships, including private foundations' grants,
memorandum rows and rows the package flags as repeats of others. ONE Data's full-value commitments
leave these rows out, so the totals of the two differ by them.

The recipient "developing countries, unspecified" (`DACBilateralUnallocated`) holds finance the OECD
records for developing countries in general.

## Splitting by objective

Each figure is published as a total (`ClimateChangePolicyObjective`) and in three parts that add up to
it: adaptation only, mitigation only, and cross-cutting. The OECD counts a project marked for both
objectives under adaptation and under mitigation. Here it counts once, as cross-cutting
(`CrossCuttingClimateChangePolicyObjective`), and the adaptation-only and mitigation-only parts leave it
out.
