# WA Streamflow Pipeline

An end-to-end data pipeline analysing long-term streamflow change in south-west Western Australia.
Author: Chaoyue Zhao

## Question
How has streamflow in south-west WA catchments changed since 1975, and which catchments show the steepest decline?

## Data sources
- Bureau of Meteorology Water Data Online (KiWIS API): daily mean discharge, time series `DMQaQc.Merged.DailyMean.24HR`
- DWER Water Information Sites (DWER-145), data.wa.gov.au: station metadata (planned)

## Station selection
1. Daily discharge record starting before 1975 and still current
2. At least 90% of days coded Quality A (code 10) since 1975.
   Completeness alone was misleading: some "100% complete" stations were mostly estimated values.
3. Listed as a BoM Hydrologic Reference Station (unregulated, minimal land-use change)
4. One station per basin

Exceptions:
- 613002 Dingo Rd accepted at 89% Quality A because it is an HRS station
- Basin 602 excluded: no candidate station was an HRS station
- 613007 Waterous ranked highest in basin 613 but is not an HRS station, so 613002 Dingo Rd was used instead
- HRS status was checked manually at bom.gov.au/water/hrs, so the final list differs from the notebook's automated shortlist

Final stations: see `config/stations.csv` (9 stations).

## Notes
- Timestamps are returned in UTC (16:00 UTC = 00:00 AWST) and converted to Australia/Perth. The `DailyMean` series is already daily, so no aggregation is needed.
- Annual analysis will use the HRS water year, which BoM defines per station as starting in the month with the lowest average monthly flow. Water-year start months will be recorded per station in `config/stations.csv`.
