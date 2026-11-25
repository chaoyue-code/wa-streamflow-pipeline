# wa_streamflow (dbt project)

The dbt Core project for the WA Streamflow Pipeline. It transforms raw BoM discharge and DWER station data in Snowflake into tested marts.
For the project question, data sources and architecture, see the [main README](../README.md).

## Layers
| Layer | Models | Materialisation | Purpose |
|---|---|---|---|
| Staging | `stg_bom__discharge`, `stg_dwer__stations` | View | Flatten raw JSON, cast types, convert to Perth dates, keep the latest ingested row per station and day |
| Intermediate | `int_station_water_year` | View | Water year start month per station (BoM HRS rule: the month with the lowest average flow) |
| Marts | `fct_daily_discharge`, `dim_station`, `agg_annual_flow` | Table (`MARTS` schema) | Daily fact, station dimension, annual flow and runoff depth per water year |

Seed: `bom_quality_codes` maps BoM quality codes to grades and works as a small dimension.

## Tests
33 data tests, all passing (`dbt build`):
- **Grain:** station plus date is unique in the fact table, and station plus water year is unique in the annual table
- **Keys:** `unique` and `not_null` on station keys, plus `relationships` from the fact and annual tables to `dim_station`
- **Quality codes:** `accepted_values`, plus a `relationships` test to the seed
- **Ranges:** water year start month between 1 and 12, discharge of 0 or more on non-missing days, completeness between 0 and 1
- **DWER join check:** coordinates and catchment area are not null, and the coordinates fall inside WA

Tests are defined in the `_*.yml` files next to each layer. They use [dbt_utils](https://github.com/dbt-labs/dbt-utils).

## How to run
```bash
pip install -r requirements.txt
dbt deps      # installs dbt_utils
dbt build     # seeds, models and tests in dependency order
```
You need a `wa_streamflow` profile in `~/.dbt/profiles.yml` with Snowflake key-pair authentication (not committed).

## Variables
| Variable | Default | Used in |
|---|---|---|
| `min_completeness_pct` | 0.95 | `agg_annual_flow.is_complete_year` |
