# Python API

The [worked notebook](../notebooks/GCAM_to_MARIO.ipynb) exposes the whole sequence.
The equations and their interpretation are in [From GCAM to MARIO](from-gcam-to-mario.md).

```python
from gcam_mario import Projections, demo_database, apply_mix_shock, explain_coefficients

inputs = Projections()
selected = inputs.select("NDC_LTT_CBAM", 2030)
db = demo_database()
report = apply_mix_shock(db, selected, name="CBAM_2030")
changes = explain_coefficients(db, selected, scenario="CBAM_2030")
```

## Read outputs

`Projections(electricity_export=None, steel_export=None, *, steel_mix=None,
variable_mapping=None)` reads the bundled physical GCAM outputs by default.
Custom exports must follow the [input contract](input-contract.md).
`variable_mapping` is a dictionary with `electricity` and `steel` concordances;
its keys are GCAM variables and its values are MARIO item names. `steel_mix` is an
explicit alternative for an already prepared legacy share workbook.

- `inputs.available()` returns the available scenario/region/year combinations.
- `inputs.tables` exposes the original rows, mapped physical totals and shares.
- `inputs.select(scenario, year, *, region_mapping=None, profile="eu-clusters",
  kinds=("electricity", "steel"))` returns a `MixSelection`. `kinds` can select one
  mix, so a new electricity scenario does not require matching steel projections.
- `selected.trace` exposes each original value, unit, denominator, source row and
  destination label. `selected.as_dict()` records mappings and selected shares.

## Apply a shock

`apply_mix_shock(db, selection, *, name, base_scenario="baseline", electricity=True,
steel=True, exclude_activities=("Hydrogen production with electrolysis",))`
creates an independent MARIO scenario and returns a `ShockReport`.

The database must be a SUT with matching region, commodity and activity labels.
All selected shares and target labels are validated before creating the scenario.
Names cannot overwrite existing scenarios. The native blocks updated are `u`,
`s` and `Yc`; dependent matrices are invalidated and calculated by MARIO on demand.

For electricity alone:

```python
selected = inputs.select("NDC_LTT_CBAM", 2030, kinds=("electricity",))
report = apply_mix_shock(db, selected, name="electricity_2030", steel=False)
```

The default electrolysis exclusion preserves its dedicated electricity inventory,
as in the old script. Use `exclude_activities=()` to apply the grid mix to that
consumer as well. `report.as_dict()` records the choice and conservation checks.

`explain_coefficients(db, selection, *, scenario, base_scenario="baseline")`
reads back every selected steel share and representative electricity `u`/`Yc`
columns. It returns the old value, original group total, target share, expected
product, actual new value and unit. Electricity changes apply to all eligible
columns; the inspection table is a compact sample.

## Full database and results

```python
from gcam_mario.preparation import prepare_database, load_prepared
from gcam_mario.results import footprints, export_run

# First use: automatically obtain the source when missing.
db, provenance = prepare_database("models/prepared")
# Later, without source files or network:
db, provenance = load_prepared("models/prepared")
```

`prepare_database(destination="models/prepared", *, source=None, offline=False,
progress=print)` creates or loads a verified parameter-independent bundle.
`source` selects existing coefficient Parquets; `offline=True` prohibits download.

`footprints(db, scenarios, regions)` returns commodity footprints in tCO2eq/tonne
steel, requiring compatible tonne units for both steel and the GHG account.

`export_run(db, reports, destination, *, database_kind, prepared_manifest=None,
overwrite=False)` writes input interpretations, mixes, actual coefficient changes,
footprints and an integrity/provenance manifest. Use `database_kind="demo"` for
fictional teaching inventories or `"extended-exiobase"` for the prepared source.
All matrices remain available through the MARIO `db` object.
