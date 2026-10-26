# Follow a GCAM value into a MARIO shock

Start with `gcam-mario demo`, or follow the [notebook](../notebooks/GCAM_to_MARIO.ipynb).
Both use the same functions as the extended-EXIOBASE workflow.

## 1. GCAM produces results; export physical production

GCAM is run upstream. For a chosen policy scenario and year, its result export
contains electricity generation by source and steel production by route and region.
The adapter consumes these results; no policy optimisation occurs inside MARIO.

The electricity CSV retains the GCAM export layout. This is an excerpt, showing
only the 2030 column and one technology:

```csv
Model,Scenario,Region,Variable,Unit,2030
GCAM 7.0,NDC_LTT_CBAM,EU-15,Secondary Energy|Electricity|Wind,EJ/yr,4.51167191
```

All nine electricity technologies are required to calculate the denominator.
The [complete file](../examples/gcam-output/electricity.csv) also contains other
years, scenarios and EU-12. Its physical values are extracted from the published
supplement before calculating any shares.

The steel CSV has the same layout. Its variables are names such as
`Production|Steel|DRI_EAF_H2`, with unit `Mt/yr`.

## 2. Interpret names and units explicitly

[`config/variables.json`](../config/variables.json) is the technology concordance:

| GCAM variable | MARIO label | Role |
|---|---|---|
| `Secondary Energy\|Electricity\|Wind` | `Wind` | Electricity commodity |
| `Production\|Steel\|DRI_EAF_H2` | `DRI-EAF-H2` | Steel-producing activity |
| Four `Production\|Steel\|EAF_scrap_*` variables | `Re-processing of secondary steel into new steel` | Sum the four physical outputs before calculating the activity share |

GCAM `EU-12` and `EU-15` map directly to the corresponding prepared MARIO regions.
The optional country profile assigns a cluster's mix to its individual countries;
it is a separate assumption recorded in the run. Many GCAM regions cannot be
combined into one MARIO rest-of-world region by averaging their shares. That
requires physical weights and an explicit aggregation before importing.

Within each scenario/region/year, production values must share a unit. Units
cancel in the quotient; EJ/yr electricity never becomes a TJ/tonne use coefficient
directly. Missing technologies, unknown names, duplicates, negative or non-finite
values, mixed units and zero production totals produce errors. There is no
interpolation and no epsilon replacement of zeros.

## 3. Convert physical output to target shares

For technology `i`, region `r`, scenario `g` and year `t`:

```text
share(i,r,g,t) = sum of GCAM output mapped to i / sum of all selected GCAM outputs
```

For EU-15 wind in NDC_LTT_CBAM, 2030:

```text
4.51167191 EJ/yr / 11.5304167607 EJ/yr = 0.391284374506
```

For steel, the same rule uses Mt/yr. The four secondary-steel export variables
first become one MARIO activity. The trace retains each original row, its
contribution, the mapped sum, the denominator and the resulting target share.

## 4. Apply a coefficient shock in MARIO

Every scenario starts from an explicitly named MARIO base scenario. `u` contains
commodity inputs per unit activity output; `s` contains activity market shares in
commodity supply. `Yc` contains commodity final demand in its physical units.

### Electricity: redistribute a fixed total in u and Yc

For each supplying region, sum its nine electricity rows separately in each
consuming column. Multiply that column's original total by the GCAM shares:

```python
old_total = u.loc[electricity_rows, consuming_columns].sum(axis=0)
u.loc[electricity_rows, consuming_columns] = shares[:, None] * old_total.to_numpy()[None, :]
```

The same operation is applied to `Yc`. This preserves total electricity sourced
from that region in every column, including imported electricity. It changes the
source technologies. Dedicated electricity inputs to `Hydrogen production with
electrolysis` are excluded by default, as in the original script; pass
`exclude_activities=()` in Python to change that assumption.

In the small example, EU-15 conventional steel uses 0.001275 TJ/tonne from EU-15
in total. Wind therefore receives 0.000498887577495 TJ/tonne. These inventory
values belong to the fictional teaching database. On extended EXIOBASE, the
**same equation** uses the actual database totals.

### Steel: redistribute market shares in s

For a region's steel commodity column, preserve the total market share of the
18 selected steel activities and distribute it using the GCAM production mix:

```python
old_share_total = s.loc[steel_activity_rows, steel_commodity_column].sum()
s.loc[steel_activity_rows, steel_commodity_column] = steel_shares * old_share_total
```

The technology inventories remain available in `u` and the satellite coefficients.
Changing `s` changes their contribution to the average steel commodity footprint.
Production quantities from GCAM determine these **relative shares**; this operation
does not force total MARIO production to equal GCAM production.

The implementation updates native `u`, `s` and `Yc` blocks, resets dependent
matrices and calculates the new footprints with MARIO. It preserves the baseline
and creates independent scenarios.

## 5. Inspect what actually changed

`coefficients_before_after.csv` shows every steel share and an electricity example
for the conventional steel activity and one final-demand column in each region.
It includes the target share, original group total, old coefficient, expected
product and **actual coefficient read back from MARIO**. The full electricity
shock applies the same equation to every eligible consuming column.

`run.json` reports conservation checks over all affected columns, alongside the
input hashes and region/technology mappings. `footprints.csv` compares baseline and
shocked steel footprints in tCO2eq/tonne. The small example's inventories and demand
are fictional; use [extended EXIOBASE](database-preparation.md) for that database's
quantitative results.
