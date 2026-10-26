# GCAM files and their interpretation

The default workflow starts with two physical-output CSVs in
[`examples/gcam-output/`](../examples/gcam-output/README.md). They are inputs to the
adapter, not MARIO shock workbooks and not policy settings for a new GCAM run.

## File schema

| Column | Meaning | Example |
|---|---|---|
| `Model` | Optional model identifier; select one model per file | `GCAM 7.0` |
| `Scenario` | Scenario whose results were exported by GCAM | `NDC_LTT_CBAM` |
| `Region` | GCAM output region | `EU-15` |
| `Variable` | Physical production variable | `Secondary Energy\|Electricity\|Wind` |
| `Unit` | A consistent physical production unit within the mix | `EJ/yr` or `Mt/yr` |
| `2030`, `2035`, … | Production in that year | `4.51167191` |

A long layout with `Year,Value` instead of year columns is also accepted. Keep
zero-production routes in the file: an absent row and an explicit zero have
different meanings. The reader requires all entries in the selected technology
concordance for each scenario, region and year.

Filter a large GCAM export to the variables in
[`config/variables.json`](../config/variables.json). Mixing aggregate electricity
variables with their technology subtotals would double-count output; the reader
rejects unrecognised variables instead of dropping them. A different variable
concordance can be passed through the Python API.

## Physical quantities become shares

The reader validates units and non-negative finite values, maps technology names,
sums many-to-one mappings, then divides by the regional production total. It does
not convert absolute GCAM volumes to MARIO demand. No interpolation, extrapolation
or replacement of zero shares is performed.

`input_interpretation.csv` preserves `SourceFile`, `SourceRow` (CSV header is row 1),
`Scenario`, `Region`, `Variable`, `Unit`, `Year`, `Value`, `MarioItem`, `MappedValue`,
`GroupTotal`, `ContributionShare`, `Share`, `Kind` and `MarioRegion`. `ContributionShare`
belongs to an original GCAM row; `Share` belongs to the mapped MARIO technology.
When several rows map to one technology, `Share` is repeated in the trace and is
written only once in `mixes.csv`. Do not sum the repeated `Share` column.

## Region concordance

The default `eu-clusters` profile maps EU-12/EU-15 directly to the prepared MARIO
regions. `eu-countries` applies those cluster mixes to the constituent country
codes recorded in the original aggregation workbook. It requires a country-level
database; it cannot be applied to the two-region demo or cluster-prepared bundle.

A custom JSON maps each GCAM region to a list of MARIO regions:

```json
{"EU-12": ["EU-12"], "EU-15": ["EU-15"]}
```

Each MARIO region must have one GCAM source. Several GCAM regions cannot be mapped
to the same destination without a separately defined physical aggregation. Only
the selected regions are shocked; the rest of the database retains its background
assumptions.

## Already prepared legacy mixes

`support/gcam_data/Steel_mixes.xlsx` and `Electricity_mixes.xlsx` are **derived
share workbooks**, not raw GCAM physical outputs. The original
`gcam_data_prep.py` mapped variables, calculated shares and replaced zeros with
`1e-8`. These files are retained for transparency. The main new example starts
from physical output instead and retains exact zeros.

The optional `Projections(steel_mix=...)` Python argument reads the legacy steel
workbook and renormalises its shares, recording that its input is already prepared.
Do not supply a share workbook as `steel_export` or claim that its values are Mt/yr.

[Mechanism](from-gcam-to-mario.md) · [API](api.md) · [Input provenance](../examples/gcam-output/README.md)
