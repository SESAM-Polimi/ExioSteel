# Study sources and reproduction scope

The research reference is Ghezzi, Rinaldi, Horowitz, van de Ven, Holtz, Jülich and
Rocco (2026), [*Linking Global Integrated Assessment and Hybrid Input–Output Models
To Support Steel Decarbonization Policies in Europe*](https://doi.org/10.1021/acs.est.5c15099).
The [open article](https://pmc.ncbi.nlm.nih.gov/articles/PMC13296473/) describes the
coupling and its assumptions; the [supporting information](https://doi.org/10.1021/acs.est.5c15099.s001)
contains the published inputs and results.

## What is available here

The original source revision is
[`151dcf6bcdeed5a3f76277ace5decf93c408066a`](https://github.com/SESAM-Polimi/ExioSteel/tree/151dcf6bcdeed5a3f76277ace5decf93c408066a).
It is retained as the historical reference; this development does not replace its
scripts or claim a new archived paper version.

| Existing resource | Role |
|---|---|
| `support/db_building.py` | Reconstruction from raw EXIOBASE, electricity nowcasting, energy accounts and initial steel adjustments |
| `add_sectors.py` and inventory workbooks | Technology insertion and database export |
| `support/gcam_data/gcam_data_prep.py` | Original mapping of GCAM variables and creation of share workbooks |
| `shocks_analysis.py` | Study shock/calculation driver and result merging, with machine-specific paths |
| `post_process.py`, `plots.py` | Downstream processing and figures, with external result-file requirements |
| Aggregation and inventory workbooks | Source concordances and technology data |

## What the new route verifies

The new example isolates the use of exported GCAM results. It reads physical
quantities, checks the input contract, maps names, computes mix shares, applies
native MARIO coefficient updates and exports an auditable before/after trace.
It supports both a quick teaching database and the published extended-EXIOBASE
source. GCAM model execution and its scenario optimisation remain upstream.

The new default source scope is EU-12/EU-15. Other regions retain their background
mixes. Total consumption and bilateral trade retain their database values.
Therefore the resulting experiment is a documented mix-coupling calculation,
not a claim to reproduce every study figure or its total EU consumption emissions.

## Differences from the old driver

Source inspection identified behaviours that should not be copied implicitly:

- The old preprocessor replaces zero shares by `1e-8`. The new physical-output
  reader keeps zeros and calculates shares from a positive physical denominator.
- The old shock driver reads `Steel_consumption.xlsx` but never applies it.
- Its import loop builds temporary steel tables without writing them back to
  `u` or `Y`; the first supplier also lacks the share multiplier. The new workflow
  does not claim those temporary tables implement a trade shock.
- The original loop modifies derived `z` and reuses a scenario. The new route
  reads an explicit base scenario, writes native `u`, `s`, `Yc`, and resets
  dependent matrices, keeping scenarios independent.
- The default electrolysis electricity exclusion is retained and recorded.

The [mechanism guide](from-gcam-to-mario.md) specifies exactly what is preserved
and changed. The original scripts remain available for historical investigation;
portable reproduction of all upstream steps and the complete paper's processing
would require a separate verification against its fixed results.
