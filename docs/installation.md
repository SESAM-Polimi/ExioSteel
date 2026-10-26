# Install and run

Use Python 3.12 in a dedicated environment, from this checkout:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python tools/install.py
gcam-mario demo
```

Alternatively create the environment with `conda env create -f environment.yml`
and activate `gcam_mario` before running the installer. For the notebook install
JupyterLab or use an editor with this environment's kernel.

`tools/install.py` installs the pinned runtime, then `mariopy==1.0.0` and this
project with `--no-deps`. MARIO 1.0.0's metadata requires pandas 3.0.2 and an optional
cvxlab distribution that pins pandas 2.3.3; the ordinary resolver cannot satisfy
both. This example uses IO calculations and does not install that optimisation
extension. `pip check` can therefore report MARIO's missing cvxlab dependency.
The workaround is confined to this dedicated environment.

## Small example

All projection inputs are included. `gcam-mario demo` needs no database download
or GCAM installation. It uses a small real MARIO SUT with explicitly fictional
inventories, demand and baseline shares. Changing `--scenario`, `--year` and
`--only electricity|steel|both` selects and isolates the projection shock.

Use a new `--output` directory for each experiment. Existing results are protected
unless you explicitly pass `--overwrite`.

## Full database

```bash
gcam-mario download             # optional separate download
gcam-mario prepare --offline
gcam-mario run --year 2030 --offline
```

The automatic source download is 1.78 GB. Allow around 3 GB for original TXT files,
converted Parquets and the prepared model, plus results. Downloads resume and are
verified against the fixed record's published checksums. Preparation and full
MRIO calculation take substantially longer than the small example.

Existing coefficient Parquets can be supplied without downloading:

```bash
gcam-mario prepare --source /path/to/parquets --prepared models/prepared --offline
```

The prepared bundle can subsequently run without its source files. See
[database preparation](database-preparation.md) for source checks and assumptions.
