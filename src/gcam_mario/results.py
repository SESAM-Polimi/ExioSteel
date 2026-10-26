"""Export the input-to-coefficient trace alongside calculated MARIO footprints."""
from datetime import datetime, timezone
from importlib.metadata import version
import json
import os
from pathlib import Path
import tempfile

import numpy as np
import pandas as pd

from .resources import sha256
from .shocks import STEEL, explain_coefficients


def footprints(db, scenarios, regions):
    """Return commodity GHG footprints in tonnes CO2eq per tonne of steel."""
    if str(db.units["Commodity"].loc[STEEL, "unit"]) != "tonnes" or str(db.units["Satellite account"].loc["GHG", "unit"]) != "tonnes":
        raise ValueError("Expected tonnes for steel and GHG before labelling the footprint tCO2eq/t")
    rows = []
    for scenario in scenarios:
        fc = db.query(matrices="fc", scenarios=scenario)
        for region in regions:
            value = float(fc.loc["GHG", (region, "Commodity", STEEL)])
            if not np.isfinite(value):
                raise ValueError(f"Non-finite footprint for {region}/{scenario}")
            rows.append({"scenario": scenario, "region": region, "ghg_tco2eq_per_tonne_steel": value})
    return pd.DataFrame(rows)


def export_run(db, reports, destination, *, database_kind, prepared_manifest=None, overwrite=False):
    """Write raw-row interpretation, target mixes, actual coefficient edits and results."""
    destination = Path(destination).expanduser().resolve()
    if destination.exists() and any(destination.iterdir()) and not overwrite:
        raise ValueError(f"{destination} is not empty; select a new output directory")
    if not reports:
        raise ValueError("At least one shock report is required")
    scenarios = list(dict.fromkeys([r.base_scenario for r in reports] + [r.name for r in reports]))
    regions = sorted({r for report in reports for group in [report.selection.electricity, report.selection.steel] for r in group})
    result = footprints(db, scenarios, regions)
    traces, coefficients = [], []
    for report in reports:
        trace = report.selection.trace.copy()
        trace["MarioScenario"] = report.name
        traces.append(trace)
        changes = explain_coefficients(db, report.selection, scenario=report.name, base_scenario=report.base_scenario)
        changes["MarioScenario"] = report.name
        coefficients.append(changes)
    trace = pd.concat(traces, ignore_index=True)
    mix = trace[["MarioScenario", "Kind", "Scenario", "Year", "Region", "MarioRegion", "MarioItem", "MappedValue", "GroupTotal", "Share"]].drop_duplicates()
    manifest = {"created_utc": datetime.now(timezone.utc).isoformat(), "database_kind": database_kind,
                "inventories_are_illustrative": database_kind == "demo", "mariopy": version("mariopy"),
                "application_sha256": {p.name: sha256(p) for p in sorted(Path(__file__).parent.glob("*.py"))},
                "prepared_database": prepared_manifest, "shocks": [r.as_dict() for r in reports],
                "note": "GCAM output volumes determine mixes. This workflow does not set total steel consumption or bilateral import volumes."}
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".gcam-export-", dir=destination.parent) as temporary:
        temp = Path(temporary)
        result.to_csv(temp/"footprints.csv", index=False)
        trace.to_csv(temp/"input_interpretation.csv", index=False)
        mix.to_csv(temp/"mixes.csv", index=False)
        pd.concat(coefficients, ignore_index=True).to_csv(temp/"coefficients_before_after.csv", index=False)
        manifest["files"] = {p.name: sha256(p) for p in temp.iterdir()}
        (temp/"run.json").write_text(json.dumps(manifest, indent=2)+"\n")
        destination.mkdir(parents=True, exist_ok=True)
        (destination/"run.json").unlink(missing_ok=True)
        for p in temp.iterdir():
            if p.name != "run.json":
                os.replace(p, destination/p.name)
        os.replace(temp/"run.json", destination/"run.json")
    return destination
