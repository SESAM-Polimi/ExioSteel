"""Translate selected GCAM mixes into explicit, conservative native SUT updates."""
from dataclasses import dataclass

import numpy as np
import pandas as pd

from .inputs import MixSelection

STEEL = "Basic iron and steel and of ferro-alloys and first products thereof"
EXCLUDED_ACTIVITIES = ("Hydrogen production with electrolysis",)


@dataclass
class ShockReport:
    name: str
    base_scenario: str
    selection: MixSelection
    electricity: bool
    steel: bool
    excluded_activities: tuple
    checks: list

    def as_dict(self):
        return {"name": self.name, "base_scenario": self.base_scenario,
                "inputs": self.selection.as_dict(), "apply_electricity": self.electricity,
                "apply_steel": self.steel, "excluded_electricity_consumers": list(self.excluded_activities),
                "conservation_checks": self.checks,
                "interpretation": "GCAM output volumes set mix shares; they do not overwrite total MARIO demand or bilateral trade"}


def _validated_rows(frame, region, level, shares):
    if not isinstance(shares, pd.Series) or not shares.index.is_unique or shares.empty:
        raise ValueError(f"{region}: shares must be a non-empty Series with unique technology labels")
    values = shares.to_numpy(dtype=float)
    if not np.isfinite(values).all() or (values < 0).any() or not np.isclose(values.sum(), 1, rtol=0, atol=1e-12):
        raise ValueError(f"{region}: shares must be finite, non-negative and sum to one")
    rows = pd.MultiIndex.from_tuples([(region, level, item) for item in shares.index], names=frame.index.names)
    missing = rows.difference(frame.index)
    if len(missing):
        raise ValueError(f"Missing MARIO {level} labels: {missing.tolist()}")
    return rows


def apply_mix_shock(db, selection, *, name, base_scenario="baseline", electricity=True,
                    steel=True, exclude_activities=EXCLUDED_ACTIVITIES):
    """Clone a base scenario and change u, Yc and s using GCAM production shares.

    Electricity preserves each source region's total in every consuming column;
    steel preserves the selected routes' market-share total in each source region.
    Hydrogen electrolysis keeps its dedicated electricity inventory by default,
    matching the original script. All selections are validated before cloning.
    """
    if not isinstance(selection, MixSelection):
        raise TypeError("selection must be returned by Projections.select")
    if not isinstance(name, str) or not name.strip() or name in db.scenarios:
        raise ValueError("Choose a non-empty scenario name that does not already exist")
    if base_scenario not in db.scenarios or not (electricity or steel):
        raise ValueError("Choose an existing base scenario and at least one mix to apply")
    if (electricity and not selection.electricity) or (steel and not selection.steel):
        raise ValueError("The selection must contain every requested mix")
    if db.meta.table != "SUT":
        raise ValueError("This adapter requires a supply-use table (SUT)")
    u = db.query(matrices="u", scenarios=base_scenario).copy()
    s = db.query(matrices="s", scenarios=base_scenario).copy()
    yc = db.query(matrices="Yc", scenarios=base_scenario).copy()
    columns = u.columns[~u.columns.get_level_values(2).isin(exclude_activities)]
    checks = []
    if electricity:
        for region, shares in selection.electricity.items():
            rows = _validated_rows(u, region, "Commodity", shares)
            if len(rows.difference(yc.index)):
                raise ValueError(f"Missing electricity final-demand rows for {region}")
            before_u = u.loc[rows, columns].sum(axis=0).to_numpy()
            before_y = yc.loc[rows].sum(axis=0).to_numpy()
            # This is the entire electricity interpretation: share * old total.
            u.loc[rows, columns] = shares.to_numpy()[:, None] * before_u[None, :]
            yc.loc[rows] = shares.to_numpy()[:, None] * before_y[None, :]
            checks.append({"kind": "electricity", "region": region,
                           "max_use_total_change": float(np.max(np.abs(u.loc[rows, columns].sum(axis=0).to_numpy()-before_u), initial=0)),
                           "max_final_demand_total_change": float(np.max(np.abs(yc.loc[rows].sum(axis=0).to_numpy()-before_y), initial=0))})
    if steel:
        for region, shares in selection.steel.items():
            rows = _validated_rows(s, region, "Activity", shares)
            column = (region, "Commodity", STEEL)
            if column not in s.columns:
                raise ValueError(f"Missing MARIO steel commodity in {region}")
            before = float(s.loc[rows, column].sum())
            if not np.isfinite(before) or before <= 0:
                raise ValueError(f"{region}: selected steel routes have no positive supply share to redistribute")
            # GCAM technology production determines the supplier shares in s.
            s.loc[rows, column] = shares.to_numpy() * before
            checks.append({"kind": "steel", "region": region, "selected_market_share": before,
                           "market_share_total_change": float(s.loc[rows, column].sum()-before)})
    try:
        db.clone_scenario(base_scenario, name)
        db.update_scenarios(name, u=u, s=s, Yc=yc)
        db.reset_to_coefficients(name)
    except Exception:
        db.matrices.pop(name, None)
        raise
    return ShockReport(name, base_scenario, selection, electricity, steel, tuple(exclude_activities), checks)


def explain_coefficients(db, selection, *, scenario, base_scenario="baseline"):
    """Show a compact before/after trace for every steel share and one electricity user.

    The full electricity shock applies this same formula to every eligible column.
    This inspection table selects one conventional steel activity for readability.
    """
    rows = []
    for matrix, mixes, level in [("s", selection.steel, "Activity"), ("u", selection.electricity, "Commodity"),
                                  ("Yc", selection.electricity, "Commodity")]:
        before = db.query(matrices=matrix, scenarios=base_scenario)
        after = db.query(matrices=matrix, scenarios=scenario)
        for region, shares in mixes.items():
            if matrix == "s":
                column = (region, "Commodity", STEEL)
            elif matrix == "u":
                column = (region, "Activity", "Manufacture of basic iron and steel and of ferro-alloys and first products thereof")
            else:
                candidates = [c for c in before.columns if c[0] == region]
                column = candidates[0] if candidates else before.columns[0]
            index = [(region, level, item) for item in shares.index]
            total = float(before.loc[index, column].sum())
            for key, share in zip(index, shares.to_numpy(), strict=True):
                rows.append({"matrix": matrix, "region": region, "item": key[2],
                             "consumer_or_product": str(column), "gcam_share": float(share),
                             "original_group_total": total, "before": float(before.loc[key, column]),
                             "share_times_original_total": float(share*total), "after": float(after.loc[key, column]),
                             "unit": "share" if matrix == "s" else str(db.units["Commodity"].loc[key[2], "unit"]) +
                             ("/" + str(db.units["Activity"].loc[column[2], "unit"]) if matrix == "u" else "")})
    return pd.DataFrame(rows)
