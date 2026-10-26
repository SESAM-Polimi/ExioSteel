"""Explicit GCAM export contract: physical output -> concordance -> shares.

The adapter does not run GCAM or infer its policy assumptions. It reads the
Model, Scenario, Region, Variable, Unit and year columns of exported results.
"""
from dataclasses import dataclass
from pathlib import Path
import re

import numpy as np
import pandas as pd

from .resources import configuration, resource_root, sha256


def read_prepared_mix(path):
    """Read the legacy derived workbook; do not relabel shares as GCAM production."""
    frame = pd.read_excel(path, index_col=[0, 1, 2]).reset_index()
    long = frame.melt(id_vars=["Scenario", "Region", "Variable"], var_name="Year", value_name="Value")
    long["Year"] = pd.to_numeric(long.Year).astype(int)
    keys = ["Scenario", "Region", "Year"]
    if long.duplicated(keys + ["Variable"]).any() or not np.isfinite(long.Value).all() or (long.Value < 0).any():
        raise ValueError("Invalid prepared steel shares")
    long["GroupTotal"] = long.groupby(keys).Value.transform("sum")
    if (long.GroupTotal <= 0).any():
        raise ValueError("Prepared steel mix has a zero total")
    long["MarioItem"] = long.Variable
    long["MappedValue"] = long.Value
    long["Share"] = long.Value / long.GroupTotal
    long["ContributionShare"] = long.Share
    long["Unit"] = "share"
    long["InputStage"] = "prepared MARIO mix, not a raw GCAM export"
    return long


def read_export(path, mapping):
    """Read a wide or long GCAM CSV; require all mapped technologies, including zeros."""
    frame = pd.read_csv(path, float_precision="round_trip")
    frame["SourceFile"] = Path(path).name
    frame["SourceRow"] = frame.index + 2  # CSV header is line 1
    required = {"Scenario", "Region", "Variable", "Unit"}
    if not required <= set(frame):
        raise ValueError(f"{path}: required columns are {sorted(required)}")
    for column in required | ({"Model"} if "Model" in frame else set()):
        if frame[column].isna().any():
            raise ValueError(f"{path}: missing {column}")
        frame[column] = frame[column].astype(str).str.strip()
    if "Model" in frame and frame.Model.nunique() != 1:
        raise ValueError("Select exactly one GCAM Model before importing")
    if {"Year", "Value"} <= set(frame):
        long = frame.copy()
    else:
        years = [c for c in frame if re.fullmatch(r"\d{4}(?:\.0)?", str(c))]
        if not years:
            raise ValueError(f"{path}: no year columns or Year/Value pair")
        long = frame.melt(id_vars=[c for c in frame if c not in years], value_vars=years,
                          var_name="Year", value_name="Value")
    long["Year"] = pd.to_numeric(long.Year, errors="raise")
    if not np.isfinite(long.Year).all() or (long.Year % 1 != 0).any():
        raise ValueError("Years must be finite integers")
    long["Year"] = long.Year.astype(int)
    unknown = set(long.Variable) - set(mapping)
    if unknown:
        raise ValueError(f"Unmapped GCAM variables: {sorted(unknown)}. Supply a filtered export or an explicit concordance.")
    keys = ["Scenario", "Region", "Year"]
    if long.duplicated(keys + ["Variable"]).any():
        raise ValueError("Duplicate GCAM scenario/region/year/variable rows")
    long["Value"] = pd.to_numeric(long.Value, errors="raise")
    if not np.isfinite(long.Value).all() or (long.Value < 0).any():
        raise ValueError("GCAM production values must be finite and non-negative")
    for key, group in long.groupby(keys, sort=False):
        missing = set(mapping) - set(group.Variable)
        if missing:
            raise ValueError(f"{key}: missing technologies {sorted(missing)}; include zero-production rows")
        if group.Unit.nunique() != 1:
            raise ValueError(f"{key}: mixed units; harmonise units before computing shares")
        if group.Value.sum() <= 0:
            raise ValueError(f"{key}: zero total production; no mix can be calculated")
    long["MarioItem"] = long.Variable.map(mapping)
    long["GroupTotal"] = long.groupby(keys).Value.transform("sum")
    long["MappedValue"] = long.groupby(keys + ["MarioItem"]).Value.transform("sum")
    long["ContributionShare"] = long.Value / long.GroupTotal
    long["Share"] = long.MappedValue / long.GroupTotal
    long["InputStage"] = "GCAM physical output"
    # Exact zeros stay zero. No epsilon floor, interpolation or extrapolation.
    return long.sort_values(keys + ["Variable"]).reset_index(drop=True)


@dataclass
class MixSelection:
    scenario: str
    year: int
    region_mapping: dict
    electricity: dict
    steel: dict
    trace: pd.DataFrame
    sources: dict

    def as_dict(self):
        return {"gcam_scenario": self.scenario, "year": self.year,
                "region_mapping": self.region_mapping,
                "electricity": {r: s.to_dict() for r, s in self.electricity.items()},
                "steel": {r: s.to_dict() for r, s in self.steel.items()},
                "sources": self.sources,
                "normalisation": "sum physical output by mapped item / sum physical output in region, scenario and year"}


class Projections:
    """Read GCAM results, keeping a row-by-row explanation of their interpretation."""

    def __init__(self, electricity_export=None, steel_export=None, *, steel_mix=None, variable_mapping=None):
        root = resource_root() / "examples/gcam-output"
        if steel_export is not None and steel_mix is not None:
            raise ValueError("Choose a raw steel export or a prepared steel mix, not both")
        paths = {"electricity": Path(electricity_export) if electricity_export else root / "electricity.csv",
                 "steel": Path(steel_mix) if steel_mix else Path(steel_export) if steel_export else root / "steel.csv"}
        mapping = variable_mapping or configuration("variables.json")
        self.tables = {"electricity": read_export(paths["electricity"], mapping["electricity"])}
        if steel_mix is None:
            self.tables["steel"] = read_export(paths["steel"], mapping["steel"])
        else:
            self.tables["steel"] = read_prepared_mix(paths["steel"])
        self.sources = {kind: {"file": path.name, "sha256": sha256(path)} for kind, path in paths.items()}
        self.sources["variable_concordance"] = mapping
        self.sources["steel_input_stage"] = "previously prepared shares" if steel_mix else "GCAM physical output"

    def available(self):
        return {kind: table[["Scenario", "Region", "Year"]].drop_duplicates().to_dict("records")
                for kind, table in self.tables.items()}

    def select(self, scenario, year, *, region_mapping=None, profile="eu-clusters", kinds=("electricity", "steel")):
        if not kinds or not set(kinds) <= {"electricity", "steel"}:
            raise ValueError("Select electricity, steel, or both")
        regions = region_mapping if region_mapping is not None else configuration("regions.json")[profile]
        if not regions or any(not isinstance(v, list) or not v for v in regions.values()):
            raise ValueError("Region mapping must map GCAM names to non-empty lists of MARIO names")
        targets = [r for values in regions.values() for r in values]
        if len(targets) != len(set(targets)) or any(not isinstance(r, str) or not r for r in targets):
            raise ValueError("Each MARIO region must have exactly one GCAM source; weighted many-to-one mapping is not inferred")
        mixes, traces = {"electricity": {}, "steel": {}}, []
        for kind, table in self.tables.items():
            if kind not in kinds:
                continue
            for source, destinations in regions.items():
                rows = table[(table.Scenario == scenario) & (table.Year == year) & (table.Region == source)].copy()
                if rows.empty:
                    raise ValueError(f"No {kind} output for {scenario}/{source}/{year}; no interpolation is performed")
                shares = rows.groupby("MarioItem").Share.first().sort_index()
                for destination in destinations:
                    mixes[kind][destination] = shares.copy()
                    trace = rows.copy()
                    trace["Kind"], trace["MarioRegion"] = kind, destination
                    traces.append(trace)
        return MixSelection(scenario, int(year), {k: list(v) for k, v in regions.items()},
                            mixes["electricity"], mixes["steel"], pd.concat(traces, ignore_index=True), self.sources)
