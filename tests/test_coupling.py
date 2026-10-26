import copy
import json

import numpy as np
import pandas as pd
import pytest

from gcam_mario import Projections, demo_database, apply_mix_shock, explain_coefficients
from gcam_mario.demo import HYDROGEN, CONVENTIONAL
from gcam_mario.inputs import read_export
from gcam_mario.results import export_run
from gcam_mario.shocks import STEEL
from gcam_mario.resources import resource_root, sha256


@pytest.fixture
def db():
    import mario
    mario.set_log_verbosity("error")
    return demo_database()


def test_every_bundled_scenario_year_has_complete_normalised_mix():
    p = Projections()
    for scenario in p.tables["electricity"].Scenario.unique():
        for year in p.tables["electricity"].Year.unique():
            s = p.select(scenario, int(year))
            for mix in [*s.electricity.values(), *s.steel.values()]:
                assert mix.sum() == pytest.approx(1)
                assert (mix >= 0).all()


def test_raw_physical_values_are_traceable_and_many_to_one_is_summed():
    s = Projections().select("NDC_LTT_CBAM", 2030)
    raw = pd.read_csv(resource_root()/"examples/gcam-output/electricity.csv", float_precision="round_trip")
    row = s.trace.query("Kind=='electricity' and Region=='EU-15' and MarioItem=='Wind'").iloc[0]
    assert raw.iloc[int(row.SourceRow)-2]["2030"] == row.Value
    assert row.Unit == "EJ/yr"
    assert row.Share == pytest.approx(row.Value/row.GroupTotal)
    rows = s.trace.query("Kind=='steel' and Region=='EU-15' and MarioItem=='Re-processing of secondary steel into new steel'")
    assert len(rows) == 4
    assert rows.MappedValue.iloc[0] == pytest.approx(rows.Value.sum())
    assert rows.Share.iloc[0] == pytest.approx(rows.Value.sum()/rows.GroupTotal.iloc[0])
    assert (s.trace.query("Kind=='steel' and MappedValue==0").Share == 0).all()


def test_electricity_shock_conserves_each_origin_and_consumer_and_excludes_electrolysis(db):
    s = Projections().select("NDC_LTT_CBAM", 2030, kinds=("electricity",))
    u = db.query(matrices="u", scenarios="baseline").copy()
    yc = db.query(matrices="Yc", scenarios="baseline").copy()
    baseline_s = db.query(matrices="s", scenarios="baseline").copy()
    apply_mix_shock(db, s, name="electricity", steel=False)
    after = db.query(matrices="u", scenarios="electricity")
    after_y = db.query(matrices="Yc", scenarios="electricity")
    for r, mix in s.electricity.items():
        index = [(r, "Commodity", item) for item in mix.index]
        np.testing.assert_allclose(u.loc[index].sum(), after.loc[index].sum(), rtol=0, atol=1e-15)
        np.testing.assert_allclose(yc.loc[index].sum(), after_y.loc[index].sum(), rtol=0, atol=1e-12)
        consumer = (r, "Activity", CONVENTIONAL)
        np.testing.assert_allclose(after.loc[index, consumer], mix*u.loc[index, consumer].sum())
    excluded = u.columns[u.columns.get_level_values(2) == HYDROGEN]
    pd.testing.assert_frame_equal(u[excluded], after[excluded])
    pd.testing.assert_frame_equal(u, db.query(matrices="u", scenarios="baseline"))
    pd.testing.assert_frame_equal(baseline_s, db.query(matrices="s", scenarios="electricity"))


def test_steel_shock_changes_supply_shares_and_keeps_inventories_and_final_demand(db):
    s = Projections().select("NDC_LTT_CBAM", 2050, kinds=("steel",))
    before = {m: db.query(matrices=m, scenarios="baseline").copy() for m in ["s", "u", "ea", "Yc"]}
    apply_mix_shock(db, s, name="steel", electricity=False)
    after = db.query(matrices="s", scenarios="steel")
    np.testing.assert_allclose(before["s"].sum(), after.sum(), rtol=0, atol=1e-14)
    for region, mix in s.steel.items():
        rows = [(region, "Activity", item) for item in mix.index]
        col = (region, "Commodity", STEEL)
        np.testing.assert_allclose(after.loc[rows, col], mix*before["s"].loc[rows, col].sum())
    for matrix in ["u", "ea", "Yc"]:
        pd.testing.assert_frame_equal(before[matrix], db.query(matrices=matrix, scenarios="steel"))


def test_scenarios_are_independent_and_inspection_matches_actual_matrices(db):
    p = Projections()
    first = p.select("NDC_LTT_CBAM", 2030)
    second = p.select("NDC_LTT_CBAM-G_SUBS_HI", 2050)
    apply_mix_shock(db, first, name="first")
    apply_mix_shock(db, second, name="second")
    other = demo_database()
    apply_mix_shock(other, second, name="second")
    for matrix in ["u", "s", "Yc"]:
        pd.testing.assert_frame_equal(db.query(matrices=matrix, scenarios="second"), other.query(matrices=matrix, scenarios="second"))
    inspection = explain_coefficients(db, second, scenario="second")
    np.testing.assert_allclose(inspection.after, inspection.share_times_original_total, atol=1e-14)


def test_missing_label_or_invalid_mix_leaves_no_partial_scenario(db):
    s = Projections().select("NDC_LTT_CBAM", 2030)
    s.electricity["EU-12"] = s.electricity["EU-12"].rename(index={"Wind": "Unknown"})
    with pytest.raises(ValueError, match="Missing MARIO"):
        apply_mix_shock(db, s, name="bad")
    assert "bad" not in db.scenarios
    s = Projections().select("NDC_LTT_CBAM", 2030)
    s.steel["EU-12"] *= .9
    with pytest.raises(ValueError, match="sum to one"):
        apply_mix_shock(db, s, name="bad")
    assert "bad" not in db.scenarios


@pytest.mark.parametrize("problem", ["negative", "unit", "duplicate", "unknown", "missing", "zero", "nan"])
def test_ambiguous_or_invalid_gcam_exports_fail_before_shocking(tmp_path, problem):
    frame = pd.DataFrame({"Scenario": ["s", "s"], "Region": ["r", "r"],
                          "Variable": ["a", "b"], "Unit": ["EJ/yr", "EJ/yr"], "2030": [1., 3.]})
    if problem == "negative": frame.loc[0, "2030"] = -1
    if problem == "unit": frame.loc[0, "Unit"] = "TWh/yr"
    if problem == "duplicate": frame = pd.concat([frame, frame.iloc[:1]])
    if problem == "unknown": frame.loc[0, "Variable"] = "surprise"
    if problem == "missing": frame = frame.iloc[:1]
    if problem == "zero": frame["2030"] = 0
    if problem == "nan": frame.loc[0, "2030"] = np.nan
    path = tmp_path/"export.csv"
    frame.to_csv(path, index=False)
    with pytest.raises(ValueError): read_export(path, {"a": "A", "b": "B"})


def test_missing_year_and_ambiguous_regions_are_not_inferred():
    p = Projections()
    with pytest.raises(ValueError, match="No electricity"):
        p.select("NDC_LTT_CBAM", 2031)
    with pytest.raises(ValueError, match="exactly one"):
        p.select("NDC_LTT_CBAM", 2030, region_mapping={"EU-12": ["EU"], "EU-15": ["EU"]})


def test_output_contains_physical_input_trace_actual_edits_and_integrity_hashes(db, tmp_path):
    s = Projections().select("NDC_LTT_CBAM", 2030)
    report = apply_mix_shock(db, s, name="example")
    out = export_run(db, [report], tmp_path/"result", database_kind="demo")
    manifest = json.loads((out/"run.json").read_text())
    assert manifest["inventories_are_illustrative"]
    assert set(manifest["files"]) == {"footprints.csv", "input_interpretation.csv", "mixes.csv", "coefficients_before_after.csv"}
    for file, digest in manifest["files"].items(): assert sha256(out/file) == digest
    inspection = pd.read_csv(out/"coefficients_before_after.csv")
    np.testing.assert_allclose(inspection.after, inspection.share_times_original_total, atol=1e-14)
    f = pd.read_csv(out/"footprints.csv")
    assert len(f) == 4 and np.isfinite(f.ghg_tco2eq_per_tonne_steel).all()
    with pytest.raises(ValueError, match="not empty"):
        export_run(db, [report], out, database_kind="demo")
