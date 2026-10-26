"""Small fictional SUT: real GCAM projections, deliberately illustrative inventories."""
import numpy as np
import pandas as pd

from .resources import configuration
from .shocks import STEEL

GWP = {"Carbon dioxide, fossil (air - Emiss)": 1.0, "CH4 (air - Emiss)": 29.8, "N2O (air - Emiss)": 273.0}
HYDROGEN = "Hydrogen production with electrolysis"
CONVENTIONAL = "Manufacture of basic iron and steel and of ferro-alloys and first products thereof"
SCRAP = "Re-processing of secondary steel into new steel"


def demo_database():
    """Return a real MARIO Database with 2 regions, 28 activities and 11 products per region.

    Inventories, final demand and baseline shares are fictional teaching values.
    This fixture demonstrates the coupling equations, not the paper's footprints.
    """
    import mario
    mapping = configuration("variables.json")
    electricity = sorted(set(mapping["electricity"].values()))
    steel = sorted(set(mapping["steel"].values()))
    regions = ["EU-12", "EU-15"]
    activities = electricity + steel + [HYDROGEN]
    commodities = electricity + [STEEL, "Electrolysis hydrogen"]
    all_keys = sorted((r, level, item) for r in regions
                      for level, items in [("Activity", activities), ("Commodity", commodities)] for item in items)
    index = pd.MultiIndex.from_tuples(all_keys, names=["Region", "Level", "Item"])
    columns = pd.MultiIndex.from_tuples([(r, "Consumption category", "Households") for r in regions], names=index.names)
    z = pd.DataFrame(0.0, index=index, columns=index)
    y = pd.DataFrame(0.0, index=index, columns=columns)
    e = pd.DataFrame(0.0, index=list(GWP), columns=index)
    v = pd.DataFrame(0.0, index=["Value added"], columns=index)
    electricity_ghg = {"Coal": 95, "Gas": 55, "Other Fossil": 75, "Bioenergy": 7,
                       "Nuclear": .4, "Hydro": 1.1, "Solar": 3, "Wind": .8, "Other Renewables": 2}
    for region in regions:
        for tech in electricity:
            z.loc[(region, "Activity", tech), (region, "Commodity", tech)] = 1.0
            e.loc[list(GWP)[0], (region, "Activity", tech)] = electricity_ghg[tech]
            y.loc[(region, "Commodity", tech), (region, "Consumption category", "Households")] = 20.0 / len(electricity)
        for i, tech in enumerate(steel):
            share = .6 if tech == CONVENTIONAL else .3 if tech == SCRAP else .1/(len(steel)-2)
            z.loc[(region, "Activity", tech), (region, "Commodity", STEEL)] = share
            power = .0015 if tech == CONVENTIONAL else .004 + .0002*i
            for origin in regions:
                origin_share = .85 if origin == region else .15
                for source in electricity:
                    z.loc[(origin, "Commodity", source), (region, "Activity", tech)] = origin_share*power/len(electricity)
            if "H2" in tech:
                z.loc[(region, "Commodity", "Electrolysis hydrogen"), (region, "Activity", tech)] = .04
            direct = 1.8 if tech == CONVENTIONAL else .15 if tech == SCRAP else .04 + .02*(i % 5)
            e.loc[list(GWP)[0], (region, "Activity", tech)] = direct
        z.loc[(region, "Activity", HYDROGEN), (region, "Commodity", "Electrolysis hydrogen")] = 1.0
        z.loc[(region, "Commodity", "Solar"), (region, "Activity", HYDROGEN)] = .12
        y.loc[(region, "Commodity", STEEL), (region, "Consumption category", "Households")] = 100.0
        y.loc[(region, "Commodity", "Electrolysis hydrogen"), (region, "Consumption category", "Households")] = 1.0
    x = np.linalg.solve(np.eye(len(index))-z.to_numpy(), y.sum(axis=1).to_numpy())
    z_flows = z.mul(x, axis=1)
    e_flows = e.mul(x, axis=1)
    v.loc[:, pd.IndexSlice[:, "Activity", :]] = .1
    units = {
        "Activity": pd.DataFrame({"unit": ["TJ" if a in electricity else "tonnes" for a in sorted(activities)]}, index=sorted(activities)),
        "Commodity": pd.DataFrame({"unit": ["TJ" if c in electricity else "tonnes" for c in sorted(commodities)]}, index=sorted(commodities)),
        "Factor of production": pd.DataFrame({"unit": ["M EUR"]}, index=["Value added"]),
        "Satellite account": pd.DataFrame({"unit": ["tonnes"]*len(GWP)}, index=list(GWP)),
    }
    db = mario.Database(name="Illustrative GCAM-MARIO coupling SUT", table="SUT", Z=z_flows,
                        Y=y, E=e_flows, V=v.mul(x, axis=1),
                        EY=pd.DataFrame(0.0, index=e.index, columns=y.columns), units=units, calc_all=False,
                        source="Fictional inventories and baseline; GCAM inputs are supplied separately")
    db.calc_ghg(gwp=GWP, label="GHG")
    db.reset_to_coefficients("baseline")
    return db
