#%%
import mario
import yaml
import os 
from ember.ember_remapping import map_ember_to_classification
import pandas as pd
import time
import warnings
warnings.filterwarnings("ignore")

#%% Parse raw Exiobase database
# Path should target the folder containing the txt files ("flows")
db = mario.parse_from_txt(
    path = '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/Exiobase/Hybrid/3.3.18_mario_with_va/flows',
    mode='flows',
    table='SUT'
    )

# %% Get excel to aggregate database (Comment if already done) - Comment in case the file is already available
# db.get_aggregation_excel('aggregations/raw_to_aggregated.xlsx')

#%% Aggregate database electricity activities and commodities (Aggregation consistent with EMBER)
db.aggregate(
    io = 'aggregations/raw_to_aggregated.xlsx',
    ignore_nan=True
    )

#%% Update electricity mixes
# Parse ember electricity generation data, map to exiobase and get electricity mix for a given year 
ee_mix = map_ember_to_classification(
    path = 'ember/yearly_full_release_long_format.csv',  # ember yearly electricity data in csv format
    classification = 'EXIO3', # exiobase 3 country classification
    year = None, 
    mode = 'mix', # return the mix of electricity generation (other options are 'mix' and 'values')
)

#%% Change electricity mix from the use side, keeping all techs disaggregated
def change_mix(db, ee_mix, scenario):

    if scenario != 'baseline':
        db.clone_scenario(scenario,'baseline')
    
    u = db.u
    Y = db.Y
    z = db.z

    for region in db.get_index('Region'):
        start = time.time()
        print(region,end=' ')

        u_ee = u.loc[(region,slice(None),sorted(list(set(list(ee_mix.index.get_level_values(-1)))))),:]
        u_index = u_ee.index
        u_ee = u_ee.sum(0).to_frame().T.values

        Y_ee = Y.loc[(region,slice(None),sorted(list(set(list(ee_mix.index.get_level_values(-1)))))),:].sum(0).to_frame().T.values

        region_latest_year = ee_mix.loc[(region,slice(None),slice(None))].index.get_level_values(0).max()
        ember_ee_mix = ee_mix.loc[(region,region_latest_year,sorted(list(set(list(ee_mix.index.get_level_values(-1)))))),'Value'].to_frame().sort_index(axis=0)
        ember_ee_mix.index = pd.MultiIndex.from_arrays([
            [region]*ember_ee_mix.shape[0],
            ['Commodity']*ember_ee_mix.shape[0],
            ember_ee_mix.index.get_level_values(-1)
        ],names=["Region","Level","Item"])

        region_ee_mix = pd.DataFrame(0,index = u_index, columns = ['Value'])
        region_ee_mix.update(ember_ee_mix)
        region_ee_mix = region_ee_mix.values

        new_u_ee = pd.DataFrame(region_ee_mix @ u_ee, index=u_index, columns=u.columns)
        new_Y_ee = pd.DataFrame(region_ee_mix @ Y_ee, index=u_index, columns=Y.columns)

        u.update(new_u_ee)
        Y.update(new_Y_ee)
        print('done in {:.2f} s'.format(time.time()-start))

    z.update(u)
    db.update_scenarios(scenario,z=z)
    db.reset_to_coefficients(scenario)

change_mix(db,ee_mix,'baseline')

#%% Adding energy accounts from Exiobase 3.8.2 2011
exiobase_382 = mario.parse_exiobase(
    path = '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/Exiobase/Monetary/v3.8.2/IOT/IOT_2011_ixi.zip',
    table = 'IOT',
    unit = 'Monetary',
    version = '3.8.2',
)
exiobase_382.aggregate('aggregations/aggr_exio_382.xlsx',ignore_nan=True)

E_iot = exiobase_382.E
E_sut = db.E
new_E_sut = pd.DataFrame(0.0, index=E_iot.index, columns=E_sut.columns)
new_column_levels = pd.MultiIndex.from_arrays([
    E_iot.columns.get_level_values(0),
    ['Activity' for i in range(E_iot.shape[1])],
    E_iot.columns.get_level_values(2)
])

E_iot.columns = new_column_levels

new_E_sut.update(E_iot)
new_units_sut = exiobase_382.units['Satellite account']

db.add_extensions(
    io=new_E_sut,
    units=new_units_sut.loc[new_E_sut.index], # We should only pass the items that are in the new_E_sut
    matrix='E'
)

#%% Splitting "BF-BOF" to disjoint its byproducts from the main product (steel production)
# This procedure is done with the new add sectors method

# Adding two new activities to the database to represent the production of "Blast furnace gas" and "Oxygen steel furnace gas"
master_file_path = 'inventories/blastfurnacegas.xlsx'
db.read_add_sectors_excel(master_file_path,read_inventories=True)  # read_inventories=True because the file is already prepared
db.add_sectors()

# changing emissions of the two sectors
s = db.s
u = db.u
v = db.v
e = db.e
z = db.z

for region in db.get_index('Region'):
    print(region)

    parent_activity = 'Manufacture of basic iron and steel and of ferro-alloys and first products thereof'
    main_commodity = 'Basic iron and steel and of ferro-alloys and first products thereof'
    by_product_commodities = db.add_sectors_master['Commodity'].unique()
    by_product_market_shares = db.s.loc[(region,'Activity',parent_activity),(region,'Commodity',by_product_commodities)].sum().sum()

    # removing production of "Blast furnace gas" and "Oxygen steel furnace gas" from "Manufacture of basic iron"
    s_byprod = s.loc[(region,'Activity',parent_activity),(region,'Commodity',by_product_commodities)]*0
    s_byprod = s_byprod.to_frame().T
    s.update(s_byprod)

    # adding production of "Blast furnace gas" and "Oxygen steel furnace gas" to "Blast furnace gas production"
    for new_act in db.new_activities:
        commodity = db.add_sectors_master.loc[db.add_sectors_master['Activity']==new_act,'Commodity'].values[0]        
        s.loc[(region,'Activity',new_act),(region,'Commodity',commodity)] = 1
    
    # updating use coefficients of main activity based on the sole production of steel
    S_main = db.S.loc[(region,'Activity',parent_activity),(region,'Commodity',main_commodity)]
    u_main = db.U.loc[:,(region,'Activity',parent_activity)]/S_main.sum()
    u.update(u_main)

    v_main = db.V.loc[:,(region,'Activity',parent_activity)]/S_main.sum()
    v.update(v_main)

    e_main = db.E.loc[:,(region,'Activity',parent_activity)]/S_main.sum()
    e.update(e_main)


z.update(s)
z.update(u)

db.update_scenarios('baseline',z=z,v=v,e=e)
db.reset_to_coefficients('baseline')

#%% manually fixing coefficients for re-processing of secondary steel into new steel and self-consumption of steel in Japan and South Korea
steel_coms = 'Basic iron and steel and of ferro-alloys and first products thereof'

z = db.z.copy()
z.loc[('AU','Activity','Re-processing of secondary steel into new steel'),('AU','Commodity','Secondary steel for treatment; Re-processing of secondary steel into new steel')] = 101
z.loc[('JP','Commodity',steel_coms),('JP','Activity',"Manufacture of basic iron and steel and of ferro-alloys and first products thereof")] = 0.2835556621431132
z.loc[('KR','Commodity',steel_coms),('KR','Activity',"Manufacture of basic iron and steel and of ferro-alloys and first products thereof")] = 0.2835556621431132

db.update_scenarios('baseline',z=z)
db.reset_to_coefficients('baseline')

#%% Fix steelmaking market shares
# %% Parsing the raw exiobase database (data from World Steel Association 2023)
# db = mario.parse_from_txt(
#     path='/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/Exiobase/Hybrid/EnergyTranstionEdition/v1.0.1/coefficients',
#     mode='coefficients',
#     table='SUT',
# )

steel_com = 'Basic iron and steel and of ferro-alloys and first products thereof'
steelmaking_routes = {
    'AT': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.907,
        'Re-processing of secondary steel into new steel': 0.093,
    },
    'BE': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.693,
        'Re-processing of secondary steel into new steel': 0.307,
    },
    'BG': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.0,
        'Re-processing of secondary steel into new steel': 1.0,
    },
    'HR': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.0,
        'Re-processing of secondary steel into new steel': 1.0,
    },
    'CZ': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.956,
        'Re-processing of secondary steel into new steel': 0.044,
    },
    'FI': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.62,
        'Re-processing of secondary steel into new steel': 0.38,
    },
    'FR': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.592,
        'Re-processing of secondary steel into new steel': 0.408,
    },
    'DE': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.723,
        'Re-processing of secondary steel into new steel': 0.277,
    },
    'GR': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.0,
        'Re-processing of secondary steel into new steel': 1.0,
    },
    'HU': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.523,
        'Re-processing of secondary steel into new steel': 0.477,
    },
    'IT': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.142,
        'Re-processing of secondary steel into new steel': 0.858,
    },
    'LU': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.0,
        'Re-processing of secondary steel into new steel': 1.0,
    },
    'NL': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 1.0,
        'Re-processing of secondary steel into new steel': 0.0,
    },
    'PL': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.488,
        'Re-processing of secondary steel into new steel': 0.512,
    },
    'PT': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.0,
        'Re-processing of secondary steel into new steel': 1.0,
    },
    'RO': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.532,
        'Re-processing of secondary steel into new steel': 0.468,
    },
    'SK': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.1,
        'Re-processing of secondary steel into new steel': 0.9,
    },
    'SI': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.0,
        'Re-processing of secondary steel into new steel': 1.0,
    },
    'ES': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.281,
        'Re-processing of secondary steel into new steel': 0.719,
    },
    'SE': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.686,
        'Re-processing of secondary steel into new steel': 0.314,
    },
    'TR': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.284,
        'Re-processing of secondary steel into new steel': 0.716,
    },
    'GB': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.796,
        'Re-processing of secondary steel into new steel': 0.204,
    },
    'RU': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.68,
        'Re-processing of secondary steel into new steel': 0.32,
    },
    'CA': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.582,
        'Re-processing of secondary steel into new steel': 0.418,
    },
    'MX': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.066,
        'Re-processing of secondary steel into new steel': 0.934,
    },
    'US': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.317,
        'Re-processing of secondary steel into new steel': 0.683,
    },
    'BR': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.775,
        'Re-processing of secondary steel into new steel': 0.225,
    },
    'WL': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.32,
        'Re-processing of secondary steel into new steel': 0.68,
    },
    'ZA': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.568,
        'Re-processing of secondary steel into new steel': 0.432,
    },
    'WF': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.02,
        'Re-processing of secondary steel into new steel': 0.98,
    },
    'CN': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.901,
        'Re-processing of secondary steel into new steel': 0.099,
    },
    'IN': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.436,
        'Re-processing of secondary steel into new steel': 0.564,
    },
    'JP': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.738,
        'Re-processing of secondary steel into new steel': 0.262,
    },
    'KR': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.705,
        'Re-processing of secondary steel into new steel': 0.295,
    },
    'WA': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.488,
        'Re-processing of secondary steel into new steel': 0.512,
    },
    'WE': {
        'Manufacture of basic iron and steel and of ferro-alloys and first products thereof': 0.41,
        'Re-processing of secondary steel into new steel': 0.59,
    },
}

z = db.z.copy()
s = db.s.copy()

for country, routes in steelmaking_routes.items():
    for route,share in routes.items():
        print(country, route, share)
        s.loc[(country,slice(None),route),(country,slice(None),steel_com)] = share

z.update(s)

db.update_scenarios('baseline',z=z)
db.reset_to_coefficients('baseline')

# db.to_txt(
#     path='/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/Exiobase/Hybrid/EnergyTranstionEdition/v1.0.1',
#     flows=False,
#     coefficients=True,
# )

# %% Export aggregated database to txt
db.to_txt(
    '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/Exiobase/Hybrid/EnergyTranstionEdition/v0.1.2',
) 

# %%
db.f.loc['Carbon dioxide, fossil (air - Emiss)',(slice(None),'Commodity',steel_com)]
# %%
