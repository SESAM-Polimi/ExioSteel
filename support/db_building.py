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
    path = '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/Exiobase Hybrid 3.3.18 with VA/flows',
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
def change_mix(db, region, mix, scenario):

    if scenario != 'baseline':
        db.clone_scenario(scenario,'baseline')
    
    u = db.u
    Y = db.Y
    z = db.z

    for region in db.get_index('Region'):
        start = time.time()
        print(region,end=' ')

        u_ee = u.loc[(region,slice(None),ee_mix.index),:]
        u_index = u_ee.index
        u_ee = u_ee.sum(0).to_frame().T.values

        Y_ee = Y.loc[(region,slice(None),ee_mix.index),:].sum(0).to_frame().T.values

        region_latest_year = ee_mix.loc[(region,slice(None),slice(None))].index.get_level_values(0).max()
        ember_ee_mix = ee_mix.loc[(region,region_latest_year,ee_mix.index),'Value'].to_frame().sort_index(axis=0)
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

change_electricity_mix(db,ee_mix,'baseline')

#%% Splitting "BF-BOF" to disjoint its byproducts from the main product (steel production)
# This procedure is done with the new add sectors method

# Adding two new activities to the database to represent the production of "Blast furnace gas" and "Oxygen steel furnace gas"
master_file_path = 'inventories/blastfurnacegas.xlsx'
db.read_add_sectors_excel(master_file_path,read_inventories=True)  # read_inventories=True because the file is already prepared
db.add_sectors()

# changing emissions of the two sectors
e = db.e
s = db.s
z = db.z

for region in db.get_index('Region'):

    parent_activity = db.add_sectors_master['Parent Activity'].unique()[0]
    by_product_commodities = db.add_sectors_master['Commodity'].unique()
    by_product_market_shares = db.s.loc[(region,'Activity',parent_activity),(region,'Commodity',by_product_commodities)].sum().sum()

    # we estimated the allocation of emissions over multiple by-products more or less halved the real emission intensity of the parent activity
    if by_product_market_shares != 0: 
        # doubling emissions of "Manufacture of basic iron" activity if it produces by-products
        e_cols_main = e.loc[:,(region,slice(None),parent_activity)]*2  
        e.update(e_cols_main) 

        # removing production of "Blast furnace gas" and "Oxygen steel furnace gas" from "Manufacture of basic iron"
        s_byprod = s.loc[(region,'Activity',parent_activity),(region,'Commodity',by_product_commodities)]*0
        s.update(s_byprod)

        # null ghgs emissions for "Blast furnace gas production" and "Oxygen steel furnace gas production" activities
        e_cols_new  = e.loc[:,(region,slice(None),db.new_activities)]*0
        e.update(e_cols_new) 

        for new_act in db.new_activities:
            # adding production of "Blast furnace gas" and "Oxygen steel furnace gas" to "Blast furnace gas production"
            commodity = db.add_sectors_master.loc[db.add_sectors_master['Activity']==new_act,'Commodity'].values[0]
            
            s.loc[(region,'Activity',new_act),(region,'Commodity',commodity)] = 1

z.update(s)
db.update_scenarios('baseline',z=z,e=e)
db.reset_to_coefficients('baseline')

#%% Adding energy accounts from Exiobase 3.8.2 2011
exiobase_382 = mario.parse_exiobase(
    path = '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/Exiobase 3.8.2/IOT/IOT_2011_ixi.zip',
    table = 'IOT',
    unit = 'Monetary',
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

# %% Export aggregated database to txt
db.to_txt(
    '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/ExioSteel/Raw_aggregated'
) 

# %%
