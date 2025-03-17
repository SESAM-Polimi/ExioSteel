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
    year = 2023, 
    mode = 'mix', # return the mix of electricity generation (other options are 'mix' and 'values')
)

#%% Change electricity mix from the use side, keeping all techs disaggregated
z = db.z
u = db.u
Y = db.Y

ee_com = ['Coal', 'Gas', 'Other Fossil','Nuclear','Bioenergy','Hydro','Other Renewables','Solar','Wind']

for region_from in db.get_index('Region'):
    start = time.time()
    print(region_from,end=' ')

    u_ee = u.loc[(region_from,slice(None),ee_com),:]
    u_index = u_ee.index
    u_ee = u_ee.sum(0).to_frame().T.values

    Y_ee = Y.loc[(region_from,slice(None),ee_com),:].sum(0).to_frame().T.values

    ember_ee_mix = ee_mix.loc[(region_from,slice(None),ee_com),'Value'].to_frame()
    ember_ee_mix.index = pd.MultiIndex.from_arrays([
        [region_from]*ember_ee_mix.shape[0],
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
db.update_scenarios('baseline',z=z)
db.reset_to_coefficients('baseline')


#%% check electricity mix
db.z.loc[('IT','Commodity',ee_com),:]/db.u.loc[('IT','Commodity',ee_com),:].sum(0)

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

#%%
ghgs = {
    'Carbon dioxide, fossil (air - Emiss)':1,
    'CH4 (air - Emiss)':26,
    'N2O (air - Emiss)':298
    }

f = db.f.loc[ghgs.keys(),(slice(None),'Activity',parent_activity)]
for ghg,gwp in ghgs.items():
    f.loc[ghg,:] *= gwp

f = f.sum(0)
f = f.to_frame()    
f.reset_index(inplace=True)
f.columns = ['Region','Item','Activity','Value']
f = f.drop('Item',axis=1)
f.set_index(['Region','Activity'],inplace=True)
f = f.unstack()
f = f.droplevel(0,axis=1)
f.to_clipboard()

# %% Export aggregated database to txt
db.to_txt(
    '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/ExioSteel/Raw_aggregated'
) 

# %%
