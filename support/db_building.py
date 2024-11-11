#%%
import mario
import yaml
import os 
from ember_remapping import map_ember_to_classification
import pandas as pd

user = 'LR'

with open('paths.yml', 'r') as file:
    paths = yaml.safe_load(file)

folder = paths['onedrive_folder'][user]

#%% Parse raw Exiobase database
mode = 'flows'
db = mario.parse_from_txt(
    path = os.path.join(folder,paths['database']['exiobase']['raw']),
    mode=mode,
    table='SUT'
    )

# %% Get excel to aggregate database (Comment if already done)
# db.get_aggregation_excel('aggregations/raw_to_aggregated.xlsx')

#%% Aggregate database  
db.aggregate(
    io = 'aggregations/raw_to_aggregated.xlsx',
    ignore_nan=True
    )

#%% Update electricity mixes
# Parse ember electricity generation data, map to exiobase and get electricity mix for a given year 
ee_mix = map_ember_to_classification(
    path = os.path.join(folder,paths['database']['ember']),
    classification = 'EXIO3',
    year = 2023,
    mode = 'mix',
)

#%% Map ember electricity generation data to aggregated exiobase electricity commodities
ee_tech_map = {
    'Electricity, fossil': ['Coal', 'Gas', 'Other Fossil'],
    'Electricity, nuclear': ['Nuclear'],
    'Electricity, renewable': ['Bioenergy','Hydro','Other Renewables','Solar','Wind'],
}

#%% Implement changes in matrices
z = db.z
s = db.s

for region in db.get_index('Region'):
    print(region,end=' ')
    for ee_com, ee_techs in ee_tech_map.items():        
        
        new_mix = pd.DataFrame()
        for tech in ee_techs:
            try:
                df = ee_mix.loc[(region,slice(None),tech),'Value'].to_frame().sort_index(axis=0)
                new_mix = pd.concat([new_mix,df], axis=0)
            except:
                print(f'{tech} not found in ember data for {region}')
                continue

        if new_mix.empty:
            print(f'No data found for {ee_com} in {region}')
            continue
        
        else:
            new_mix /= new_mix.sum()

            old_market_share = s.loc[(region, 'Activity', ee_techs),(region,'Commodity',ee_com)].sum().sum()
            new_market_share = new_mix.values * old_market_share # multipliy by old_market_share to account also for electricity from other activities as by-products

            new_market_share_index = pd.MultiIndex.from_arrays([
                [region]*len(new_market_share),
                ['Activity']*len(new_market_share),
                list(new_mix.index.get_level_values(2)),
            ])
            new_market_share_columns = pd.MultiIndex.from_arrays([
                [region],
                ['Commodity'],
                [ee_com],
            ])

            new_market_share = pd.DataFrame(new_market_share, index=new_market_share_index, columns=new_market_share_columns)
            s.loc[new_market_share.index,new_market_share.columns] = new_market_share.values

    print('done')

z.update(s)

db.update_scenarios('baseline',z=z)
db.reset_to_coefficients('baseline')

#%% Splitting "BF-BOF" to disjoint its byproducts from the main product (steel production)

# Adding two new activities to the database to represent the production of "Blast furnace gas" and "Oxygen steel furnace gas"
master_file_path = os.path.join(folder,paths['inventories'],'blastfurnacegas.xlsx')
db.read_add_sectors_excel(master_file_path,read_inventories=True)
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
db.to_txt(os.path.join(folder,paths['database']['exiobase']['aggregated'])) 

# %%
