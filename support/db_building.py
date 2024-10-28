#%%
import mario
import yaml
import os 
from ember_remapping import map_ember_to_classification

user = 'LR'

with open('paths.yml', 'r') as file:
    paths = yaml.safe_load(file)

folder = paths['onedrive_folder'][user]

#%% Parse raw Exiobase database
mode = 'flows'
raw_db = mario.parse_from_txt(
    path = os.path.join(folder,paths['database']['exiobase']['raw']),
    mode=mode,
    table='SUT'
    )

#%% Parse ember electricity generation data, map to exiobase and get electricity mix for a given year 
ee_mix = map_ember_to_classification(
    path = os.path.join(folder,paths['database']['ember']),
    classification = 'EXIO3',
    year = 2023,
    mode = 'mix',
)

# %% Get excel to aggregate database (Comment if already done)
# raw_db.get_aggregation_excel('aggregations/raw_to_aggregated.xlsx')

#%% Aggregate database  
aggregated_db = raw_db.aggregate(
    io = 'aggregations/raw_to_aggregated.xlsx',
    inplace=False, 
    ignore_nan=True
    )

#%%
z = aggregated_db.z
s = aggregated_db.s

for region in aggregated_db.get_index('Region'):
    print(region,end=' ')
    new_mix = ee_mix.loc[(region,slice(None),slice(None)),'Value'].to_frame().sort_index(axis=0) 
    new_mix.index = new_mix.index.get_level_values(2)
    s.loc[(region, 'Activity', new_mix.index),(region,'Commodity','Electricity')] = new_mix.values  # check if commodity electricity is called "Electricity" in aggregation excel file
    print('done')

z.update(s)

aggregated_db.update_scenarios('baseline',z=z)
aggregated_db.reset_to_coefficients('baseline')

# %% Export aggregated database to txt
aggregated_db.to_txt(os.path.join(folder,paths['database']['exiobase']['aggregated'])) 

# %%
