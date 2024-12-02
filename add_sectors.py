#%%
import mario
import yaml
import os

user = 'LR'
with open('support/paths.yml', 'r') as file:
    paths = yaml.safe_load(file)

onedrive_folder = paths['onedrive_folder'][user]
master_file_path = os.path.join(onedrive_folder,paths['inventories'],'steelmaking_routes.xlsx')

#%%
db = mario.parse_from_txt(
    path=os.path.join(onedrive_folder,paths['database']['exiobase']['aggregated'],'flows'),
    mode='flows',
    table='SUT',
)

#%%
# db.get_add_sectors_excel(master_file_path)

#%%
db.read_add_sectors_excel(master_file_path,read_inventories=True)

#%%
# db.read_inventory_sheets(master_file_path)

#%%
db.add_sectors()

# %% Export aggregated database to txt
db.to_txt(os.path.join(onedrive_folder,paths['database']['exiobase']['extended']))

# %%
ghgs = {
    'Carbon dioxide, fossil (air - Emiss)':1,
    'CH4 (air - Emiss)':26,
    'N2O (air - Emiss)':298
    }

steel_acts = [
    'Manufacture of basic iron and steel and of ferro-alloys and first products thereof',
    'DRI-NG',
    'DRI-NG-CCS',
    'DRI-COAL',
    'DRI-COAL-CCS',
    'DRI-H2',
    'DRI-BECCS',
    'AEL-EAF',
    'EAF-NG',
    'EAF-NG-CCS',
    'EAF-COAL',
    'EAF-COAL-CCS',
    'EAF-H2',
    'EAF-BECCS',
    'SAF-BOF-NG',
    'SAF-BOF-H2',
    'SAF-BOF-BECCS',
    'MOE',
    'SR-BOF',
    'SR-BOF-CCS',
    'BF-BOF-CCS-73%',
    'BF-BOF-CCS-86%',
    'BF-BOF-BECCSmax',
    'BF-BOF-BECCSmin',
    'Re-processing of secondary steel into new steel',
    ]

#%%
f = db.f.loc[ghgs.keys(),(slice(None),'Activity',steel_acts)]
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


#%%
import numpy as np
import pandas as pd

e = db.e.loc[ghgs.keys(),:]
for ghg,gwp in ghgs.items():
    e.loc[ghg,:] *= gwp

e = e.sum(0)
e = e.to_frame().T

f_ex = np.diagflat(e.values) @ db.w.values
f_ex = pd.DataFrame(f_ex, index=e.columns, columns= e.columns)

f_ex_filtered = f_ex.loc[(slice(None),'Activity',slice(None)),(slice(None),'Activity',['Steel production through 100%H2-DR','EAF-H2'])]
f_ex_filtered.to_clipboard()

# %%
