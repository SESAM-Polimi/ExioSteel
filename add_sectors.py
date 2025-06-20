#%%
import mario
import yaml
import os

master_file_path = '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/IAM COMPACT Study 9/Model/Master.xlsx'

#%%
db = mario.parse_from_txt(
    path='/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/Exiobase/Hybrid/EnergyTranstionEdition/v0.1.2/flows',
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
db.to_txt(
    path='/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/Exiobase/Hybrid/EnergyTranstionEdition/v1.0.1',
    flows=False,
    coefficients=True,
)
#%%
# # %%
import pandas as pd
df = pd.read_excel('support/gcam_data/Steel_mixes.xlsx',index_col=[0,1,2]).reset_index()
df.to_excel('support/gcam_data/Steel_mixes_noindex.xlsx',index=False)
# %%
