#%%
import mario
import yaml
import os

master_file_path = 'support/inventories/steelmakingroutes.xlsx'

#%%
db = mario.parse_from_txt(
    path='/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/ExioSteel/Raw_aggregated/flows',
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
    '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/ExioSteel/Extended'
)

# %%  List sectors and emission accounts
ghgs = {
    'Carbon dioxide, fossil (air - Emiss)':1,
    'CH4 (air - Emiss)':26,
    'N2O (air - Emiss)':298
    }

steel_acts = [
    'Manufacture of basic iron and steel and of ferro-alloys and first products thereof',
    'Steel production through 100%H2-DR',
    'Steel production with H2 inj to BF',
    'Steel production with charcoal inj to BF',
    'Steel production with charcoal inj to BF + CCUS',
    'Steel production through NG-DR',
    'Steel production BF-BOF + CCUS',
    ]

#%% Calculate footprints of aggregated GHGs
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
f.to_excel('results/footprints.xlsx') # export to excel

# %%
