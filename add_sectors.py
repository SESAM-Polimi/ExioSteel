#%%
import mario
import yaml
import os

user = 'LR'

with open('support/paths.yml', 'r') as file:
    paths = yaml.safe_load(file)

onedrive_folder = paths['onedrive_folder'][user]
git_folder = paths['git_folder'][user]

master_file_path = os.path.join(git_folder,'support/add_sectors/master.xlsx')

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

# %%
ghgs = {
    'Carbon dioxide, fossil (air - Emiss)':1,
    'CH4 (air - Emiss)':26,
    'N2O (air - Emiss)':298
    }
steel_acts = [
    'Steel production through 100%H2-DR',
    'Steel production with H2 inj to BF',
    'Steel production with charcoal inj to BF',
    'Steel production with charcoal inj to BF + CCUS',
    'Steel production through NG-DR',
    'Steel production BF-BOF + CCUS'
    ]

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
# %%
