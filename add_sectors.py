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
db.u.loc[:,('CN','Activity',db.new_activities)].sum(0)
# %%
