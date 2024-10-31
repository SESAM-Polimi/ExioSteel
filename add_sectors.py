#%%
from fiona.core.db_builder import DB_builder
import yaml
import os

user = 'LR'

with open('support/paths.yml', 'r') as file:
    paths = yaml.safe_load(file)

onedrive_folder = paths['onedrive_folder'][user]
git_folder = paths['git_folder'][user]

master_file_path = os.path.join(git_folder,'support/add_sectors/master.xlsx')

#%%
db = DB_builder(
    sut_path=os.path.join(onedrive_folder,paths['database']['exiobase']['aggregated'],'flows'),
    sut_mode='flows',
    master_file_path=master_file_path,
    sut_format='txt',
    read_master_file=True,
)

#%%
# db.read_master_template(master_file_path,get_inventories=True)

#%%
db.read_inventories(master_file_path,check_errors=False)

#%%
db.add_inventories('excel')

# %%
db.sut.u.loc[:,('CN','Activity',db.new_activities)].sum(0)
# %%
