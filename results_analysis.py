#%%
import mario
import yaml
import os

db = mario.parse_from_txt(
    path='/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/IAM COMPACT Study 9/Model/Extended/coefficients',
    mode='coefficients',
    table='SUT',
)

# %%  List activities and satellite accounts
ghgs = {
    'Carbon dioxide, fossil (air - Emiss)':1,
    'CH4 (air - Emiss)':29.8,
    'N2O (air - Emiss)':273
    }
energy = ['Energy Carrier Supply: Total']

steel_acts = [
    'Manufacture of basic iron and steel and of ferro-alloys and first products thereof',
    'Manufacturing of steam reformer',
    'Manufacturing of electrolyser',
    'Hydrogen production with steam reforming',
    'Hydrogen production with steam reforming with CCS',
    'Hydrogen production with coal gasification',
    'Hydrogen production with coal gasification with CCS',
    'Hydrogen production with electrolysis',
    'DRI-EAF-NG',
    'DRI-EAF-NG-CCS',
    'DRI-EAF-COAL',
    'DRI-EAF-COAL-CCS',
    'DRI-EAF-H2',
    'DRI-EAF-BECCS',
    'DRI-SAF-BOF-NG',
    'DRI-SAF-BOF-H2',
    'DRI-SAF-BOF-BECCS',
    'SR-BOF',
    'SR-BOF-CCS',
    'BF-BOF-CCS-73%',
    'BF-BOF-CCS-86%',
    'BF-BOF-BECCSmax',
    'BF-BOF-BECCSmin',
    'AEL-EAF',
    'MOE'
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

#%%
f = db.f.loc[energy,(slice(None),'Activity',steel_acts)]
