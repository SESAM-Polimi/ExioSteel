#%% 
import pandas as pd

mixes = {
    'Steel': {
        'sheet_name': 'SteelProdTech',
        'regions': {
            'EU-15': ['IT','ES','PT','FR','BE','LU','NL','DE','DK','SE','FI','AT','IE','GR','MT'],
            'EU-12': ['PL','CZ','SK','SI','HU','RO','BG','HR','LT','LV','EE','CY'],
        },
        'variables': {
            'Production|Steel|BF_BOF':'Manufacture of basic iron and steel and of ferro-alloys and first products thereof',
            'Production|Steel|EAF_scrap_fossil_NG_finish':'Re-processing of secondary steel into new steel',
            'Production|Steel|DRI_EAF_NG':'DRI-EAF-NG',
            'Production|Steel|DRI_SAF_BOF_NG':'DRI-SAF-BOF-NG',
            'Production|Steel|BF_BOF_BECCSmax':'BF-BOF-BECCSmax',
            'Production|Steel|BF_BOF_BECCSmin':'BF-BOF-BECCSmin',
            'Production|Steel|BF_BOF_CCS_73%':'BF-BOF-CCS-73%',
            'Production|Steel|BF_BOF_CCS_86%':'BF-BOF-CCS-86%',
            'Production|Steel|DRI_EAF_BECCS':'DRI-EAF-BECCS',
            'Production|Steel|DRI_EAF_H2':'DRI-EAF-H2',
            'Production|Steel|DRI_EAF_NG_CCS':'DRI-EAF-NG-CCS',
            'Production|Steel|DRI_EAF_coal_CCS':'DRI-EAF-COAL-CCS',
            'Production|Steel|DRI_SAF_BOF_BECCS':'DRI-SAF-BOF-BECCS',
            'Production|Steel|DRI_SAF_BOF_H2':'DRI-SAF-BOF-H2',
            'Production|Steel|EAF_scrap_bio_NG_finish':'Re-processing of secondary steel into new steel',
            'Production|Steel|SR_BOF':'SR-BOF',
            'Production|Steel|SR_BOF_CCS':'SR-BOF-CCS',
            'Production|Steel|MOE':'MOE',
            'Production|Steel|AEL_EAF':'AEL-EAF',
            'Production|Steel|EAF_scrap_bio_elec_finish':'Re-processing of secondary steel into new steel',
            'Production|Steel|EAF_scrap_fossil_elec_finish':'Re-processing of secondary steel into new steel',
        }
    },
    'Electricity': {
        'sheet_name': 'full_data',
        'regions': {
            'EU-15': ['IT','ES','PT','FR','BE','LU','NL','DE','DK','SE','FI','AT','IE','GR','MT'],
            'EU-12': ['PL','CZ','SK','SI','HU','RO','BG','HR','LT','LV','EE','CY'],
        },
        'variables': {
            'Secondary Energy|Electricity|Biomass':'Bioenergy',
            'Secondary Energy|Electricity|Coal':'Coal',
            'Secondary Energy|Electricity|Gas':'Gas',
            'Secondary Energy|Electricity|Hydro':'Hydro',
            'Secondary Energy|Electricity|Geothermal':'Other Renewables',
            'Secondary Energy|Electricity|Nuclear':'Nuclear',
            'Secondary Energy|Electricity|Oil':'Other Fossil',
            'Secondary Energy|Electricity|Solar':'Solar',
            'Secondary Energy|Electricity|Wind':'Wind',
        }
    }
}

scenarios = ['NDC_LTT','NDC_LTT_CBAM','NDC_LTT_CBAM-G_SUBS','NDC_LTT_CBAM-G_SUBS_HI']
path = '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/IAM COMPACT Study 9/Data/GCAM/GCAM data.xlsx'
indices = ['Scenario','Region','Variable']
to_drop = ['Model','Unit']

# %%
for commodity,info in mixes.items():
    df = pd.read_excel(path,sheet_name=info['sheet_name'])
    df = df.query("Region in @info['regions'] & Scenario in @scenarios & Variable in @info['variables'].keys()") 
    df = df.drop(columns=to_drop)
    
    df['Variable'] = df['Variable'].map(info['variables'])
    df.set_index(indices,inplace=True)

    df = df.div(df.groupby(['Scenario', 'Region']).transform('sum'))
    df = df.groupby(indices).sum()

    df.to_excel("{}_mixes.xlsx".format(commodity))

# %%
