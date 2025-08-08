#%% Setting the ground
import mario
import pandas as pd
import time
import numpy as np
import warnings
import os
warnings.filterwarnings("ignore")

aggregated = False
scenarios = [
    'NDC_LTT',
    'NDC_LTT_CBAM',
    'NDC_LTT_CBAM-G_SUBS',
    'NDC_LTT_CBAM-G_SUBS_HI'
    ]
years = range(2020,2051,5)
ghgs = {'Carbon dioxide, fossil (air - Emiss)':1,'CH4 (air - Emiss)':29.8,'N2O (air - Emiss)':273}
energy = ['Energy Carrier Supply: Total']

regions = {
    True: {
        'EU-15': ['EU-15'],
        'EU-12': ['EU-12'],
        'Africa_Northern': ['WF'],
        'Central America and Caribbean': ['WL'],
        'Colombia': ['WL'],
        'South America_Southern': ['WL'],
        'Australia_NZ': ['AU'],
        'Brazil': ['BR'],
        'Canada': ['CA'],
        'Central Asia': ['WA'],
        'Pakistan': ['WA'],
        'South Asia': ['WA'],
        'Southeast Asia': ['WA'],
        'Taiwan': ['WA'],
        'China': ['CN'],
        'Europe_Eastern': ['WE'],
        'India': ['IN'],
        'Indonesia': ['ID'],
        'Japan': ['JP'],
        'Mexico': ['MX'],
        'Middle East': ['WM'],
        'Russia': ['RU'],
        'South Africa': ['ZA'],
        'South Korea': ['KR'],
        'USA': ['US'],
    },
    False: {
        'EU-15': ['IT','ES','PT','FR','BE','LU','NL','DE','DK','SE','FI','AT','IE','GR','MT'],
        'EU-12': ['PL','CZ','SK','SI','HU','RO','BG','HR','LT','LV','EE','CY'],
        'Africa_Northern': ['WF'],
        'South America_Southern': ['WL'],
        'Australia_NZ': ['AU'],
        'Brazil': ['BR'],
        'Canada': ['CA'],
        'Southeast Asia': ['WA'],
        'China': ['CN'],
        'Europe_Eastern': ['WE'],
        'European Free Trade Association': ['NO','CH'],
        'India': ['IN'],
        'Indonesia': ['ID'],
        'Japan': ['JP'],
        'Mexico': ['MX'],
        'Middle East': ['WM'],
        'Russia': ['RU'],
        'South Africa': ['ZA'],
        'South Korea': ['KR'],
        'USA': ['US'],
    }
}

EU27 = ['IT','ES','PT','FR','BE','LU','NL','DE','DK','SE','FI','AT','IE','GR','MT','PL','CZ','SK','SI','HU','RO','BG','HR','LT','LV','EE','CY']
steel_com = 'Basic iron and steel and of ferro-alloys and first products thereof'
results_folder = '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/IAM COMPACT Study 9/Results'
steel_acts = ['AEL-EAF','BF-BOF-BECCSmax','BF-BOF-BECCSmin','BF-BOF-CCS-73%','BF-BOF-CCS-86%','DRI-EAF-BECCS','DRI-EAF-COAL-CCS','DRI-EAF-H2','DRI-EAF-NG','DRI-EAF-NG-CCS','DRI-SAF-BOF-BECCS','DRI-SAF-BOF-H2','DRI-SAF-BOF-NG','MOE','Manufacture of basic iron and steel and of ferro-alloys and first products thereof','Re-processing of secondary steel into new steel','SR-BOF','SR-BOF-CCS']
h2_coms = ['Electrolysis hydrogen','Coal gasification hydrogen','Coal gasification hydrogen with CCS','Steam reforming hydrogen','Steam reforming hydrogen with CCS']
h2_acts = [
    'Hydrogen production with coal gasification',
    'Hydrogen production with coal gasification with CCS',
    'Hydrogen production with electrolysis',
    'Hydrogen production with steam reforming',
    'Hydrogen production with steam reforming with CCS'
    ]

#%% Parse Exiobase database extended to new sectors as well as mixes projections for steel and electricity
db = mario.parse_from_txt(
    path='/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/Exiobase/Hybrid/EnergyTranstionEdition/v1.0.1/coefficients',
    mode='coefficients',
    table='SUT',
)

#%% Read electricity and steel mixes
ee_mixes = pd.read_excel('support/gcam_data/Electricity_mixes.xlsx',index_col=[0,1,2])
steel_mixes = pd.read_excel('support/gcam_data/Steel_mixes.xlsx',index_col=[0,1,2])
steel_cons = pd.read_excel('support/gcam_data/Steel_consumption.xlsx',index_col=[0,1,2])
steel_imports = pd.read_excel('support/gcam_data/Steel_imports.xlsx')

#%% Implement shocks and perform calculations
scemarios_done = [
    'NDC_LTT_2020',
    'NDC_LTT_2025',
]

db.clone_scenario('baseline','shock')
for scenario in scenarios:
    
    if scenario != 'baseline':
        for year in years:

            if f'{scenario}_{year}' in scemarios_done:
                print(f'Scenario {scenario} for year {year} already done, skipping...')

            else:
                print('Processing',scenario,year)
                start_scen = time.time()
                z = db.z
                u = db.u
                s = db.s
                Y = db.Y

                # Update electricity and steel mixes
                for r_cluster,r_lists in regions[aggregated].items():
                    for region_from in r_lists:

                        start = time.time()
                        print("   ",region_from,end=' ')

                        def rearrange_mixes(mixes,scen,rc,rf,yy):
                            try:
                                mix_yr = mixes.loc[(scen,rc,slice(None)),str(yy)]
                            except:
                                mix_yr = mixes.loc[(scen,rc,slice(None)),yy]
                            if isinstance(mix_yr,pd.Series):
                                mix_yr = mix_yr.to_frame()
                            mix_yr.columns = [yy]
                            mix_yr = mix_yr.stack().to_frame()
                            mix_yr.columns = ['Value']

                            mix_yr.index = pd.MultiIndex.from_arrays([
                                [rf]*mix_yr.shape[0],
                                ['Commodity']*mix_yr.shape[0],
                                mix_yr.index.get_level_values('Variable')
                            ],names=["Region","Level","Item"])
                        
                            return mix_yr
                    
                        ee_mix_yr = rearrange_mixes(ee_mixes,scenario,r_cluster,region_from,year)
                        steel_mix_yr = rearrange_mixes(steel_mixes,scenario,r_cluster,region_from,year)

                        ee_coms = ee_mix_yr.index.get_level_values('Item')
                        steel_acts = steel_mix_yr.index.get_level_values('Item')

                        activities_to_exclude = ['Hydrogen production with electrolysis']
                        activities_to_keep = [act for act in db.get_index('Activity') if act not in activities_to_exclude]

                        u_ee = u.loc[(region_from,slice(None),ee_coms),(slice(None),'Activity',activities_to_keep)]
                        u_index = u_ee.index
                        u_columns = u_ee.columns
                        u_ee = u_ee.sum(0).to_frame().T.values
                        Y_ee = Y.loc[(region_from,slice(None),ee_coms),:].sum(0).to_frame().T.values

                        region_ee_mix = pd.DataFrame(0,index = u_index, columns = ['Value'])
                        region_ee_mix.update(ee_mix_yr)
                        region_ee_mix = region_ee_mix.values

                        new_u_ee = pd.DataFrame(region_ee_mix @ u_ee, index=u_index, columns=u_columns)
                        new_Y_ee = pd.DataFrame(region_ee_mix @ Y_ee, index=u_index, columns=Y.columns)

                        u.loc[new_u_ee.index,new_u_ee.columns] = new_u_ee.values
                        Y.loc[new_Y_ee.index,new_Y_ee.columns] = new_Y_ee.values

                        old_market_share = s.loc[
                            (region_from, 'Activity', steel_acts),
                            (region_from,'Commodity',steel_com)
                            ].sum().sum()
                        
                        s.loc[
                            (region_from, 'Activity', steel_acts),
                            (region_from, 'Commodity', steel_com)] = steel_mix_yr.values*old_market_share

                        print('done in {:.2f} s'.format(time.time()-start))


                # Update steel imports and final consumption
                for region_to in db.get_index('Region'):
                    u_steel_tot = u.loc[(slice(None),'Commodity',steel_com),(region_to,'Activity',slice(None))].sum(0).to_frame().T
                    Y_steel_tot = Y.loc[(slice(None),'Commodity',steel_com),(region_to,'Consumption category',slice(None))].sum(0).to_frame().T

                    u_steel_tot_values = u_steel_tot.values
                    Y_steel_tot_values = Y_steel_tot.values

                    for region_from in steel_imports['Region_from'].unique():
                        new_index = pd.MultiIndex.from_arrays([[region_from],['Commodity'],[steel_com]],names=u.index.names)
                        share = steel_imports.query("Scenario == @scenario and Year == @year and Region_from == @region_from")['Value'].values[0]

                        if region_from == steel_imports['Region_from'].unique()[0]:
                            u_steel_tot.index = new_index
                            Y_steel_tot.index = new_index
                        else:
                            u_steel_tot = pd.concat([
                                u_steel_tot,
                                pd.DataFrame(
                                    u_steel_tot_values*share,
                                    index=new_index,
                                    columns=u_steel_tot.columns
                            )],axis=0)
                            Y_steel_tot = pd.concat([
                                Y_steel_tot,
                                pd.DataFrame(
                                    Y_steel_tot_values*share,
                                    index=new_index,
                                    columns=Y_steel_tot.columns
                            )],axis=0)

                z.update(u)
                z.update(s)
                Y.update(Y)

                db.update_scenarios('shock',z=z,Y=Y)
                db.reset_to_coefficients('shock')
                print('Scenario updated in {:.2f} s'.format(time.time()-start_scen))


                # Calculate footprints
                start_calc = time.time()
                print('Calculating and exporting results', end=' ')
                def aggregate_ghgs(accounts,info):
                    for ghg,gwp in info.items():
                        accounts.loc[ghg,:] *= gwp

                    accounts = accounts.sum(0)
                    accounts = accounts.to_frame()   
                    if accounts.shape[1] == 1:
                        accounts = accounts.T 
                    accounts.index = ['GHGs']
                    return accounts

                def calc_f_ex(e,w): 
                    f_ex = np.diagflat(e.values) @ w.values
                    f_ex = pd.DataFrame(f_ex,index=w.index,columns=w.columns)
                    return f_ex
                
                e_ghg = aggregate_ghgs(db.query(matrices='e',scenarios='shock').loc[ghgs.keys(),:],ghgs)

                f_ex_ghg = calc_f_ex(e_ghg,db.query(matrices='w',scenarios='shock'))
                f_ex_ghg_act = f_ex_ghg.loc[(slice(None),'Activity',slice(None)),(slice(None),'Activity',(list(steel_acts)+list(ee_coms)+h2_acts))]
                f_ex_ghg_act = f_ex_ghg_act.droplevel(1,axis=0)
                f_ex_ghg_act = f_ex_ghg_act.droplevel(1,axis=1)
                f_ex_ghg_act.index.names = ['Region_from','Activity_from']
                f_ex_ghg_act.columns.names = ['Region_to','Activity_to']
                f_ex_ghg_act = f_ex_ghg_act.stack().stack().to_frame()
                f_ex_ghg_act.columns = ['Value']
                f_ex_ghg_act.reset_index(inplace=True)
                f_ex_ghg_act.to_parquet(os.path.join(results_folder,'GHG footprints/f_ex_ghg_act_{}_{}.parquet'.format(scenario,str(year))))

                f_ex_ghg_com = f_ex_ghg.loc[(slice(None),'Activity',slice(None)),(slice(None),'Commodity',list(ee_coms) + [steel_com] + h2_coms)]
                f_ex_ghg_com = f_ex_ghg_com.droplevel(1,axis=0)
                f_ex_ghg_com = f_ex_ghg_com.droplevel(1,axis=1)
                f_ex_ghg_com.index.names = ['Region_from','Activity_from']
                f_ex_ghg_com.columns.names = ['Region_to','Commodity_to']
                f_ex_ghg_com = f_ex_ghg_com.stack().stack().to_frame()
                f_ex_ghg_com.columns = ['Value']
                f_ex_ghg_com.reset_index(inplace=True)
                f_ex_ghg_com.to_parquet(os.path.join(results_folder,'GHG footprints/f_ex_ghg_com_{}_{}.parquet'.format(scenario,str(year))))


                e_ene = db.query(matrices='e',scenarios='shock').loc[energy,:]
                
                f_ex_ene = calc_f_ex(e_ene,db.query(matrices='w',scenarios='shock'))
                f_ex_ene_act = f_ex_ene.loc[(slice(None),'Activity',slice(None)),(EU27,'Activity',(list(steel_acts)+list(ee_coms)+h2_acts))]
                f_ex_ene_act = f_ex_ene_act.droplevel(1,axis=0)
                f_ex_ene_act = f_ex_ene_act.droplevel(1,axis=1)
                f_ex_ene_act.index.names = ['Region_from','Activity_from']
                f_ex_ene_act.columns.names = ['Region_to','Activity_to']
                f_ex_ene_act = f_ex_ene_act.stack().stack().to_frame()
                f_ex_ene_act.columns = ['Value']
                f_ex_ene_act.reset_index(inplace=True)
                f_ex_ene_act.to_parquet(os.path.join(results_folder,'Energy footprints/f_ex_ene_act_{}_{}.parquet'.format(scenario,str(year))))
                
                f_ex_ene_com = f_ex_ene.loc[(slice(None),'Activity',slice(None)),(EU27,'Commodity',list(ee_coms) + [steel_com] + h2_coms)]
                f_ex_ene_com = f_ex_ene_com.droplevel(1,axis=0)
                f_ex_ene_com = f_ex_ene_com.droplevel(1,axis=1)
                f_ex_ene_com.index.names = ['Region_from','Activity_from']
                f_ex_ene_com.columns.names = ['Region_to','Commodity_to']
                f_ex_ene_com = f_ex_ene_com.stack().stack().to_frame()
                f_ex_ene_com.columns = ['Value']
                f_ex_ene_com.reset_index(inplace=True)
                f_ex_ene_com.to_parquet(os.path.join(results_folder,'Energy footprints/f_ex_ene_com_{}_{}.parquet'.format(scenario,str(year))))

                db.query(matrices='X',scenarios='shock').to_parquet(os.path.join(results_folder,'Production/X_{}_{}.parquet'.format(scenario,str(year))))

                print('done in ' + str(time.time()-start_calc) + ' s\n')



# %% merge results
import pandas as pd
import os
import time 
import warnings
warnings.filterwarnings("ignore")

path = '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/IAM COMPACT Study 9/Results'
folders_prefixes = {
    'GHG footprints': [
        'f_ex_ghg_act',
        'f_ex_ghg_com',
        ],
    'Energy footprints': [
        'f_ex_ene_act',
        'f_ex_ene_com',
        ],
    'Production': ['X'],
}

start_all = time.time()
for folder,prefixes in folders_prefixes.items():
    for prefix in prefixes:
        print(f'\nMerging {prefix} in {folder}')
        data = pd.DataFrame()

        files = [f for f in os.listdir(os.path.join(path, folder)) if f.startswith(prefix) and f.endswith('parquet')]
        for file in files:
            scemario = file.replace(prefix + '_', '').replace('.parquet', '')
            if 'baseline' in scemario:
                scenario = 'baseline'
                year = 2024
            else:
                year = int(scemario.split('_')[-1])
                scenario = scemario.replace(f'_{year}','')

            print(f'   {scenario} {year}',end=' ')
            start = time.time()
            if folder == 'Production':
                temp_data = pd.read_parquet(os.path.join(path,folder,file)).query("Level=='Activity'")#,index_col=[0,1,2],header=[0])
                temp_data = temp_data.droplevel(1,axis=0)
                temp_data.index.names = ['Region_from','Activity_from']
                temp_data.columns = ['Value']
                temp_data['Scenario'] = scenario    
                temp_data['Year'] = year
                temp_data.reset_index(inplace=True)
                data = pd.concat([data, temp_data], axis=0)
            else:
                temp_data = pd.read_parquet(os.path.join(path,folder,file))
                temp_data['Scenario'] = scenario    
                temp_data['Year'] = year
                temp_data.reset_index(inplace=True) 
                if 'act' in prefix:
                    temp_data.loc[temp_data['Activity_from'] == temp_data['Activity_to'], 'Activity_from'] = 'Scope 1'
                else:
                    temp_data.loc[temp_data['Activity_from'].isin(steel_acts), 'Activity_from'] = 'Scope 1'
                data = pd.concat([data, temp_data], axis=0)
            
            print('done in {:.2f} s'.format(time.time()-start))
        
        # Drop all rows where 'Value' is between -1e-10 and 1e-10 (keep negatives)
        data = data[(data['Value'] < 0) | (data['Value'] >= 1e-7)]
        data['Value'] = data['Value'].round(6)
        data.to_parquet(os.path.join(path,f'Merged/{prefix}.parquet'),index=False)

print('\nAll done in {:.2f} s'.format(time.time()-start_all))

