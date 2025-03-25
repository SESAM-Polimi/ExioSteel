#%% Setting the ground
import mario
import pandas as pd
import time
import numpy as np
import warnings
import os
warnings.filterwarnings("ignore")

aggregated = True
scenarios = ['baseline','NDC_LTT','NDC_LTT_CBAM','NDC_LTT_CBAM-G_SUBS','NDC_LTT_CBAM-G_SUBS_HI']
years = range(2005,2051,5)
ghgs = {'Carbon dioxide, fossil (air - Emiss)':1,'CH4 (air - Emiss)':29.8,'N2O (air - Emiss)':273}
energy = ['Energy Carrier Supply: Total']
regions = {
    True: {
        'EU-15': ['EU-15'],
        'EU-12': ['EU-12'],
    },
    False: {
        'EU-15': ['IT','ES','PT','FR','BE','LU','NL','DE','DK','SE','FI','AT','IE','GR','MT'],
        'EU-12': ['PL','CZ','SK','SI','HU','RO','BG','HR','LT','LV','EE','CY'],
    }
}

steel_com = 'Basic iron and steel and of ferro-alloys and first products thereof'
results_folder = '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/IAM COMPACT Study 9/Results'

#%% Parse Exiobase database extended to new sectors as well as mixes projections for steel and electricity
# db = mario.parse_from_txt(
#     path='/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/IAM COMPACT Study 9/Model/Extended/coefficients',
#     mode='coefficients',
#     table='SUT',
# )

#%% Read electricity and steel mixes
ee_mixes = pd.read_excel('support/gcam_data/Electricity_mixes.xlsx',index_col=[0,1,2])
steel_mixes = pd.read_excel('support/gcam_data/Steel_mixes.xlsx',index_col=[0,1,2])
steel_cons = pd.read_excel('support/gcam_data/Steel_consumption.xlsx',index_col=[0,1,2])

#%% Aggregate the database
# db.get_aggregation_excel('support/aggregations/aggr_to_EU.xlsx')
# if aggregated:
#     db.aggregate('support/aggregations/aggr_to_EU12-EU15.xlsx',ignore_nan=True)

# %% Export aggregated database to txt
# db.to_txt(
#     '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/IAM COMPACT Study 9/Model/Extended_aggr',
# )

#%%
db = mario.parse_from_txt(
    path='/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/IAM COMPACT Study 9/Model/Extended_aggr/flows',
    mode='flows',
    table='SUT',
)

#%% Implement shocks and perform calculations
db.clone_scenario('baseline','shock')
for scenario in scenarios:
    
    if scenario != 'baseline':
        for year in years:

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


                    u_ee = u.loc[(region_from,slice(None),ee_coms),:]
                    u_index = u_ee.index
                    u_ee = u_ee.sum(0).to_frame().T.values
                    Y_ee = Y.loc[(region_from,slice(None),ee_coms),:].sum(0).to_frame().T.values

                    region_ee_mix = pd.DataFrame(0,index = u_index, columns = ['Value'])
                    region_ee_mix.update(ee_mix_yr)
                    region_ee_mix = region_ee_mix.values

                    new_u_ee = pd.DataFrame(region_ee_mix @ u_ee, index=u_index, columns=u.columns)
                    new_Y_ee = pd.DataFrame(region_ee_mix @ Y_ee, index=u_index, columns=Y.columns)

                    u.update(new_u_ee)
                    Y.update(new_Y_ee)

                    old_market_share = s.loc[
                        (region_from, 'Activity', steel_acts),
                        (region_from,'Commodity',steel_com)
                        ].sum().sum()
                    
                    s.loc[
                        (region_from, 'Activity', steel_acts),
                        (region_from, 'Commodity', steel_com)] = steel_mix_yr.values*old_market_share

                    print('done in {:.2f} s'.format(time.time()-start))

            z.update(u)
            z.update(s)

            db.update_scenarios('shock',z=z)
            db.reset_to_coefficients('shock')
            print('Scenario updated in {:.2f} s'.format(time.time()-start_scen))

            db_aggr = db.aggregate("support/aggregations/aggr_to_EU27.xlsx",ignore_nan=True, inplace=False)
            db_aggr = db_aggr.build_new_instance('shock')
            db_aggr.clone_scenario('baseline','shock')

            # # apply final demand shocks
            # Y = db_aggr.Y
            # Y.loc[
            #     ('EU27','Commodity',steel_com),
            #     ('EU27','Consumption category','Final consumption expenditure by households')
            #     ] = steel_cons.loc[(scenario,slice(None),slice(None)),str(year)].values

            # db_aggr.update_scenarios('shock',Y=Y)
            # db_aggr.reset_to_coefficients('shock')

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

            f_ghg = aggregate_ghgs(db_aggr.query(matrices='f',scenarios='shock').loc[ghgs.keys(),:],ghgs)
            E_ghg = aggregate_ghgs(db_aggr.query(matrices='E',scenarios='shock').loc[ghgs.keys(),:],ghgs)
            e_ghg = aggregate_ghgs(db_aggr.query(matrices='e',scenarios='shock').loc[ghgs.keys(),:],ghgs)

            f_ghg_act = f_ghg.loc[:,(slice(None),'Activity',steel_acts)]
            E_ghg_act = E_ghg.loc[:,(slice(None),'Activity',slice(None))]
            f_ghg_com = f_ghg.loc[:,(slice(None),'Commodity',steel_com)]
            if aggregated:
                f_ghg_act.T.to_csv(os.path.join(results_folder,'GHG footprints/f_ghg_act_{}_{}_aggr.csv'.format(scenario,str(year))))        
                E_ghg_act.T.to_csv(os.path.join(results_folder,'GHG footprints/E_ghg_act_{}_{}_aggr.csv'.format(scenario,str(year))))
                f_ghg_com.T.to_csv(os.path.join(results_folder,'GHG footprints/f_ghg_com_{}_{}_aggr.csv'.format(scenario,str(year))))
            else:
                f_ghg_act.T.to_csv(os.path.join(results_folder,'GHG footprints/f_ghg_act_{}_{}.csv'.format(scenario,str(year))))       
                E_ghg_act.T.to_csv(os.path.join(results_folder,'GHG footprints/E_ghg_act_{}_{}.csv'.format(scenario,str(year)))) 
                f_ghg_com.T.to_csv(os.path.join(results_folder,'GHG footprints/f_ghg_com_{}_{}.csv'.format(scenario,str(year))))
            
            f_ex_ghg = calc_f_ex(e_ghg,db_aggr.query(matrices='w',scenarios='shock'))
            f_ex_ghg_act = f_ex_ghg.loc[(slice(None),'Activity',slice(None)),(slice(None),'Activity',steel_acts)]
            if aggregated:
                f_ex_ghg_act.to_csv(os.path.join(results_folder,'GHG footprints/f_ex_ghg_act_{}_{}_aggr.csv'.format(scenario,str(year))))
            else:
                f_ex_ghg_act.to_csv(os.path.join(results_folder,'GHG footprints/f_ex_ghg_act_{}_{}.csv'.format(scenario,str(year))))

            f_ene = db_aggr.query(matrices='f',scenarios='shock').loc[energy,:]
            E_ene = db_aggr.query(matrices='E',scenarios='shock').loc[energy,:]
            e_ene = db_aggr.query(matrices='e',scenarios='shock').loc[energy,:]

            f_ene_act = f_ene.loc[:,(slice(None),'Activity',steel_acts)]
            E_ene_act = E_ene.loc[:,(slice(None),'Activity',slice(None))]
            f_ene_com = f_ene.loc[:,(slice(None),'Commodity',steel_com)]
            if aggregated:
                f_ene_act.T.to_csv(os.path.join(results_folder,'Energy footprints/f_ene_act_{}_{}_aggr.csv'.format(scenario,str(year))))
                E_ene_act.T.to_csv(os.path.join(results_folder,'Energy footprints/E_ene_act_{}_{}_aggr.csv'.format(scenario,str(year))))
                f_ene_com.T.to_csv(os.path.join(results_folder,'Energy footprints/f_ene_com_{}_{}_aggr.csv'.format(scenario,str(year))))
            else:
                f_ene_act.T.to_csv(os.path.join(results_folder,'Energy footprints/f_ene_act_{}_{}.csv'.format(scenario,str(year))))
                E_ene_act.T.to_csv(os.path.join(results_folder,'Energy footprints/E_ene_act_{}_{}.csv'.format(scenario,str(year))))
                f_ene_com.T.to_csv(os.path.join(results_folder,'Energy footprints/f_ene_com_{}_{}.csv'.format(scenario,str(year))))

            f_ex_ene = calc_f_ex(e_ene,db_aggr.query(matrices='w',scenarios='shock'))
            f_ex_ene_act = f_ex_ene.loc[(slice(None),'Activity',slice(None)),(slice(None),'Activity',steel_acts)]
            if aggregated:
                f_ex_ene_act.to_csv(os.path.join(results_folder,'Energy footprints/f_ex_ene_act_{}_{}_aggr.csv'.format(scenario,str(year))))
            else:
                f_ex_ene_act.to_csv(os.path.join(results_folder,'Energy footprints/f_ex_ene_act_{}_{}.csv'.format(scenario,str(year))))
            print('done in ' + str(time.time()-start_calc) + ' s\n')


    else:

        print('Processing',scenario)
        db_aggr = db.aggregate("support/aggregations/aggr_to_EU27.xlsx",ignore_nan=True, inplace=False)

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

        ee_coms = sorted(list(set(list(ee_mixes.index.get_level_values('Variable')))))
        steel_acts = sorted(list(set(list(steel_mixes.index.get_level_values('Variable')))))

        f_ghg = aggregate_ghgs(db_aggr.query(matrices='f',scenarios='baseline').loc[ghgs.keys(),:],ghgs)
        e_ghg = aggregate_ghgs(db_aggr.query(matrices='e',scenarios='baseline').loc[ghgs.keys(),:],ghgs)
        E_ghg = aggregate_ghgs(db_aggr.query(matrices='E',scenarios='baseline').loc[ghgs.keys(),:],ghgs)

        f_ghg_act = f_ghg.loc[:,(slice(None),'Activity',steel_acts)]
        E_ghg_act = E_ghg.loc[:,(slice(None),'Activity',slice(None))]
        f_ghg_com = f_ghg.loc[:,(slice(None),'Commodity',steel_com)]
        if aggregated:
            f_ghg_act.T.to_csv(os.path.join(results_folder,'GHG footprints/f_ghg_act_{}_aggr.csv'.format(scenario)))        
            E_ghg_act.T.to_csv(os.path.join(results_folder,'GHG footprints/E_ghg_act_{}_aggr.csv'.format(scenario)))        
            f_ghg_com.T.to_csv(os.path.join(results_folder,'GHG footprints/f_ghg_com_{}_aggr.csv'.format(scenario)))
        else:
            f_ghg_act.T.to_csv(os.path.join(results_folder,'GHG footprints/f_ghg_act_{}.csv'.format(scenario)))  
            E_ghg_act.T.to_csv(os.path.join(results_folder,'GHG footprints/E_ghg_act_{}.csv'.format(scenario)))      
            f_ghg_com.T.to_csv(os.path.join(results_folder,'GHG footprints/f_ghg_com_{}.csv'.format(scenario)))
        
        f_ex_ghg = calc_f_ex(e_ghg,db_aggr.query(matrices='w',scenarios='baseline'))
        f_ex_ghg_act = f_ex_ghg.loc[(slice(None),'Activity',slice(None)),(slice(None),'Activity',steel_acts)]
        if aggregated:
            f_ex_ghg_act.to_csv(os.path.join(results_folder,'GHG footprints/f_ex_ghg_act_{}_aggr.csv'.format(scenario)))
        else:
            f_ex_ghg_act.to_csv(os.path.join(results_folder,'GHG footprints/f_ex_ghg_act_{}.csv'.format(scenario)))

        f_ene = db_aggr.query(matrices='f',scenarios='baseline').loc[energy,:]
        E_ene = db_aggr.query(matrices='E',scenarios='baseline').loc[energy,:]
        e_ene = db_aggr.query(matrices='e',scenarios='baseline').loc[energy,:]

        f_ene_act = f_ene.loc[:,(slice(None),'Activity',steel_acts)]
        E_ene_act = E_ene.loc[:,(slice(None),'Activity',slice(None))]
        f_ene_com = f_ene.loc[:,(slice(None),'Commodity',steel_com)]
        if aggregated:
            f_ene_act.T.to_csv(os.path.join(results_folder,'Energy footprints/f_ene_act_{}_aggr.csv'.format(scenario)))
            E_ene_act.T.to_csv(os.path.join(results_folder,'Energy footprints/E_ene_act_{}_aggr.csv'.format(scenario)))
            f_ene_com.T.to_csv(os.path.join(results_folder,'Energy footprints/f_ene_com_{}_aggr.csv'.format(scenario)))
        else:
            f_ene_act.T.to_csv(os.path.join(results_folder,'Energy footprints/f_ene_act_{}.csv'.format(scenario)))
            E_ene_act.T.to_csv(os.path.join(results_folder,'Energy footprints/E_ene_act_{}.csv'.format(scenario)))
            f_ene_com.T.to_csv(os.path.join(results_folder,'Energy footprints/f_ene_com_{}.csv'.format(scenario)))

        f_ex_ene = calc_f_ex(e_ene,db_aggr.query(matrices='w',scenarios='baseline'))
        f_ex_ene_act = f_ex_ene.loc[(slice(None),'Activity',slice(None)),(slice(None),'Activity',steel_acts)]
        if aggregated:
            f_ex_ene_act.to_csv(os.path.join(results_folder,'Energy footprints/f_ex_ene_act_{}_aggr.csv'.format(scenario)))
        else:
            f_ex_ene_act.to_csv(os.path.join(results_folder,'Energy footprints/f_ex_ene_act_{}.csv'.format(scenario)))
        print('done in ' + str(time.time()-start_calc) + ' s\n\n')



# %% merge results
import pandas as pd
import os
import time 
import warnings
warnings.filterwarnings("ignore")

path = '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/IAM COMPACT Study 9/Results'
folders_prefixes = {
    'GHG footprints': ['f_ghg_act','f_ghg_com','f_ex_ghg_act','E_ghg_act'],
    'Energy footprints': ['f_ene_act','f_ene_com','f_ex_ene_act','E_ene_act']
}

start_all = time.time()
for folder,prefixes in folders_prefixes.items():
    for prefix in prefixes:
        print(f'\nMerging {prefix} in {folder}')
        data = pd.DataFrame()

        files = [f for f in os.listdir(os.path.join(path, folder)) if f.startswith(prefix) and f.endswith('csv')]
        for file in files:
            scemario = file.replace(prefix + '_', '').replace('.csv', '')
            if 'aggr' in scemario:
                scemario = scemario.replace('_aggr','')

            if 'baseline' in scemario:
                scenario = 'baseline'
                year = 2024
            else:
                year = int(scemario.split('_')[-1])
                scenario = scemario.replace(f'_{year}','')

            print(f'   {scenario} {year}',end=' ')
            start = time.time()
            if "f_ex" not in prefix:
                temp_data = pd.read_csv(os.path.join(path,folder,file))
                temp_data = temp_data.query('Region == "EU27"')   # filter only EU27
                temp_data = temp_data.drop(columns='Level')
                if 'act' in prefix:
                    temp_data.columns = ['Region_to','Activity_to','Value']
                else:
                    temp_data.columns = ['Region_to','Commodity_to','Value']
                temp_data['Scenario'] = scenario
                temp_data['Year'] = year
            
            else:
                temp_data = pd.read_csv(os.path.join(path,folder,file),index_col=[0,1,2],header=[0,1,2])
                temp_data = temp_data.loc[:,('EU27',slice(None),slice(None))]   # filter only EU27
                temp_data = temp_data.droplevel(1,axis=0)
                temp_data = temp_data.droplevel(1,axis=1)
                temp_data.index.names = ['Region_from','Activity_from']
                temp_data.columns.names = ['Region_to','Activity_to']
                temp_data = temp_data.stack()
                temp_data = temp_data.stack()
                temp_data = temp_data.to_frame()
                temp_data.columns = ['Value']
                temp_data['Scenario'] = scenario    
                temp_data['Year'] = year
                temp_data.reset_index(inplace=True) 
                
            data = pd.concat([data, temp_data], axis=0)
            print('done in {:.2f} s'.format(time.time()-start))
        
        data.to_csv(os.path.join(path,f'Merged/{prefix}.csv'),index=False)

print('\nAll done in {:.2f} s'.format(time.time()-start_all))

# %%
