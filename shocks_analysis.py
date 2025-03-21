#%% Setting the ground
import mario
import pandas as pd
import time
import numpy as np
import warnings
import os
warnings.filterwarnings("ignore")

scenarios = ['baseline','NDC_LTT','NDC_LTT_CBAM','NDC_LTT_CBAM-G_SUBS','NDC_LTT_CBAM-G_SUBS_HI']
years = range(2025,2051,5)
ghgs = {'Carbon dioxide, fossil (air - Emiss)':1,'CH4 (air - Emiss)':29.8,'N2O (air - Emiss)':273}
energy = ['Energy Carrier Supply: Total']
regions = {
    'EU-15': ['IT','ES','PT','FR','BE','LU','NL','DE','DK','SE','FI','AT','IE','GR','MT'],
    'EU-12': ['PL','CZ','SK','SI','HU','RO','BG','HR','LT','LV','EE','CY'],
}

steel_com = 'Basic iron and steel and of ferro-alloys and first products thereof'
results_folder = '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/IAM COMPACT Study 9/Results'

#%% Parse Exiobase database extended to new sectors as well as mixes projections for steel and electricity
db = mario.parse_from_txt(
    path='/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/IAM COMPACT Study 9/Model/Extended/coefficients',
    mode='coefficients',
    table='SUT',
)

ee_mixes = pd.read_excel('support/gcam_data/Electricity_mixes.xlsx',index_col=[0,1,2])
steel_mixes = pd.read_excel('support/gcam_data/Steel_mixes.xlsx',index_col=[0,1,2])

#%% Implement shocks and perform calculations
db.clone_scenario('baseline','shock')

for scenario in scenarios:
    
    if scenario != 'baseline':
        for year in years:

            print('Processing',scenario,year)
            z = db.z
            u = db.u
            s = db.s
            Y = db.Y

            # Update electricity and steel mixes
            for r_cluster,r_lists in regions.items():
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

            
            # Calculate footprints
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

            f_ghg = aggregate_ghgs(db.query(matrices='f',scenarios='shock').loc[ghgs.keys(),:],ghgs)
            f_ghg_act = f_ghg.loc[:,(slice(None),'Activity',steel_acts)]
            f_ghg_com = f_ghg.loc[:,(slice(None),'Commodity',steel_com)]
            f_ghg_act.T.to_csv(os.path.join(results_folder,'GHG footprints/f_ghg_act_{}_{}.csv'.format(scenario,str(year))))        
            f_ghg_com.T.to_csv(os.path.join(results_folder,'GHG footprints/f_ghg_com_{}_{}.csv'.format(scenario,str(year))))
            
            f_ex_ghg = calc_f_ex(f_ghg,db.query(matrices='w',scenarios='shock'))
            f_ex_ghg_act = f_ex_ghg.loc[(slice(None),'Activity',slice(None)),(slice(None),'Activity',steel_acts)]
            f_ex_ghg_act.to_csv(os.path.join(results_folder,'GHG footprints/f_ex_ghg_act_{}_{}.csv'.format(scenario,str(year))))

            f_ene = db.query(matrices='f',scenarios='shock').loc[energy,:]
            f_ene_act = f_ene.loc[:,(slice(None),'Activity',steel_acts)]
            f_ene_com = f_ene.loc[:,(slice(None),'Commodity',steel_com)]
            f_ene_com.T.to_csv(os.path.join(results_folder,'Energy footprints/f_ene_com_{}_{}.csv'.format(scenario,str(year))))

            f_ex_ene = calc_f_ex(f_ene,db.query(matrices='w',scenarios='shock'))
            f_ex_ene_act = f_ex_ene.loc[(slice(None),'Activity',slice(None)),(slice(None),'Activity',steel_acts)]
            f_ex_ene_act.to_csv(os.path.join(results_folder,'Energy footprints/f_ex_ene_act_{}_{}.csv'.format(scenario,str(year))))
            
    else:

        print('Processing',scenario)

        # Calculate footprints
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

        f_ghg = aggregate_ghgs(db.query(matrices='f',scenarios='baseline').loc[ghgs.keys(),:],ghgs)
        f_ghg_act = f_ghg.loc[:,(slice(None),'Activity',steel_acts)]
        f_ghg_com = f_ghg.loc[:,(slice(None),'Commodity',steel_com)]
        f_ghg_act.T.to_csv(os.path.join(results_folder,'GHG footprints/f_ghg_act_{}.csv'.format(scenario)))        
        f_ghg_com.T.to_csv(os.path.join(results_folder,'GHG footprints/f_ghg_com_{}.csv'.format(scenario)))
        
        f_ex_ghg = calc_f_ex(f_ghg,db.query(matrices='w',scenarios='baseline'))
        f_ex_ghg_act = f_ex_ghg.loc[(slice(None),'Activity',slice(None)),(slice(None),'Activity',steel_acts)]
        f_ex_ghg_act.to_csv(os.path.join(results_folder,'GHG footprints/f_ex_ghg_act_{}.csv'.format(scenario)))

        f_ene = db.query(matrices='f',scenarios='baseline').loc[energy,:]
        f_ene_act = f_ene.loc[:,(slice(None),'Activity',steel_acts)]
        f_ene_com = f_ene.loc[:,(slice(None),'Commodity',steel_com)]
        f_ene_com.T.to_csv(os.path.join(results_folder,'Energy footprints/f_ene_com_{}.csv'.format(scenario)))

        f_ex_ene = calc_f_ex(f_ene,db.query(matrices='w',scenarios='baseline'))
        f_ex_ene_act = f_ex_ene.loc[(slice(None),'Activity',slice(None)),(slice(None),'Activity',steel_acts)]
        f_ex_ene_act.to_csv(os.path.join(results_folder,'Energy footprints/f_ex_ene_act_{}.csv'.format(scenario)))



# %%
