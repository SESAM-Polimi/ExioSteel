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
years = range(2015,2051,5)
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
        'European Free Trade Association': ['NO'],
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
    }
}

steel_com = 'Basic iron and steel and of ferro-alloys and first products thereof'
results_folder = '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/IAM COMPACT Study 9/Results'
steel_acts = ['AEL-EAF','BF-BOF-BECCSmax','BF-BOF-BECCSmin','BF-BOF-CCS-73%','BF-BOF-CCS-86%','DRI-EAF-BECCS','DRI-EAF-COAL-CCS','DRI-EAF-H2','DRI-EAF-NG','DRI-EAF-NG-CCS','DRI-SAF-BOF-BECCS','DRI-SAF-BOF-H2','DRI-SAF-BOF-NG','MOE','Manufacture of basic iron and steel and of ferro-alloys and first products thereof','Re-processing of secondary steel into new steel','SR-BOF','SR-BOF-CCS']

#%% Parse Exiobase database extended to new sectors as well as mixes projections for steel and electricity
db = mario.parse_from_txt(
    path='/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/Exiobase/Hybrid/EnergyTranstionEdition/v1.0.1/coefficients',
    mode='coefficients',
    table='SUT',
)

#%%
f_techs = db.f.loc[ghgs.keys(),(slice(None),'Activity',steel_acts)]
f_techs = f_techs.T
f_techs = f_techs.stack()
f_techs = f_techs.to_frame()
f_techs.index.names = ['Region','Level','Tech','Gas']
# f_techs.reset_index(inplace=True)
f_techs = f_techs.unstack(level='Tech')
f_techs = f_techs.droplevel(0,axis=1)  # remove the first level of the columns])
f_techs = f_techs.droplevel(1,axis=0)
f_techs = f_techs.unstack(level='Region')
f_techs.loc['CH4 (air - Emiss)',:] *=29.8
f_techs.loc['N2O (air - Emiss)',:] *=273
f_techs = f_techs.sum(0)
f_techs = f_techs.to_frame()
f_techs = f_techs.unstack(level='Tech')
f_techs = f_techs.droplevel(0,axis=1)  # remove the first level of the columns])

f_techs.to_clipboard()


#%% Read electricity and steel mixes
ee_mixes = pd.read_excel('support/gcam_data/Electricity_mixes.xlsx',index_col=[0,1,2])
steel_mixes = pd.read_excel('support/gcam_data/Steel_mixes.xlsx',index_col=[0,1,2])
steel_cons = pd.read_excel('support/gcam_data/Steel_consumption.xlsx',index_col=[0,1,2])
steel_imports = pd.read_excel('support/gcam_data/Steel_imports.xlsx')

#%% Aggregate the database
# db.get_aggregation_excel('support/aggregations/aggr_to_EU.xlsx')
if aggregated:
    db.aggregate('support/aggregations/aggr_to_EU12-EU15.xlsx',ignore_nan=True)

#%% Export aggregated database to txt
db.to_txt(
    '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/IAM COMPACT Study 9/Model/Extended_aggr',
)

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

            db_aggr = db.aggregate("support/aggregations/aggr_to_EU27.xlsx",ignore_nan=True, inplace=False)
            db_aggr = db_aggr.build_new_instance('shock')
            db_aggr.clone_scenario('baseline','shock')

            # apply final demand shocks
            Y = db_aggr.Y
            Y.loc[
                ('EU27','Commodity',steel_com),
                ('EU27','Consumption category','Final consumption expenditure by households')
                ] = steel_cons.loc[(scenario,slice(None),slice(None)),str(year)].values

            db_aggr.update_scenarios('shock',Y=Y)
            db_aggr.reset_to_coefficients('shock')

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
            f_ex_ghg_com = f_ex_ghg.loc[(slice(None),'Activity',slice(None)),(slice(None),'Commodity',steel_com)]
            if aggregated:
                f_ex_ghg_act.to_csv(os.path.join(results_folder,'GHG footprints/f_ex_ghg_act_{}_{}_aggr.csv'.format(scenario,str(year))))
                f_ex_ghg_com.to_csv(os.path.join(results_folder,'GHG footprints/f_ex_ghg_com_{}_{}_aggr.csv'.format(scenario,str(year))))
            else:
                f_ex_ghg_act.to_csv(os.path.join(results_folder,'GHG footprints/f_ex_ghg_act_{}_{}.csv'.format(scenario,str(year))))
                f_ex_ghg_com.to_csv(os.path.join(results_folder,'GHG footprints/f_ex_ghg_com_{}_{}.csv'.format(scenario,str(year))))

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
            f_ex_ene_com = f_ex_ene.loc[(slice(None),'Activity',slice(None)),(slice(None),'Commodity',steel_com)]
            if aggregated:
                f_ex_ene_act.to_csv(os.path.join(results_folder,'Energy footprints/f_ex_ene_act_{}_{}_aggr.csv'.format(scenario,str(year))))
                f_ex_ene_com.to_csv(os.path.join(results_folder,'Energy footprints/f_ex_ene_com_{}_{}_aggr.csv'.format(scenario,str(year))))
            else:
                f_ex_ene_act.to_csv(os.path.join(results_folder,'Energy footprints/f_ex_ene_act_{}_{}.csv'.format(scenario,str(year))))
                f_ex_ene_com.to_csv(os.path.join(results_folder,'Energy footprints/f_ex_ene_com_{}_{}.csv'.format(scenario,str(year))))


            def calc_FF_ex(f_ex,Y):
                if Y.shape[1] > 1:
                    Y = Y.sum(1).to_frame().T
                F_ex_by_region = f_ex.values @ np.diagflat(Y.values)
                F_ex = pd.DataFrame(F_ex_by_region, index=f_ex.index, columns=f_ex.columns)
                return F_ex
            
            Y_EU = db_aggr.query(matrices='Y',scenarios='shock').loc[:,('EU27','Consumption category',slice(None))]
            F_ex_ghg = calc_FF_ex(f_ex_ghg,Y_EU)
            F_ex_ene = calc_FF_ex(f_ex_ene,Y_EU)

            F_ex_ghg_com = F_ex_ghg.loc[(slice(None),'Activity',slice(None)),('EU27','Commodity',slice(None))]
            F_ex_ene_com = F_ex_ene.loc[(slice(None),'Activity',slice(None)),('EU27','Commodity',slice(None))]

            if aggregated:
                F_ex_ghg_com.to_csv(os.path.join(results_folder,'GHG footprints/FF_ex_ghg_com_{}_{}_aggr.csv'.format(scenario,str(year))))
                F_ex_ene_com.to_csv(os.path.join(results_folder,'Energy footprints/FF_ex_ene_com_{}_{}_aggr.csv'.format(scenario,str(year))))
            else:
                F_ex_ghg_com.to_csv(os.path.join(results_folder,'GHG footprints/FF_ex_ghg_com_{}_{}.csv'.format(scenario,str(year))))
                F_ex_ene_com.to_csv(os.path.join(results_folder,'Energy footprints/FF_ex_ene_com_{}_{}.csv'.format(scenario,str(year))))

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
        f_ex_ghg_com = f_ex_ghg.loc[(slice(None),'Activity',slice(None)),(slice(None),'Commodity',steel_com)]
        if aggregated:
            f_ex_ghg_act.to_csv(os.path.join(results_folder,'GHG footprints/f_ex_ghg_act_{}_aggr.csv'.format(scenario)))
            f_ex_ghg_com.to_csv(os.path.join(results_folder,'GHG footprints/f_ex_ghg_com_{}_aggr.csv'.format(scenario)))
        else:
            f_ex_ghg_act.to_csv(os.path.join(results_folder,'GHG footprints/f_ex_ghg_act_{}.csv'.format(scenario)))
            f_ex_ghg_com.to_csv(os.path.join(results_folder,'GHG footprints/f_ex_ghg_com_{}.csv'.format(scenario)))

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
        f_ex_ene_com = f_ex_ene.loc[(slice(None),'Activity',slice(None)),(slice(None),'Commodity',steel_com)]
        if aggregated:
            f_ex_ene_act.to_csv(os.path.join(results_folder,'Energy footprints/f_ex_ene_act_{}_aggr.csv'.format(scenario)))
            f_ex_ene_com.to_csv(os.path.join(results_folder,'Energy footprints/f_ex_ene_com_{}_aggr.csv'.format(scenario)))
        else:
            f_ex_ene_act.to_csv(os.path.join(results_folder,'Energy footprints/f_ex_ene_act_{}.csv'.format(scenario)))
            f_ex_ene_com.to_csv(os.path.join(results_folder,'Energy footprints/f_ex_ene_com_{}.csv'.format(scenario)))

        def calc_FF_ex(f_ex,Y):
            if Y.shape[1] > 1:
                Y = Y.sum(1).to_frame()
            F_ex_by_region = f_ex.values @ np.diagflat(Y.values)
            F_ex = pd.DataFrame(F_ex_by_region, index=f_ex.index, columns=f_ex.columns)
            return F_ex
        
        Y_EU = db_aggr.query(matrices='Y',scenarios='baseline').loc[:,('EU27','Consumption category',slice(None))]
        F_ex_ghg = calc_FF_ex(f_ex_ghg,Y_EU)
        F_ex_ene = calc_FF_ex(f_ex_ene,Y_EU)

        F_ex_ghg_com = F_ex_ghg.loc[(slice(None),'Activity',slice(None)),('EU27','Commodity',slice(None))]
        F_ex_ene_com = F_ex_ene.loc[(slice(None),'Activity',slice(None)),('EU27','Commodity',slice(None))]

        if aggregated:
            F_ex_ghg_com.to_csv(os.path.join(results_folder,'GHG footprints/FF_ex_ghg_com_{}_aggr.csv'.format(scenario)))
            F_ex_ene_com.to_csv(os.path.join(results_folder,'Energy footprints/FF_ex_ene_com_{}_aggr.csv'.format(scenario)))
        else:
            F_ex_ghg_com.to_csv(os.path.join(results_folder,'GHG footprints/FF_ex_ghg_com_{}.csv'.format(scenario)))
            F_ex_ene_com.to_csv(os.path.join(results_folder,'Energy footprints/FF_ex_ene_com_{}.csv'.format(scenario)))

        print('done in ' + str(time.time()-start_calc) + ' s\n\n')



# %% merge results
import pandas as pd
import os
import time 
import warnings
warnings.filterwarnings("ignore")

path = '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/IAM COMPACT Study 9/Results'
folders_prefixes = {
    'GHG footprints': ['f_ghg_act','f_ghg_com','f_ex_ghg_act','f_ex_ghg_com','FF_ex_ghg_com'],
    'Energy footprints': ['f_ene_act','f_ene_com','f_ex_ene_act','f_ex_ene_com','FF_ex_ene_com']
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
            if "f_ex" not in prefix and "FF_ex" not in prefix:
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
                if 'act' in prefix:
                    temp_data.columns.names = ['Region_to','Activity_to']
                else:
                    temp_data.columns.names = ['Region_to','Commodity_to']
                temp_data = temp_data.stack()
                temp_data = temp_data.stack()
                temp_data = temp_data.to_frame()
                temp_data.columns = ['Value']
                temp_data['Scenario'] = scenario    
                temp_data['Year'] = year
                temp_data.reset_index(inplace=True) 
                if 'act' in prefix:
                    temp_data.loc[temp_data['Activity_from'] == temp_data['Activity_to'], 'Activity_from'] = 'Scope 1'
                else:
                    temp_data.loc[temp_data['Activity_from'].isin(steel_acts), 'Activity_from'] = 'Scope 1'

            data = pd.concat([data, temp_data], axis=0)
            print('done in {:.2f} s'.format(time.time()-start))
        
        data.to_csv(os.path.join(path,f'Merged/{prefix}.csv'),index=False)

print('\nAll done in {:.2f} s'.format(time.time()-start_all))

#%%
df = pd.read_csv('/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/IAM COMPACT Study 9/Results/Merged/FF_ex_ghg_com.csv')
df =df.query('Region_to == "EU27"')
df.to_csv('/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/IAM COMPACT Study 9/Results/Merged/FF_ex_ghg_com_EU27.csv',index=False)

#%% multiply by volumes to get F
# import pandas as pd
# import os
# import warnings

# warnings.filterwarnings("ignore")

# sn = slice(None)

# steel_cons = pd.read_excel('support/gcam_data/Steel_consumption.xlsx',index_col=[0,1])
# steel_cons.columns.names = ['Year']
# steel_cons = steel_cons.stack().to_frame()
# steel_cons.columns = ['Value']
# steel_cons.reset_index(inplace=True)

# path = '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/IAM COMPACT Study 9/Results'
# f_ex_ghg_com = pd.read_csv(os.path.join(path,f'Merged/f_ex_ghg_com.csv'),index_col=[5,6])

# for i in steel_cons.index:
#     scenario = steel_cons.loc[i,"Scenario"]
#     try:
#         year = steel_cons.loc[i,'Year']
#         f_ex_ghg_com.loc[(scenario,int(year)),"Value"] *= steel_cons.loc[i, 'Value']
#     except:
#         pass

# f_ex_ghg_com.to_csv(os.path.join(path,f'Merged/FF_ex_ghg_com.csv'))


# %%
# db.u.loc[
#     (slice(None),'Commodity',slice(None)),
#     ('EU-15','Activity',db.search("Activity","Aluminium")),
# ].groupby(level=2).sum().to_clipboard()


e = db_aggr.e.loc["Carbon dioxide, fossil (air - Emiss)",:].values
w = db_aggr.w.values
Y = db_aggr.Y.sum(1).values

f_ex = np.diagflat(e) @ w
F_ex = f_ex @ np.diagflat(Y)

F_ex = pd.DataFrame(F_ex,index=db_aggr.w.index,columns=db_aggr.w.columns).loc[(slice(None),'Activity',steel_acts),('EU27','Commodity',slice(None))].sum(0).T
F_ex.to_clipboard()

# %%
