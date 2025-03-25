#%%
import pandas as pd
import os
import plotly.express as px
import inspect

aggregated = True
scenarios = ['baseline','NDC_LTT','NDC_LTT_CBAM','NDC_LTT_CBAM-G_SUBS','NDC_LTT_CBAM-G_SUBS_HI']
EU_countries = ['AT','BE','BG','CY','CZ','DE','DK','EE','ES','FI','FR','GR','HR','HU','IE','IT','LT','LU','LV','MT','NL','PL','PT','RO','SE','SI','SK']
years = range(2025,2051,5)
results_folder = '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/IAM COMPACT Study 9/Results'

steel_acts_sort = ['BF-BOF','BF-BOF-BECCSmax','BF-BOF-BECCSmin','BF-BOF-CCS-73%','BF-BOF-CCS-86%','DRI-EAF-COAL','DRI-EAF-COAL-CCS','DRI-EAF-BECCS','DRI-EAF-NG','DRI-EAF-NG-CCS','DRI-EAF-H2','SR-BOF','SR-BOF-CCS','AEL-EAF','MOE','DRI-SAF-BOF-BECCS','DRI-SAF-BOF-H2','DRI-SAF-BOF-NG','Secondary']

def merge_scenario_data(
        folder,
        file_prefix,
        aggregated,
        baseline_year,
        keep_baseline,
        reg_average,
        reg_list,
        years_list,
        scenario_list,
        exploded = False,
):
    data = pd.DataFrame()
    
    if not exploded:
        for scenario in scenarios:

            if scenario == 'baseline':
                if aggregated:
                    path = os.path.join(results_folder,folder,f'{file_prefix}_{scenario}_aggr.csv')
                else:
                    path = os.path.join(results_folder,folder,f'{file_prefix}_{scenario}.csv')
                df = pd.read_csv(path,index_col=[0,1,2])
                df.columns = pd.MultiIndex.from_arrays([[scenario],[str(baseline_year)]],names=['Scenario','Year'])
                data = pd.concat([data,df],axis=1)
            
            else:
                for year in years:
                    if aggregated:
                        path = os.path.join(results_folder,folder,f'{file_prefix}_{scenario}_{str(year)}_aggr.csv')
                    else:
                        path = os.path.join(results_folder,folder,f'{file_prefix}_{scenario}_{str(year)}.csv')
                    df = pd.read_csv(path,index_col=[0,1,2])
                    df.columns = pd.MultiIndex.from_arrays([[scenario],[str(year)]],names=['Scenario','Year'])
                    data = pd.concat([data,df],axis=1)

        data = data.stack(level=0).stack(level=0)
        if isinstance(data, pd.Series):
            data = data.to_frame()
        data.columns = ['Values']
        data = data.reset_index()
        data = data.drop(columns='Level')

        if reg_list != None:
            data = data.query('Region in @reg_list')
        else:
            if aggregated:
                EU_regions = ['EU-12','EU-15']
                data = data.query('Region in @EU_regions')
            else:
                data = data.query('Region in @EU_countries')
        
        if years_list != None:
            data = data.query('Year in @years_list')
        
        if scenario_list != None:
            data = data.query('Scenario in @scenario_list')

        if not keep_baseline:
            data = data.query('Scenario != "baseline"')
        
        if reg_average:
            data.set_index(['Region','Item','Scenario','Year'],inplace=True)
            data = data.groupby(['Item','Scenario','Year']).mean().reset_index()
            data['Region'] = 'EU'
        
        if 'act' in file_prefix:
            data['Item'] = data['Item'].str.replace('Manufacture of basic iron and steel and of ferro-alloys and first products thereof','BF-BOF')
            data['Item'] = data['Item'].str.replace('Re-processing of secondary steel into new steel','Secondary')
            data['Item'] = pd.Categorical(data['Item'], categories=steel_acts_sort, ordered=True)
            data = data.sort_values(by=['Item','Scenario','Year'])
        else:
            data['Item'] = data['Item'].str.replace('Basic iron and steel and of ferro-alloys and first products thereof','Steel')

    else:
        raise NotImplementedError
    
    return data


def get_file_name(
        path,
        file_prefix,
        reg_list,
        reg_average,
        keep_baseline,
):
    
    file_name = os.path.join(path,f'{file_prefix}')
    if reg_list != None:
        file_name += "_"
        for reg in reg_list:
            file_name += f'{reg}'
    if reg_average:
        file_name += "_regAverage"
    else:
        file_name += "_byCountry"
    if not keep_baseline:
        file_name += "_noBaseline"
    file_name += ".html"

    return file_name


def get_expression(x, y, facet_col, facet_row, color, barmode, animation_frame):
    param_names = inspect.signature(get_expression).parameters.keys()
    args = {name: value for name, value in zip(param_names, [x, y, facet_col, facet_row, color, barmode, animation_frame])}
    not_none = {key: value for key, value in args.items() if value is not None}
    expression = "px.bar(data" + "".join(f", {key}={key}" for key in not_none.keys()) + ")"
    return expression
    

def plot_f(
        folder,
        file_prefix,
        aggregated,
        baseline_year = 2024,
        keep_baseline = False,
        reg_average = False,
        reg_list = None,
        years_list = None,
        scenario_list = None,
        path = os.path.join(results_folder,'Plots'),
        x = 'Year',
        y = 'Values',
        facet_col = 'Scenario',
        facet_row = 'Region',
        color = 'Item',
        barmode = 'stack',
        animation_frame = None,
):
    
    data = merge_scenario_data(folder,file_prefix,aggregated,baseline_year,keep_baseline,reg_average,reg_list,years_list,scenario_list)
    
    expression = get_expression(x,y,facet_col,facet_row,color,barmode,animation_frame)
    fig = eval(expression)

    file_name = get_file_name(path,file_prefix,reg_list,reg_average,keep_baseline)
    fig.update_layout(
        template = 'seaborn',
    )
    fig.write_html(file_name)

    return data


#%% by activities
data = plot_f(
    folder = 'GHG footprints',
    file_prefix = 'f_ghg_act',
    aggregated = True,
    # reg_average=True,
    # scenario_list = 'baseline',
    keep_baseline=True,
    barmode='group',
    animation_frame='Scenario',
    facet_col=None,
    )

data = plot_f(
    folder = 'Energy footprints',
    file_prefix = 'f_ene_act',
    aggregated = True,
    # reg_average=True,
    # scenario_list = 'baseline',
    keep_baseline=True,
    barmode='group',
    animation_frame='Scenario',
    facet_col=None,
    )

#%% by commodities
data = plot_f(
    folder = 'GHG footprints',
    file_prefix = 'f_ghg_com',
    aggregated = True,
    # reg_average=True,
    # scenario_list = 'baseline',
    keep_baseline=True,
    barmode='group',
    animation_frame='Scenario',
    facet_col=None,
    )

data = plot_f(
    folder = 'Energy footprints',
    file_prefix = 'f_ene_com',
    aggregated = True,
    # reg_average=True,
    # scenario_list = 'baseline',
    keep_baseline=True,
    barmode='group',
    animation_frame='Scenario',
    facet_col=None,
    )
# %%
