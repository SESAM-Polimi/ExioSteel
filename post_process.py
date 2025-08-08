#%%
import dask.dataframe as dd
import pandas as pd

main_folder = '/Users/lorenzorinaldi/Library/CloudStorage/OneDrive-SharedLibraries-PolitecnicodiMilano/DENG-SESAM - Documenti/c-Research/a-Datasets/IAM COMPACT Study 9'

#%%
def weighted_eu_footprint_with_from_cols_fix(
    main_folder: str,
    ex_act_filename: str,
    x_filename: str = "X.parquet",
    sets_filename: str = "sets.xlsx",
    sets_sheet: str = "_set_REGION_TO",
    drop_index: bool = True
) -> pd.DataFrame:
    """
    Calcola la media pesata EU della footprint, mantenendo sempre Activity_from e Region_from.
    Risolve problemi di duplicazione dati nella join tra footprint e produzione.
    Ritorna DataFrame con colonne:
    Scenario, Year, Activity_from, Region_from, Activity_to, Region_to, WeightedValue
    """

    # Percorsi file
    X_path = f"{main_folder}/Results/Merged/{x_filename}"
    ex_act_path = f"{main_folder}/Results/Merged/{ex_act_filename}"
    sets_path = f"{main_folder}/Results/{sets_filename}"

    # Caricamento parquet (Dask)
    f_ex_act = dd.read_parquet(ex_act_path)
    X = dd.read_parquet(X_path)
    X = X.rename(columns={'Activity_from': 'Activity_to', 'Region_from': 'Region_to'})
    if drop_index and 'index' in f_ex_act.columns:
        f_ex_act = f_ex_act.drop(columns=['index'])

    # Aggrega X: ogni combinazione deve essere unica!
    X_unique = X.groupby(['Region_to', 'Activity_to', 'Scenario', 'Year'])['Value'].sum().reset_index()

    # Carica Excel (pandas)
    sets = pd.read_excel(sets_path, sheet_name=None)
    set_REGION_TO = sets[sets_sheet]

    # Join footprint + info EU
    merged = f_ex_act.merge(
        set_REGION_TO[['Region_to', 'Region_to_agg']],
        on='Region_to', how='left'
    )
    merged = merged[merged['Region_to_agg'] == 'EU']

    # Merge footprint <-> produzione (X unica)
    merged = merged.merge(
        X_unique,
        on=['Region_to', 'Activity_to', 'Scenario', 'Year'],
        how='left',
        suffixes=('_footprint', '_prod')
    )

    # Prodotto pesato
    merged['WeightedFootprint'] = merged['Value_footprint'] * merged['Value_prod']

    # Raggruppa per Scenario, Year, Activity_from, Region_from, Activity_to
    group_cols = ['Scenario', 'Year', 'Activity_from', 'Region_from', 'Activity_to']
    agg = merged.groupby(group_cols).agg({
        'WeightedFootprint': 'sum',
        'Value_prod': 'sum'
    }).reset_index()

    agg['Region_to'] = 'EU'
    agg['WeightedValue'] = agg['WeightedFootprint'] / agg['Value_prod']

    final_cols = group_cols + ['Region_to', 'WeightedValue']
    result = agg[final_cols]
    result_df = result.compute()

    return result_df

#%% ghg footprints 
df_ghg = weighted_eu_footprint_with_from_cols_fix(main_folder, "f_ex_ghg_act.parquet")
print(df_ghg.head())
df_ghg.to_parquet(f"{main_folder}/Results/Merged/f_ex_ghg_act_EU.parquet", index=False)

#%% energy footprints
df_ene = weighted_eu_footprint_with_from_cols_fix(main_folder, "f_ex_ene_act.parquet")
print(df_ene.head())
df_ene.to_parquet(f"{main_folder}/Results/Merged/f_ex_ene_act_EU.parquet", index=False)
# %%
