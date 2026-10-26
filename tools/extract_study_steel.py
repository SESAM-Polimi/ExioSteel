"""Extract regional physical steel production, optionally checking published EU27 totals.

python tools/extract_study_steel.py 'GCAM data.xlsx' --supplement es5c15099_si_001.xlsx
No shares, zero floors or physical unit conversions are introduced here.
"""
import argparse
import hashlib
import json
from pathlib import Path
import numpy as np
import pandas as pd
ROOT=Path(__file__).resolve().parents[1]

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('workbook',type=Path)
    parser.add_argument('--supplement',type=Path)
    args=parser.parse_args()
    mapping=json.loads((ROOT/'config/variables.json').read_text())['steel']
    d=pd.read_excel(args.workbook,sheet_name='SteelProdTech')
    for col in ['Model','Scenario','Region','Variable','Unit']:d[col]=d[col].str.strip()
    d=d[d.Region.isin(['EU-12','EU-15'])&d.Variable.isin(mapping)].copy()
    years=[c for c in d if str(c).isdigit()]
    d=d[['Model','Scenario','Region','Variable','Unit',*years]].sort_values(['Scenario','Region','Variable'])
    target=ROOT/'examples/gcam-output/steel.csv'
    d.to_csv(target,index=False,float_format='%.17g')
    meta={'source_file':args.workbook.name,'sheet':'SteelProdTech',
          'source_sha256':hashlib.sha256(args.workbook.read_bytes()).hexdigest(),
          'provided_by':'Study maintainer','regions':['EU-12','EU-15'],'rows':len(d),
          'operations':['trim metadata whitespace','select rows and documented columns','sort rows','write CSV with round-trip precision'],
          'physical_values_changed':False,'csv_sha256':hashlib.sha256(target.read_bytes()).hexdigest(),
          'publication_note':'Published SI Figure3&4 table gives only EU27 totals; this file retains the regional GCAM output needed for the original coupling.'}
    if args.supplement:
        if hashlib.md5(args.supplement.read_bytes()).hexdigest()!='156a062eab9ee8d9f25af21faec1d4f1':
            raise ValueError('Unexpected supporting-information revision')
        a=d.melt(id_vars=['Scenario','Region','Variable'],value_vars=years,var_name='Year',value_name='Value')
        a['Year']=a.Year.astype(int);a['MarioItem']=a.Variable.map(mapping)
        a=a.groupby(['Scenario','Year','MarioItem']).Value.sum()
        b=pd.read_excel(args.supplement,sheet_name='4. Results | Figure3&4').iloc[:,:9].dropna(subset=['Year','Scenario','Steel technology','Value'])
        b['Year']=b.Year.astype(int)
        b['MarioItem']=b['Steel technology'].replace({'BF-BOF':mapping['Production|Steel|BF_BOF'],'EAF_scrap':mapping['Production|Steel|EAF_scrap_fossil_NG_finish']})
        b=b.groupby(['Scenario','Year','MarioItem']).Value.sum().sort_index();a=a.reindex(b.index)
        passed=bool(a.notna().all() and np.allclose(a,b,rtol=1e-8,atol=1e-8))
        meta['published_aggregate_comparison']={'sheet':'4. Results | Figure3&4','rows':len(b),
             'max_absolute_difference_Mt_yr':float((a-b).abs().max()),'pass':passed,'rtol':1e-8,'atol_Mt_yr':1e-8}
        if not passed:raise ValueError('Regional steel output does not match the published aggregates')
    (target.parent/'steel-provenance.json').write_text(json.dumps(meta,indent=2)+'\n')
    print(f'Extracted {len(d)} regional GCAM production rows; physical values unchanged')

if __name__=='__main__':main()
