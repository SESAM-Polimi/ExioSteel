"""Extract an unchanged subset of the published GCAM results (no share calculation).

Usage: python tools/extract_published_electricity.py es5c15099_si_001.xlsx
The source is ACS Figshare 32613266, file 65390366, licensed CC BY-NC 4.0.
"""
from pathlib import Path
import argparse
import hashlib
import json
import pandas as pd

ROOT=Path(__file__).resolve().parents[1]

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('workbook',type=Path)
    args=p.parse_args()
    expected='156a062eab9ee8d9f25af21faec1d4f1'
    if hashlib.md5(args.workbook.read_bytes()).hexdigest()!=expected:
        raise ValueError('This extractor targets the fixed published SI workbook; checksum differs')
    mappings=json.loads((ROOT/'config/variables.json').read_text())['electricity']
    data=pd.read_excel(args.workbook,sheet_name='8. Results | GCAM full data')
    for col in ['Model','Scenario','Region','Variable','Unit']:
        data[col]=data[col].str.strip()
    data=data[data.Variable.isin(mappings)&data.Region.isin(['EU-12','EU-15'])].copy()
    data=data.sort_values(['Scenario','Region','Variable'])
    target=ROOT/'examples/gcam-output/electricity.csv'
    data.to_csv(target,index=False,float_format='%.17g')
    meta={'source_doi':'10.1021/acs.est.5c15099.s001','source_url':'https://acs.figshare.com/articles/dataset/Linking_Global_Integrated_Assessment_and_Hybrid_Input_Output_Models_To_Support_Steel_Decarbonization_Policies_in_Europe/32613266',
          'download_url':'https://ndownloader.figshare.com/files/65390366','source_file':'es5c15099_si_001.xlsx',
          'source_md5':expected,'sheet':'8. Results | GCAM full data','regions':['EU-12','EU-15'],
          'variables':list(mappings),'operations':['trim metadata whitespace','select rows','sort rows','write CSV with round-trip precision'],
          'physical_values_changed':False,'rows':len(data),'license':'CC BY-NC 4.0',
          'csv_sha256':hashlib.sha256(target.read_bytes()).hexdigest()}
    (target.parent/'provenance.json').write_text(json.dumps(meta,indent=2)+'\n')
    print(f'{len(data)} GCAM rows exported without normalisation: {target}')

if __name__=='__main__':main()
