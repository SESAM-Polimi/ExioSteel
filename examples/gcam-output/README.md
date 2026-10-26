# GCAM result files used by the example

These CSVs contain **physical GCAM outputs**, before technology mapping and mix
normalisation. They are the files MARIO's adapter interprets. The two-region
teaching database and its inventories are constructed separately in `demo.py`.

## Electricity: published supplement

`electricity.csv` is an unchanged-value subset of sheet **8. Results | GCAM full
data** in `es5c15099_si_001.xlsx`, supporting Ghezzi et al. (2026),
[DOI 10.1021/acs.est.5c15099](https://doi.org/10.1021/acs.est.5c15099).

- [Public supporting dataset](https://acs.figshare.com/articles/dataset/Linking_Global_Integrated_Assessment_and_Hybrid_Input_Output_Models_To_Support_Steel_Decarbonization_Policies_in_Europe/32613266), DOI **10.1021/acs.est.5c15099.s001**.
- [Fixed XLSX download](https://ndownloader.figshare.com/files/65390366), MD5 `156a062eab9ee8d9f25af21faec1d4f1`.
- Dataset licence reported by ACS Figshare: [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/).
- Scope: all four study scenarios, EU-12 and EU-15, nine electricity variables and all ten available year columns, 2005–2050.

[`tools/extract_published_electricity.py`](../../tools/extract_published_electricity.py)
checks the fixed workbook checksum, trims metadata whitespace, selects and sorts
rows, then writes the numeric values with round-trip precision. It does **not**
calculate shares. `provenance.json` records the exact source, filter and CSV hash.

## Steel: regional export provided by the study maintainer

`steel.csv` is a subset of **SteelProdTech** in the study's original `GCAM data.xlsx`,
provided by the maintainer. It contains EU-12/EU-15 production in Mt/yr for the four
scenarios, 21 GCAM route variables and all ten years. The original file's extra
display column is omitted; production values are unchanged. See
`steel-provenance.json` for the source hash and extraction operations.
The extraction can be repeated with
[`tools/extract_study_steel.py`](../../tools/extract_study_steel.py); pass the
supplement with `--supplement` to repeat the aggregate comparison.

The published supplement's **4. Results | Figure3&4** table contains the steel
route results aggregated to **EU27**, rather than the two regional exports needed
for this coupling. We use the local export only for that missing regional detail.
Summing EU-12 and EU-15 and applying the route concordance reproduces all **504**
published scenario/year/technology aggregates to `rtol=1e-8, atol=1e-8 Mt/yr`;
maximum absolute difference is **1.42e-14 Mt/yr**. This is recorded in the provenance.

The regional export is supplied as study input data; cite the article and its
authors when using it. The electricity extract uses the supplement's CC BY-NC 4.0 licence.

## Preserve the source-to-shock distinction

The processed `support/gcam_data/*_mixes.xlsx` files are historical adapter outputs.
They already contain shares and MARIO labels. The new default example starts from
the physical CSVs above, exposes the mapping in `config/variables.json`, and writes
the interpretation and actual coefficient changes next to its results.
