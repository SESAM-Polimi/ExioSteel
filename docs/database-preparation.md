# Prepare the extended EXIOBASE database

The main [mechanism example](from-gcam-to-mario.md) runs immediately with a small
illustrative SUT. This route applies the same shock functions to the full database.

```bash
gcam-mario prepare
gcam-mario run --scenario NDC_LTT_CBAM --year 2030 --offline
```

## Fixed source

The original script names the extended EXIOBASE **v1.3.0**. The corresponding
fixed record is [Zenodo 16779787](https://doi.org/10.5281/zenodo.16779787), under the
article's [concept DOI 10843374](https://doi.org/10.5281/zenodo.10843374).
[`config/zenodo-source.json`](../config/zenodo-source.json) pins six URLs, file
sizes and published MD5 checksums. The six comma-separated TXT files are `z`,
`v`, `e`, `Y`, `EY` and `units`; the optional `VY` is zero in the compatible source.

The downloader fetches only these matrices, resumes interrupted transfers,
verifies them and converts them to Parquet in row chunks. It retains the originals
and writes a receipt with source metadata and converted-file SHA-256 hashes.
Default cache: `models/source-16779787/`. No changing latest-version lookup occurs.

All six published matrix sizes/checksums in v1.3.1 and v1.3.2 equal v1.3.0.
An explicitly supplied existing source cache can be reused when its receipt
contains those exact matrix hashes and its Parquets match their recorded SHA-256.
The manifest retains the actual receipt's record ID. Other explicit Parquets can
be used, but are marked as not independently verified against the published source.

## Preparation stages

1. Parse the hybrid-units SUT in coefficient mode.
2. Apply the existing `aggr_to_EU12-EU15.xlsx` concordance, producing 23 regions.
   `zero_output_epsilon=None` preserves the original coefficient treatment of
   zero-output technologies under MARIO 1.0.0.
3. Restore source unit entries omitted by aggregation, retaining their actual
   scales rather than guessing a unit for surviving satellite accounts.
4. Add the original three-gas GHG account: fossil CO2 × 1, CH4 × 29.8 and N2O × 273.
   These source accounts are checked to use tonnes.
5. Save coefficient Parquets and a completion manifest in `models/prepared/`.

The manifest records input hashes, source equivalence evidence, aggregation and
preparation hashes, GWP values and the MARIO version. Loading verifies the recipe
and every coefficient-file hash. It does not need the original raw matrices.
An incompatible or incomplete prepared directory is rejected; choose a new path
instead of overwriting it.

## What the scenario changes

The prepared database contains technology inventories and background economic
relationships. GCAM mixes are applied afterwards to native `u`, `s` and `Yc`.
This separation lets multiple independent scenarios reuse the same preparation.
The default scenario scope is the two EU clusters; other regions keep their base
mixes. Total demand and bilateral trade are retained. The driver does not consume
the old steel-demand/import workbooks as additional shocks.

The historical reconstruction from raw EXIOBASE, inventory insertion and energy
extensions remains visible in the original scripts and workbooks. This portable
route starts from the published extended database; it does not rebuild those
upstream inventories. See [research scope](research-workflow.md).
