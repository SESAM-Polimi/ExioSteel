# Verification — 26 October 2026

The checks distinguish execution of the coupling from reproduction of the paper.

## Verified input sources

- The fixed ACS supplementary XLSX was downloaded and checked against its
  published MD5. Its 72 EU electricity rows were extracted without normalising
  or changing physical values.
- The maintainer supplied the original regional steel export. EU-12 + EU-15
  route production agrees with all 504 published EU27 values at `rtol=1e-8`,
  `atol=1e-8 Mt/yr`; maximum absolute difference 1.42e-14 Mt/yr.
- All six extended-EXIOBASE v1.3.0 source sizes and published MD5 values match
  the previously downloaded and verified v1.3.2 matrices. The local source
  receipt's converted-file hashes are checked before reuse.

## Automated contracts

**25 tests passed.** All **11 code cells** of the worked notebook executed,
including the physical input tables, actual coefficient read-back, figure,
independent electricity/steel/combined scenarios and trace export.

Tests cover physical-value provenance, many-to-one route mapping, complete
scenario/year mixes, exact zeros, invalid units/values/duplicates/missing data,
region ambiguity, independent scenarios, actual MARIO matrix updates, conservation
by supplying region and consuming column, the electrolysis exclusion, failed
scenario cleanup, output integrity and prepared-model export/reload.

Downloader tests use small simulated HTTP responses for resume/retry/checksum
behaviour and real chunked CSV-to-Parquet conversion. CI does not download the
1.78 GB database. The prepared-layout contract mocks source parsing/aggregation
and uses real MARIO export/reload; full-source execution is reported separately.

The fixed-record downloader was also exercised against the real `units.txt`
endpoint, including a resumed partial transfer and published-checksum verification.

## Full database and installed package

The full source was prepared in MARIO 1.0.0, exported and **reloaded with the real
Parquet parser**. The retained cement-CO2 satellite's kg unit was restored from
the actual source because aggregation kept its numeric row outside its metadata
index. The restoration follows matrix axes and changes no numeric coefficients.
Preparation now verifies a reload before marking the bundle complete.

An installed wheel was run **outside the checkout**, in offline mode, using that
prepared 23-region database. The user CLI applied NDC_LTT_CBAM, 2030 and exported
the complete source-to-coefficient trace. Independent checks verified the unchanged
baseline and technology satellite coefficients, electricity totals by origin and
consumer, steel supply totals, the electrolysis exclusion, all output hashes and
the actual coefficients against `share × original total`. See the
[machine-readable report](../verification/full-model.json).

The full export has 60 source rows and 72 coefficient-inspection rows. Its source
steel-route share totals are approximately 1.000016 in each EU cluster; the shock
preserves those original totals rather than silently renormalising the database.
GCAM target mix weights themselves sum to one.

The [database record comparison](../verification/database-records.json) documents
the identical six published matrix signatures across 1.3.0, 1.3.1 and 1.3.2. Large
files already verified from the identical v1.3.2 record were reused; they were not
downloaded a second time just to change the record ID.

```bash
python -m pytest -q
python tools/check_docs.py
```

## Scope

The small SUT's inventories and baseline demand are fictional. Its GCAM inputs
are real; its footprints illustrate the mechanism and are not paper results.
The full workflow starts with the published extended database and applies the
selected EU mix shocks. It does not run GCAM, rebuild the database from raw
EXIOBASE, apply absolute GCAM demand/trade volumes, or claim full figure-by-figure
reproduction. The historical root scripts were inspected rather than executed.
