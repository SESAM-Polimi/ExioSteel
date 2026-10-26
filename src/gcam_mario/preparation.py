"""Prepare the extended EXIOBASE SUT once, with EU-12/EU-15 regional aggregation."""
from datetime import datetime, timezone
from importlib.metadata import version
import json
from pathlib import Path
import tempfile

from .demo import GWP
from .download import download_database, DEFAULT_DIRECTORY, REQUIRED, _verify_converted
from .resources import resource_root, sha256, configuration


def _recipe():
    return {"mariopy": version("mariopy"), "preparation_sha256": sha256(__file__), "gwp": GWP,
            "aggregation_sha256": sha256(resource_root()/"support/aggregations/aggr_to_EU12-EU15.xlsx")}


def load_prepared(path):
    """Verify a locally prepared bundle and return (MARIO Database, manifest)."""
    import mario
    path = Path(path).expanduser().resolve()
    manifest = json.loads((path / "manifest.json").read_text())
    if manifest.get("format_version") != 1 or manifest.get("recipe") != _recipe():
        raise ValueError("Incompatible prepared database; prepare a new bundle in another directory")
    required = {f"coefficients/{s}.parquet" for s in REQUIRED}
    if not required <= set(manifest.get("files", {})):
        raise ValueError("Incomplete prepared database manifest")
    for name, digest in manifest["files"].items():
        file = (path/name).resolve()
        if not file.is_relative_to(path) or not file.is_file() or sha256(file) != digest:
            raise ValueError(f"Prepared database checksum mismatch: {name}")
    return mario.parse_from_parquet(path=str(path/"coefficients"), table="SUT", mode="coefficients"), manifest


def prepare_database(destination="models/prepared", *, source=None, offline=False, progress=print):
    """Download if needed, aggregate the EU, add GHG and save a verified bundle.

    Explicit source accepts a local coefficient-Parquet directory. A source receipt
    is checked when present. Version 1.3.2 is accepted as byte-equivalent to the
    six published v1.3.0 matrices only when the receipts' published hashes match.
    """
    import mario
    import pandas as pd

    destination = Path(destination).expanduser().resolve()
    if (destination/"manifest.json").is_file():
        return load_prepared(destination)
    if destination.exists() and any(destination.iterdir()):
        raise ValueError("Prepared directory is not empty; choose another destination")
    if source is None:
        if offline:
            source = DEFAULT_DIRECTORY.resolve()
            if not _verify_converted(source):
                raise FileNotFoundError("No verified source cache offline; run gcam-mario download or provide --source")
        else:
            source = download_database(progress=progress)
    source = Path(source).expanduser().resolve()
    for stem in REQUIRED:
        p = source/f"{stem}.parquet"
        if not p.is_file() or (p.stat().st_size < 1024 and p.read_bytes().startswith(b"version https://git-lfs")):
            raise FileNotFoundError(f"Missing source Parquet: {p}")
    input_hashes = {f"{s}.parquet": sha256(source/f"{s}.parquet") for s in REQUIRED}
    source_info = {"requested_record": 16779787, "version": "1.3.0", "published_source_verified": False}
    receipt_path = source/"zenodo-source.json"
    if receipt_path.is_file():
        receipt = json.loads(receipt_path.read_text())
        pinned = configuration("zenodo-source.json")
        signature = lambda files: {f["key"]: (f["size"], f["checksum"]) for f in files}
        if signature(receipt.get("files", [])) != signature(pinned["files"]):
            raise ValueError("Source receipt does not describe the fixed v1.3.0 matrices")
        if receipt.get("parquet_sha256") != input_hashes:
            raise ValueError("Source Parquet checksums do not match the receipt")
        source_info.update(published_source_verified=True, receipt_record=receipt["record_id"],
                           equivalent_published_matrix_hashes=True)
    progress("Parsing the extended EXIOBASE coefficient database (first preparation).")
    db = mario.parse_from_parquet(path=str(source), table="SUT", mode="coefficients")
    progress("Aggregating EU countries to GCAM's EU-12 and EU-15 clusters.")
    db.aggregate(io=str(resource_root()/"support/aggregations/aggr_to_EU12-EU15.xlsx"),
                 ignore_nan=True, zero_output_epsilon=None)
    original_units = pd.read_parquet(source/"units.parquet")
    # Aggregation can retain a numeric satellite row while omitting it from the
    # metadata index. Inspect actual matrix axes, not only db.get_index().
    native_u = db.query(matrices="u", scenarios="baseline")
    actual_labels = {"Activity": native_u.columns.get_level_values(2).unique(),
                     "Commodity": native_u.index.get_level_values(2).unique(),
                     "Factor of production": db.query(matrices="v", scenarios="baseline").index,
                     "Satellite account": db.query(matrices="e", scenarios="baseline").index}
    for level, units in list(db.units.items()):
        for item in actual_labels.get(level, db.get_index(level)):
            if item not in units.index:
                if (level, item) not in original_units.index:
                    raise ValueError(f"Missing source unit: {level}/{item}")
                units.loc[item, "unit"] = original_units.loc[(level, item), "unit"]
    for account in GWP:
        if str(db.units["Satellite account"].loc[account, "unit"]) != "tonnes":
            raise ValueError(f"Expected tonnes for {account}; do not combine differently scaled GHG units")
    db.calc_ghg(gwp=GWP, label="GHG")
    progress("Saving the reusable database and provenance manifest.")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".gcam-prepare-", dir=destination.parent) as tmp:
        bundle = Path(tmp)/"bundle"
        db.to_parquet(path=str(bundle), flows=False, coefficients=True)
        manifest = {"format_version": 1, "created_utc": datetime.now(timezone.utc).isoformat(),
                    "source": source_info, "input_sha256": input_hashes, "recipe": _recipe(),
                    "files": {str(f.relative_to(bundle)): sha256(f) for f in sorted(bundle.rglob("*.parquet"))}}
        (bundle/"manifest.json").write_text(json.dumps(manifest, indent=2)+"\n")
        # A prepared bundle is complete only when the real parser can reload it.
        db, _ = load_prepared(bundle)
        if destination.exists():
            destination.rmdir()
        bundle.rename(destination)
    return db, manifest
