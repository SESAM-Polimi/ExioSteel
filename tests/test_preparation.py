import json
from pathlib import Path

import pandas as pd
import pytest

from gcam_mario import demo_database
from gcam_mario.preparation import prepare_database, load_prepared
from gcam_mario.download import REQUIRED


def test_prepared_bundle_exports_to_expected_layout_and_reloads_without_source(tmp_path, monkeypatch):
    import mario
    mario.set_log_verbosity("error")
    db = demo_database()
    # Reproduce aggregation retaining a numeric row outside its metadata index.
    account = "Carbon dioxide, fossil (air - Emiss)"
    original_get_index = db.get_index
    monkeypatch.setattr(db, "get_index", lambda level: [x for x in original_get_index(level) if x != account]
                        if level == "Satellite account" else original_get_index(level))
    db.units["Satellite account"].drop(index=account, inplace=True)
    original_parser = mario.parse_from_parquet
    source = tmp_path/"source"
    source.mkdir()
    for stem in REQUIRED:
        (source/f"{stem}.parquet").write_bytes(b"placeholder: parsing and aggregation are replaced in this layout contract")
    pd.DataFrame({"unit": ["tonnes"]}, index=pd.MultiIndex.from_tuples([("Satellite account", account)],
                 names=["Level", "Item"])).to_parquet(source/"units.parquet")
    monkeypatch.setattr(db, "aggregate", lambda **kwargs: None)
    monkeypatch.setattr(db, "calc_ghg", lambda **kwargs: None)
    monkeypatch.setattr(mario, "parse_from_parquet", lambda **kwargs: db if Path(kwargs["path"]) == source else original_parser(**kwargs))
    prepared = tmp_path/"prepared"
    _, manifest = prepare_database(prepared, source=source, offline=True, progress=lambda _: None)
    assert (prepared/"coefficients/z.parquet").is_file()
    assert not (prepared/"coefficients/coefficients").exists()
    for path in source.iterdir(): path.unlink()
    source.rmdir()
    restored, loaded = load_prepared(prepared)
    assert restored.get_index("Region") == db.get_index("Region")
    assert loaded == manifest
    pd.testing.assert_frame_equal(restored.query(matrices="u", scenarios="baseline"), db.query(matrices="u", scenarios="baseline"))
    (prepared/"coefficients/e.parquet").write_bytes(b"damaged")
    with pytest.raises(ValueError, match="checksum mismatch"):
        load_prepared(prepared)


def test_offline_without_source_never_downloads(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("gcam_mario.preparation.download_database", lambda **kwargs: pytest.fail("network call"))
    with pytest.raises(FileNotFoundError, match="offline"):
        prepare_database(tmp_path/"prepared", offline=True)
