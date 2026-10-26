"""Locate the same input resources in a checkout and an installed wheel."""
from pathlib import Path
import hashlib
import json


def resource_root():
    root = Path(__file__).resolve().parents[2]
    return root if (root / "config/variables.json").is_file() else Path(__file__).parent / "resources"


def configuration(name):
    return json.loads((resource_root() / "config" / name).read_text())


def sha256(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()
