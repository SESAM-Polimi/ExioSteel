"""Check local links, notebook syntax and the checked-in physical input hashes."""
import ast
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import unquote,urlsplit
ROOT=Path(__file__).resolve().parents[1]
errors=[]
for p in [ROOT/'README.md',*ROOT.glob('docs/*.md'),ROOT/'examples/gcam-output/README.md']:
 for target in re.findall(r'\[[^\]]*\]\(([^)]+)\)',p.read_text()):
  url=urlsplit(target)
  if not url.scheme and url.path and not (p.parent/unquote(url.path)).exists():
   errors.append(f'{p.relative_to(ROOT)}: missing {target}')
n=json.loads((ROOT/'notebooks/GCAM_to_MARIO.ipynb').read_text())
for i,c in enumerate(n['cells']):
 if c['cell_type']=='code':
  try:ast.parse(''.join(c['source']))
  except SyntaxError as e:errors.append(f'Notebook cell {i}: {e}')
for name,receipt in [('electricity','provenance'),('steel','steel-provenance')]:
 d=ROOT/'examples/gcam-output'
 meta=json.loads((d/(receipt+'.json')).read_text())
 if hashlib.sha256((d/(name+'.csv')).read_bytes()).hexdigest()!=meta['csv_sha256']:
  errors.append(f'{name}.csv no longer matches its source receipt')
if errors:raise SystemExit('\n'.join(errors))
print('Documentation links, notebook syntax and physical input hashes: OK')
