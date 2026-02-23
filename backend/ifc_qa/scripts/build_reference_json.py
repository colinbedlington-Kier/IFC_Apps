import csv, json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
RES = ROOT / 'resources'
OUT = ROOT / 'backend' / 'ifc_qa' / 'reference'
OUT.mkdir(parents=True, exist_ok=True)

def read_rows(path):
    if not path.exists():
        return []
    with open(path, encoding='utf-8-sig', newline='') as f:
        return list(csv.DictReader(f))

pset_rows = read_rows(RES / 'GPA_Pset_Template.csv') or read_rows(RES / 'Pset_Template.csv')
pset = {}
for r in pset_rows:
    key = (r.get('IFC_Entity_Occurrence_Type') or '').strip()
    vals = (r.get('Pset_Dictionaries') or '').strip()
    if not key:
        continue
    entries=[]
    for p in [x.strip() for x in vals.replace(';', ',').split(',') if x.strip()]:
        entries.append({'Property_Set_Template': p, 'Property_Name_Template': ''})
    pset[key]=entries

exclude_rows = read_rows(RES / 'Exclude_Filter_Template.csv')
property_exclusions = [r.get('Exclude_Filter','').strip() for r in exclude_rows if (r.get('Exclude_Filter') or '').strip()]

# defaults intentionally lightweight and editable in-session
(OUT / 'default_config.json').write_text(json.dumps({'version': 1, 'generated_from': 'csv'}, indent=2), encoding='utf-8')
(OUT / 'pset_template.json').write_text(json.dumps(pset, indent=2), encoding='utf-8')
(OUT / 'uniclass_system_category.json').write_text(json.dumps([], indent=2), encoding='utf-8')
(OUT / 'short_codes.json').write_text(json.dumps([], indent=2), encoding='utf-8')
(OUT / 'layers.json').write_text(json.dumps([], indent=2), encoding='utf-8')
(OUT / 'entity_types.json').write_text(json.dumps([], indent=2), encoding='utf-8')
(OUT / 'property_exclusions.json').write_text(json.dumps(property_exclusions, indent=2), encoding='utf-8')
print('Reference JSON generated in', OUT)
