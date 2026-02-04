IFC Data QA Template Pack

These CSVs are repo defaults used by the IFC Data QA module (React + API).

Override model:
- UI allows uploading overrides (CSV) per session.
- Backend loads override if provided, else uses these repo defaults.

Files:
- qa_rules.template.csv: High-level QA checks (naming, required fields, duplicates, etc.)
- qa_property_requirements.template.csv: Property requirements (MRR/MER/SER) derived from GPA Requirement Conversion.xlsx
- qa_unacceptable_values.template.csv: Token list derived from requirement sheets
- regex_patterns.template.csv: Regex defaults derived from GPA Regex Patterns.xlsx (+ system syntax default)
- exclude_filter.template.csv: Substring excludes for IFC_Name
- pset_template.template.csv: Pset dictionaries per IFC occurrence type
