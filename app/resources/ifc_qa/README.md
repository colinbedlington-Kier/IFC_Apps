# IFC Data QA default configs

This folder ships the default configuration templates used by the IFC Data QA module.

## Files

- `qa_rules.template.csv`: Rule definitions used to validate extracted tables.
- `qa_property_requirements.template.csv`: Required property definitions per entity/pset.
- `qa_unacceptable_values.template.csv`: Disallowed values list for extracted fields.
- `regex_patterns.template.csv`: Regex patterns for naming and code checks.
- `exclude_filter.template.csv`: Substring filter list for excluding objects (same header as the desktop tool).
- `pset_template.template.csv`: Pset template dictionary definitions (same header as the desktop tool).

## Overrides (session-only)

Users can upload override CSVs from the UI. Overrides are stored in a temporary session directory and **never** committed back to this repo. Resetting to defaults removes the session override files and reverts to the templates in this folder.

## CSV schemas

### qa_rules.template.csv

Columns:
- `rule_id`: Unique rule identifier.
- `page`: Target dashboard page (e.g. `project_naming`, `occurrence_naming`).
- `table`: Source table name (e.g. `IFC OBJECT TYPE`).
- `field`: Column name to evaluate.
- `check_type`: `required`, `regex`, `equals`, `not_equals`, `contains`.
- `pattern`: Pattern or value to compare.
- `severity`: `low`, `medium`, `high`.
- `message`: Failure message.

### qa_property_requirements.template.csv

Columns:
- `ifc_entity`
- `pset_name`
- `property_name`
- `required` (`true`/`false`)
- `severity`
- `message`

### qa_unacceptable_values.template.csv

Columns:
- `field`
- `unacceptable_value`
- `severity`
- `message`

### regex_patterns.template.csv

Columns:
- `key`
- `pattern`
- `enabled` (`true`/`false`)
