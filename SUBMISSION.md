# My dataset submission

## Inputs and outputs

**Input tables** (read-only, `training.to6_challenge3_inputs`):

| Full table name | Description |
| --- | --- |
| `training.to6_challenge3_inputs.returns_initial` | Initial synthetic delivery from `parsed_source`; known defects — rejected (see Checks and decisions) |
| `training.to6_challenge3_inputs.returns_corrected` | Corrected synthetic delivery from `parsed_source`; passes all checks — used for final output |
| `training.to6_challenge3_inputs.source_lineage` | Lineage view linking both deliveries to upstream `training.to6_challenge3_inputs.parsed_source` |

**Output tables** (writable schema `training.to6_c3_5b9510254373`):

| Full table name | Contents |
| --- | --- |
| `training.to6_c3_5b9510254373.return_features` | All accepted returns, 2022–2024: `return_id`, `tax_year`, `deduction_to_income` |
| `training.to6_c3_5b9510254373.features_train` | 2022 returns (training split) |
| `training.to6_c3_5b9510254373.features_validation` | 2023 returns (validation split) |
| `training.to6_c3_5b9510254373.features_test` | 2024 returns (test split) |

**Row meaning:** One row = one tax return. `return_id` is the uniqueness identifier; `taxpayer_id` identifies the filer (excluded from features under the need-to-know rule). A filer may have returns in multiple years.

**Ratio definition:** `deduction_to_income` = `deduction / income`. A zero-income return stays in the table with a null ratio (not zero or dropped).

## Checks and decisions

<!-- Fill in after completing notebook section 2. Include: -->
<!-- - Source references (Catalog table comments and lineage view result) -->
<!-- - Initial batch rejection: which checks failed, the actual failing records, and the saved export path -->
<!-- - Replacement batch checks: same completeness and uniqueness checks applied to returns_corrected, with results -->
<!-- - Independently derived expected nonzero deduction_to_income value used to verify one row -->
<!-- - Saved-table readback: row count, one nonzero row, and the zero-income row confirmed as null ratio -->

## Two successful runs

<!-- Fill in after completing two full successful runs (sections 2–4). Include for each run: -->
<!-- - Notebook URL and execution timestamp (or Jobs run URL/ID) -->
<!-- - Name the retained exports or comparison evidence (e.g. outputs/run1_features.csv vs outputs/run2_features.csv) -->
<!-- - How you verified year assignment, exactly one copy of every accepted return, and no appended copies -->

## Final files and limitations

<!-- Fill in after completing exports. List every file under outputs/, e.g.: -->
<!-- - outputs/practice_connection.json  (ungraded practice export; not part of task) -->
<!-- - outputs/initial_rejected.csv  (affected records from returns_initial with rejection reasons) -->
<!-- - outputs/return_features.csv  (full feature table, 12 rows) -->
<!-- - outputs/features_train.csv  (2022, 4 rows) -->
<!-- - outputs/features_validation.csv  (2023, 4 rows) -->
<!-- - outputs/features_test.csv  (2024, 4 rows) -->
<!-- - outputs/run1_features.csv and outputs/run2_features.csv  (or equivalent comparison evidence) -->
<!-- State anything you could not verify. -->
