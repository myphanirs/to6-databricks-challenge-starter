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

**Source references**
Catalog table comments (notebook cell *1a. Catalog descriptions*): `returns_initial` — *"Initial synthetic delivery from parsed_source; known delivery defects. Validate before feature output."*; `returns_corrected` — *"Corrected synthetic delivery from parsed_source; one row per return. Validate required values and return_id uniqueness."* Lineage confirmed (cell *1c.*): both tables derived from `training.to6_challenge3_inputs.parsed_source`.

**Initial batch rejected — `returns_initial`**

Two checks run (notebook cell *2a. Completeness and uniqueness checks on returns_initial*):

1. **Completeness** — any row with a null `return_id`, `tax_year`, `income`, or `deduction`: **1 failure** — R02 (2022, null income)
2. **Uniqueness** — any `return_id` appearing more than once: **2 failures** — R01 appears twice (2022)

Both checks failed. `returns_initial` is rejected; no feature output was written from it. All 3 affected rows (R01 × 2 with `rejection_reason = duplicate_return_id`; R02 × 1 with `rejection_reason = missing_required_field`) are preserved in `outputs/initial_rejected.csv`.

**Replacement batch accepted — `returns_corrected`**

Same checks applied (cell *2c. Apply same checks to returns_corrected*):
- Completeness failures: **0**
- Uniqueness failures: **0**

`returns_corrected` passes both checks and is used as the sole source for feature output.

**Independent expected-value check**

R04 (2022) in `returns_corrected`: `income = 400`, `deduction = 59`. Expected `deduction_to_income = 59 / 400 = 0.1475`, derived directly from source values without reading the saved table. Value read back from `training.to6_c3_5b9510254373.return_features`: **0.1475** ✓ (cell *3b. Independent expected-value check*).

**Saved-table readback** (cell *3c. Readback: row count, zero-income null ratio, export CSV*)
- Row count: **12** (all 12 accepted returns, one per `return_id`) ✓
- Zero-income rows with null ratio: **2** — R03 (2022) and R09 (2024) both have `deduction_to_income = null` ✓
- Both assertions passed without error.

## Two successful runs

**Run 1** — interactive notebook, cells 2a – 4b 
Notebook: `/Users/my.t.phan.irs@catalyte.ai/to6-databricks-challenge-starter/notebook` 
Started: `2026-11-20T16:29:10Z` (cell *2a* first executed); completed: `2026-11-20T16:39:39Z` (cell *4b* last executed). 
Retained snapshot: `outputs/run1_features.csv` (12 rows, `return_id` + `tax_year` + `deduction_to_income`).

**Run 2** — same notebook, cell 4c (`4c. Run 2: re-execute pipeline and compare with run-1`) 
Timestamp: `2026-11-20T16:39:54Z`. 
Retained snapshot: `outputs/run2_features.csv` (12 rows).

**Comparison evidence** 
In cell *4c*, `run1_pdf.equals(run2_pdf)` returned `True` — same return IDs and `deduction_to_income` values in both runs. Full snapshots are printed in cells *4b* and *4c*. Row counts confirm no appended copies: all writes use `mode("overwrite")`; after run 2, every table still contains the same 12 rows (not 24).

**Year assignment check** (cell *4a*): each split table is filtered by `tax_year == year`, then `filter(tax_year != year).count() == 0` asserted for all three — features_train 4 rows year=2022 ✓, features_validation 4 rows year=2023 ✓, features_test 4 rows year=2024 ✓. Union of all three splits = 12 rows, 12 distinct `return_id`s — every accepted return in exactly one table.

## Final files and limitations

**Exports under `outputs/`**

| File | Rows | Description |
| --- | --- | --- |
| `connection-practice.json` | — | Ungraded practice export; not part of task assessment |
| `initial_rejected.csv` | 3 | Rows rejected from `returns_initial`: R01 × 2 (`duplicate_return_id`), R02 × 1 (`missing_required_field`) |
| `return_features.csv` | 12 | Full feature table: `return_id`, `tax_year`, `deduction_to_income` (R03 and R09 have null ratio) |
| `features_train.csv` | 4 | 2022 returns (training split) |
| `features_validation.csv` | 4 | 2023 returns (validation split) |
| `features_test.csv` | 4 | 2024 returns (test split) |
| `run1_features.csv` | 12 | Run-1 snapshot of `return_features` for two-run comparison |
| `run2_features.csv` | 12 | Run-2 snapshot of `return_features` for two-run comparison |

**What was not verified**
- The `source_lineage` view was read as a table; its result was not cross-checked against the Unity Catalog UI lineage graph.
- `training.to6_challenge3_inputs.parsed_source` (the upstream table) was not read directly; delivery quality was assessed only on the two delivered tables.
- Model leakage from `return_id` patterns was not assessed; this task does not train a model or supply its labels.
