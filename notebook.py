# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Prepare the next team's dataset
# MAGIC Follow CHALLENGE.md. These headings are a starting point; implement and verify each stage yourself.

# COMMAND ----------

spark.sql("SELECT 1 AS connection_check").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 1 AS connection_check

# COMMAND ----------

pwd


# COMMAND ----------

# DBTITLE 1,Connection practice export
from pathlib import Path
import json

# Copy this Git folder's absolute path from its Workspace menu; include /Workspace/.
repo_root = Path("/Workspace/Users/my.t.phan.irs@catalyte.ai/to6-databricks-challenge-starter")
assert (repo_root / "README.md").is_file(), "Check the Git folder path"
practice_result = spark.sql("SELECT 1 AS connection_check").first().asDict()
assert practice_result == {"connection_check": 1}
(repo_root / "outputs").mkdir(exist_ok=True)
(repo_root / "outputs/connection-practice.json").write_text(
    json.dumps(practice_result) + "\n", encoding="utf-8"
)

# COMMAND ----------

import hashlib

INPUT_SCHEMA = "training.to6_challenge3_inputs"
INITIAL_TABLE = f"{INPUT_SCHEMA}.returns_initial"
CORRECTED_TABLE = f"{INPUT_SCHEMA}.returns_corrected"
LINEAGE_VIEW = f"{INPUT_SCHEMA}.source_lineage"
workspace_user = spark.sql("SELECT current_user() AS username").first()["username"]
OUTPUT_SCHEMA = "training.to6_c3_" + hashlib.sha256(workspace_user.encode("utf-8")).hexdigest()[:12]
print({"input_schema": INPUT_SCHEMA, "output_schema": OUTPUT_SCHEMA})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Understand the input
# MAGIC Record Catalog descriptions, sample observations and the supplied lineage source.

# COMMAND ----------

# Catalog descriptions (table comments) and column definitions for the inputs and lineage view
catalog_desc = spark.sql(f"""
    SELECT table_catalog || '.' || table_schema || '.' || table_name AS full_name, table_type, comment
    FROM training.information_schema.tables
    WHERE table_schema = '{INPUT_SCHEMA.split('.')[1]}'
      AND table_name IN ('returns_initial', 'returns_corrected', 'source_lineage')
    ORDER BY table_name
""")
display(catalog_desc)
display(spark.sql(f"DESCRIBE TABLE {INITIAL_TABLE}"))
# Catalog descriptions (table comments) and column definitions for the inputs and lineage view


# COMMAND ----------

# At least three sample rows from each delivery
display(spark.table(INITIAL_TABLE).orderBy("return_id").limit(5))
display(spark.table(CORRECTED_TABLE).orderBy("return_id").limit(5))

# COMMAND ----------

# DBTITLE 1,1a. Catalog descriptions
# Catalog descriptions (table comments) and column definitions for the inputs and lineage view
catalog_desc = spark.sql(f"""
    SELECT table_catalog || '.' || table_schema || '.' || table_name AS full_name, table_type, comment
    FROM training.information_schema.tables
    WHERE table_schema = '{INPUT_SCHEMA.split('.')[1]}'
      AND table_name IN ('returns_initial', 'returns_corrected', 'source_lineage')
    ORDER BY table_name
""")
display(catalog_desc)
display(spark.sql(f"DESCRIBE TABLE {INITIAL_TABLE}"))

# COMMAND ----------

# DBTITLE 1,1b. Sample rows
# At least three sample rows from each delivery
display(spark.table(INITIAL_TABLE).orderBy("return_id").limit(5))
display(spark.table(CORRECTED_TABLE).orderBy("return_id").limit(5))

# COMMAND ----------

# Supplied lineage view: which upstream table each delivery was built from
display(spark.table(LINEAGE_VIEW))

# COMMAND ----------

# DBTITLE 1,1c. Upstream source from lineage view
# Supplied lineage view: which upstream table each delivery was built from
display(spark.table(LINEAGE_VIEW))

# COMMAND ----------

# Profile the FULL batch (not just samples): rows, distinct IDs, missing required values,
# duplicate return IDs, zero-income and negative-amount records, by delivery and tax year
REQUIRED = ["return_id", "tax_year", "income", "deduction"]

def profile_sql(table, label):
    missing = " OR ".join(f"{c} IS NULL" for c in REQUIRED)
    return f"""
    SELECT '{label}' AS batch, tax_year,
           COUNT(*) AS rows,
           COUNT(DISTINCT return_id) AS distinct_return_ids,
           COUNT(*) - COUNT(DISTINCT return_id) AS duplicate_rows,
           SUM(CASE WHEN {missing} THEN 1 ELSE 0 END) AS rows_missing_required,
           SUM(CASE WHEN income = 0 THEN 1 ELSE 0 END) AS zero_income,
           SUM(CASE WHEN income < 0 OR deduction < 0 THEN 1 ELSE 0 END) AS negative_amounts
    FROM {table} GROUP BY tax_year"""

profile = spark.sql(profile_sql(INITIAL_TABLE, "initial") + " UNION ALL " + profile_sql(CORRECTED_TABLE, "corrected")).orderBy("batch", "tax_year")
display(profile)

# Return IDs appearing more than once across the whole batch (duplicates can span years)
for label, table in [("initial", INITIAL_TABLE), ("corrected", CORRECTED_TABLE)]:
    print(label, "- duplicate return_ids:")
    display(spark.sql(f"SELECT return_id, COUNT(*) AS n FROM {table} GROUP BY return_id HAVING COUNT(*) > 1"))

# COMMAND ----------

# DBTITLE 1,1d. Full-batch profile by year
# Profile the FULL batch (not just samples): rows, distinct IDs, missing required values,
# duplicate return IDs, zero-income and negative-amount records, by delivery and tax year
REQUIRED = ["return_id", "tax_year", "income", "deduction"]

def profile_sql(table, label):
    missing = " OR ".join(f"{c} IS NULL" for c in REQUIRED)
    return f"""
    SELECT '{label}' AS batch, tax_year,
           COUNT(*) AS rows,
           COUNT(DISTINCT return_id) AS distinct_return_ids,
           COUNT(*) - COUNT(DISTINCT return_id) AS duplicate_rows,
           SUM(CASE WHEN {missing} THEN 1 ELSE 0 END) AS rows_missing_required,
           SUM(CASE WHEN income = 0 THEN 1 ELSE 0 END) AS zero_income,
           SUM(CASE WHEN income < 0 OR deduction < 0 THEN 1 ELSE 0 END) AS negative_amounts
    FROM {table} GROUP BY tax_year"""

profile = spark.sql(profile_sql(INITIAL_TABLE, "initial") + " UNION ALL " + profile_sql(CORRECTED_TABLE, "corrected")).orderBy("batch", "tax_year")
display(profile)

# Return IDs appearing more than once across the whole batch (duplicates can span years)
for label, table in [("initial", INITIAL_TABLE), ("corrected", CORRECTED_TABLE)]:
    print(label, "- duplicate return_ids:")
    display(spark.sql(f"SELECT return_id, COUNT(*) AS n FROM {table} GROUP BY return_id HAVING COUNT(*) > 1"))

# COMMAND ----------

# DBTITLE 1,1e. Source reference and findings
# MAGIC %md
# MAGIC ### Source reference and findings
# MAGIC **Source reference**
# MAGIC * Inputs: `training.to6_challenge3_inputs.returns_initial` (Catalog: *"Initial synthetic delivery from parsed_source; known delivery defects"*) and `training.to6_challenge3_inputs.returns_corrected` (*"Corrected synthetic delivery … one row per return"*).
# MAGIC * Upstream: the lineage view `training.to6_challenge3_inputs.source_lineage` shows both deliveries were built from `training.to6_challenge3_inputs.parsed_source`. The view says it doesn't claim a Catalog UI check.
# MAGIC * Row meaning: one row = one tax return. `return_id` is the uniqueness identifier; `taxpayer_id` is the filer, who can have returns in several years, so it is **not** unique.
# MAGIC * Relevant fields: required `return_id`, `tax_year`, `income`, `deduction`. `taxpayer_id` is excluded from features under the need-to-know rule.
# MAGIC
# MAGIC **Full-batch observations (2022–2024)**
# MAGIC * `returns_initial`: 13 rows but only 12 distinct `return_id`s. `R01` appears twice in 2022, and `R02` (2022) has a null `income`. Zero-income returns: `R03` (2022) and `R09` (2024). No negative amounts.
# MAGIC * `returns_corrected`: 12 rows, 12 distinct IDs, 4 per year, no missing required values, and the same two zero-income returns.
# MAGIC
# MAGIC **How these findings affect the next steps**
# MAGIC * The duplicate and the null show the initial batch must fail the uniqueness and completeness checks in section 2. Those records must be kept as rejection evidence, not dropped or filled with zero.
# MAGIC * Zero income is valid, so the ratio must be null for those returns, not a divide-by-zero error, and the rows must stay in the output.
# MAGIC * Duplicates are checked across the whole batch rather than per year, because one `return_id` could appear in two years.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Decide which batch is usable
# MAGIC Keep the initial rejection evidence and apply the same checks to the replacement.

# COMMAND ----------

# DBTITLE 1,2a. Completeness and uniqueness checks on returns_initial
# 2a. Completeness and uniqueness checks on returns_initial
from pyspark.sql import functions as F

REQUIRED = ["return_id", "tax_year", "income", "deduction"]
initial_df = spark.table(INITIAL_TABLE)

# Check 1: completeness — no required field may be null
null_cond = F.expr(" OR ".join(f"{c} IS NULL" for c in REQUIRED))
missing_df = (
    initial_df
    .filter(null_cond)
    .withColumn("rejection_reason", F.lit("missing_required_field"))
)

# Check 2: uniqueness — each return_id must appear exactly once
dup_ids = (
    initial_df
    .groupBy("return_id").count()
    .filter(F.col("count") > 1)
    .select("return_id")
)
dup_df = (
    initial_df
    .join(dup_ids, on="return_id", how="inner")
    .withColumn("rejection_reason", F.lit("duplicate_return_id"))
)

# All affected rows kept as rejection evidence
rejected_initial_df = missing_df.unionByName(dup_df)

n_missing = missing_df.count()
n_dup = dup_df.count()
print(f"Completeness failures : {n_missing} row(s)")
print(f"Uniqueness failures   : {n_dup} row(s)")
print("\u2192 returns_initial REJECTED: fails both completeness and uniqueness checks.")
display(rejected_initial_df.orderBy("return_id"))

# COMMAND ----------

# DBTITLE 1,2b. Export rejected records from returns_initial
# 2b. Export rejected records as evidence; kept even after replacement batch succeeds
import os

os.makedirs(str(repo_root / "outputs"), exist_ok=True)
rejected_pdf = rejected_initial_df.toPandas()
out_path = str(repo_root / "outputs" / "initial_rejected.csv")
rejected_pdf.to_csv(out_path, index=False)
print(f"Saved {len(rejected_pdf)} rows \u2192 {out_path}")
print(rejected_pdf.to_string(index=False))

# COMMAND ----------

# DBTITLE 1,2c. Apply same checks to returns_corrected
# 2c. Apply the same completeness and uniqueness checks to returns_corrected
corrected_df = spark.table(CORRECTED_TABLE)

missing_corr = corrected_df.filter(null_cond)
dup_ids_corr = (
    corrected_df
    .groupBy("return_id").count()
    .filter(F.col("count") > 1)
)

n_missing_corr = missing_corr.count()
n_dup_corr = dup_ids_corr.count()
print(f"Completeness failures : {n_missing_corr}")
print(f"Uniqueness failures   : {n_dup_corr}")

assert n_missing_corr == 0, "returns_corrected: missing required values"
assert n_dup_corr == 0, "returns_corrected: duplicate return_ids"
print("\u2192 returns_corrected PASSES all checks. Using as source for feature output.")
SOURCE_TABLE = CORRECTED_TABLE

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deliver a checked feature table
# MAGIC Implement the task's data contract and verify the saved result by reading it back.

# COMMAND ----------

# DBTITLE 1,3a. Build and save return_features
# 3a. Build deduction_to_income feature table from the accepted batch
source_df = spark.table(SOURCE_TABLE)

features_df = (
    source_df
    .withColumn(
        "deduction_to_income",
        F.when(F.col("income") == 0, F.lit(None).cast("double"))
         .otherwise(F.col("deduction").cast("double") / F.col("income").cast("double"))
    )
    .select("return_id", "tax_year", "deduction_to_income")
)

(
    features_df
    .write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{OUTPUT_SCHEMA}.return_features")
)

n = spark.table(f"{OUTPUT_SCHEMA}.return_features").count()
print(f"Saved {n} rows to {OUTPUT_SCHEMA}.return_features")
display(features_df.orderBy("tax_year", "return_id"))

# COMMAND ----------

# DBTITLE 1,3b. Independent expected-value check
# 3b. Independent expected-value check
# R04 (2022) in returns_corrected: income=400, deduction=59.
# Expected ratio derived directly from source values, without reading the saved table.
expected_id = "R04"
expected_ratio = 59 / 400  # 0.1475

saved = spark.table(f"{OUTPUT_SCHEMA}.return_features")
actual_row = saved.filter(F.col("return_id") == expected_id).first()
actual_ratio = actual_row["deduction_to_income"]
print(f"Expected {expected_id} deduction_to_income : {expected_ratio}")
print(f"Actual   {expected_id} deduction_to_income : {actual_ratio}")
assert abs(actual_ratio - expected_ratio) < 1e-9, f"Ratio mismatch: got {actual_ratio}"
print("\u2713 Nonzero expected-value check passed.")

# COMMAND ----------

# DBTITLE 1,3c. Readback: row count, zero-income null ratio, export CSV
# 3c. Read the saved table back; verify count and zero-income rows have null ratio
saved = spark.table(f"{OUTPUT_SCHEMA}.return_features")

row_count = saved.count()
null_ratio_count = saved.filter(F.col("deduction_to_income").isNull()).count()
print(f"Row count              : {row_count}  (expected 12)")
print(f"Null-ratio rows        : {null_ratio_count}  (expected 2 \u2014 R03 2022, R09 2024)")

assert row_count == 12, f"Expected 12 rows, got {row_count}"
assert null_ratio_count == 2, f"Expected 2 null-ratio rows, got {null_ratio_count}"

print("\nZero-income rows (deduction_to_income must be null):")
display(saved.filter(F.col("deduction_to_income").isNull()).orderBy("return_id"))

print("\nFull feature table:")
display(saved.orderBy("tax_year", "return_id"))

# Export for outputs/
out_csv = str(repo_root / "outputs" / "return_features.csv")
saved.orderBy("tax_year", "return_id").toPandas().to_csv(out_csv, index=False)
print(f"\nExported \u2192 {out_csv}")
print("\u2713 Saved-table readback passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Make the handoff usable
# MAGIC Save the required year tables. Keep references and IDs-and-values evidence for two successful runs.
# MAGIC Write SUBMISSION.md and the small exports inside this Databricks Git folder.

# COMMAND ----------

# DBTITLE 1,4a. Save split tables and verify year assignment
# 4a. Save features_train (2022), features_validation (2023), features_test (2024)
saved_features = spark.table(f"{OUTPUT_SCHEMA}.return_features")

splits = [("features_train", 2022), ("features_validation", 2023), ("features_test", 2024)]
for table_name, year in splits:
    split_df = saved_features.filter(F.col("tax_year") == year)
    (split_df
        .write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{OUTPUT_SCHEMA}.{table_name}"))
    n = spark.table(f"{OUTPUT_SCHEMA}.{table_name}").count()
    # Year assignment check: every row in this table must carry the correct tax_year
    wrong = spark.table(f"{OUTPUT_SCHEMA}.{table_name}").filter(F.col("tax_year") != year).count()
    status = "\u2713" if wrong == 0 else f"FAIL {wrong} wrong-year rows"
    print(f"{table_name}: {n} rows, year={year} {status}")

# Coverage: every accepted return in exactly one split table, none missing
all_splits = (
    spark.table(f"{OUTPUT_SCHEMA}.features_train")
    .unionByName(spark.table(f"{OUTPUT_SCHEMA}.features_validation"))
    .unionByName(spark.table(f"{OUTPUT_SCHEMA}.features_test"))
)
total = all_splits.count()
distinct = all_splits.select("return_id").distinct().count()
assert total == 12, f"Expected 12 rows across splits, got {total}"
assert distinct == 12, f"Expected 12 distinct IDs, got {distinct}"
print(f"\nTotal rows across splits: {total} | Distinct return_ids: {distinct}")
print("\u2713 Every accepted return appears in exactly one split table.")
display(all_splits.orderBy("tax_year", "return_id"))

# COMMAND ----------

# DBTITLE 1,4b. Export run-1 CSVs and capture run-1 snapshot
# 4b. Export run-1 split CSVs and capture a run-1 snapshot of return_features
import pandas as pd

for table_name, fname in [
    (f"{OUTPUT_SCHEMA}.features_train",      "features_train.csv"),
    (f"{OUTPUT_SCHEMA}.features_validation", "features_validation.csv"),
    (f"{OUTPUT_SCHEMA}.features_test",       "features_test.csv"),
]:
    out = str(repo_root / "outputs" / fname)
    spark.table(table_name).orderBy("return_id").toPandas().to_csv(out, index=False)
    print(f"Exported \u2192 {out}")

# Full return_features snapshot: return_id, tax_year, deduction_to_income for every accepted return
run1_pdf = (
    spark.table(f"{OUTPUT_SCHEMA}.return_features")
    .orderBy("return_id", "tax_year").toPandas()
)
run1_path = str(repo_root / "outputs" / "run1_features.csv")
run1_pdf.to_csv(run1_path, index=False)
print(f"\nRun-1 snapshot ({len(run1_pdf)} rows) \u2192 {run1_path}")
print(run1_pdf.to_string(index=False))

# COMMAND ----------

# DBTITLE 1,4c. Run 2: re-execute pipeline and compare with run-1
# 4c. Run 2 — re-execute from quality checks through split saves; verify idempotency
print("=== RUN 2 ===")

# Quality checks (must still pass on returns_corrected)
assert spark.table(CORRECTED_TABLE).filter(null_cond).count() == 0, "run2: missing required values"
assert (
    spark.table(CORRECTED_TABLE)
    .groupBy("return_id").count()
    .filter(F.col("count") > 1)
    .count()
) == 0, "run2: duplicate return_ids"
print("Quality checks: passed.")

# Re-save return_features (overwrite)
(spark.table(CORRECTED_TABLE)
    .withColumn(
        "deduction_to_income",
        F.when(F.col("income") == 0, F.lit(None).cast("double"))
         .otherwise(F.col("deduction").cast("double") / F.col("income").cast("double"))
    )
    .select("return_id", "tax_year", "deduction_to_income")
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(f"{OUTPUT_SCHEMA}.return_features")
)

# Re-save split tables (overwrite)
feats2 = spark.table(f"{OUTPUT_SCHEMA}.return_features")
for name, yr in [("features_train", 2022), ("features_validation", 2023), ("features_test", 2024)]:
    (feats2.filter(F.col("tax_year") == yr)
        .write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{OUTPUT_SCHEMA}.{name}"))
print("All tables re-saved (overwrite).")

# Capture run-2 snapshot
run2_pdf = (
    spark.table(f"{OUTPUT_SCHEMA}.return_features")
    .orderBy("return_id", "tax_year").toPandas()
)
run2_path = str(repo_root / "outputs" / "run2_features.csv")
run2_pdf.to_csv(run2_path, index=False)
print(f"Run-2 snapshot ({len(run2_pdf)} rows) \u2192 {run2_path}")

# Compare: same return_ids, same tax_year, same deduction_to_income (NaN == NaN via .equals)
match = run1_pdf.reset_index(drop=True).equals(run2_pdf.reset_index(drop=True))
print(f"\nRun-1 vs Run-2 return_features identical: {match}")
assert match, "Runs differ — tables were not properly overwritten"
print(f"\u2713 Both runs: {len(run1_pdf)} rows with identical return IDs and deduction_to_income values.")
print("\nRun-2 snapshot:")
print(run2_pdf.to_string(index=False))