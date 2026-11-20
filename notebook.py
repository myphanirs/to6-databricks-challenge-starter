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

# Your checks and decision here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Deliver a checked feature table
# MAGIC Implement the task's data contract and verify the saved result by reading it back.

# COMMAND ----------

# Your implementation and verification here.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Make the handoff usable
# MAGIC Save the required year tables. Keep references and IDs-and-values evidence for two successful runs.
# MAGIC Write SUBMISSION.md and the small exports inside this Databricks Git folder.

# COMMAND ----------

# Your implementation, rerun comparison and exports here.