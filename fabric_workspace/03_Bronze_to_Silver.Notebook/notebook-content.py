# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9a853918-e7e4-48c6-9d75-02661432c95a",
# META       "default_lakehouse_name": "LH_SNDL",
# META       "default_lakehouse_workspace_id": "d46401cf-3418-406f-81b0-7912336d9363",
# META       "known_lakehouses": [
# META         {
# META           "id": "9a853918-e7e4-48c6-9d75-02661432c95a"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # 03 · Bronze → Silver  (CDF-driven · DQ quarantine · SCD2)
# 
# The cleansing + historization layer:
# 
# * **Reads only changed rows** from Bronze via **Change Data Feed** (`read_cdf`).
# * **Soft-fail data quality** — bad rows go to `silver_quarantine_<table>` with a
#   `dq_reasons` list (catches the planted dirty data: bad timestamps, qty=0,
#   malformed postal codes, cost>list); clean rows continue.
# * **SCD2** on `dim_store` / `dim_product` / `dim_customer` — the planted
#   `dim_changes` (tier upgrades, price changes, store edits) produce new versions
#   while the superseded rows are closed (`is_current=false`, `effective_to` set).
# * **MERGE upsert** for facts and non-SCD2 dims.
# * Enables CDF on every Silver table for layer 04.

# CELL ********************

# PARAMETERS CELL
stage = "dev"
# SCD2 effective-from for the INITIAL bulk load — must predate ALL facts (sales start
# 2024-01-01), so every historical fact matches its version-1 dimension row in layer 04.
# Daily-increment runs override this with the actual change date (via the orchestrator).
processing_date = "1900-01-01"
control_path = "Tables"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run nb_utils

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Data-quality rules per table: {reason: condition that means BAD}
DQ_RULES = {
    "fact_sales_transaction": {"bad_timestamp": "to_timestamp(transaction_timestamp) IS NULL"},
    "fact_sales_line_item":   {"zero_quantity": "quantity = 0 OR quantity IS NULL"},
    "dim_customer":           {"malformed_postal":
        "home_postal_code IS NULL OR NOT (home_postal_code RLIKE '^[A-Za-z][0-9][A-Za-z] ?[0-9][A-Za-z][0-9]$')"},
    "dim_product":            {"cost_gt_list": "cost_price_cad > list_price_cad"},
}

def cleanse(df, name):
    """Light, generic cleansing: trim strings; drop landing/bronze housekeeping cols."""
    drop = [c for c in ["_landed_at", "_bronze_loaded_at", "_rn"] if c in df.columns]
    df = df.drop(*drop)
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.trim(F.col(c)))
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Facts + non-SCD2 dims: CDF → cleanse → DQ split → MERGE

# CELL ********************

for meta in get_table_metadata():
    name = meta["table_name"]
    bronze_tbl, silver_tbl = f"bronze.{name}", f"silver.{name}"
    if meta["is_scd2"]:
        continue  # SCD2 dims handled separately below
    if not spark.catalog.tableExists(bronze_tbl):
        continue

    changes = read_cdf(bronze_tbl).transform(lambda d: cleanse(d, name))
    # drop CDF metadata columns before persisting
    changes = changes.drop("_change_type", "_commit_version", "_commit_timestamp")
    if changes.rdd.isEmpty():
        print(f"· {name}: no Bronze changes"); continue

    clean, quarantine = (changes, None)
    if name in DQ_RULES:
        clean, quarantine = dq_split(changes, DQ_RULES[name])
        if quarantine is not None and not quarantine.rdd.isEmpty():
            (quarantine.withColumn("_quarantined_at", F.current_timestamp())
             .write.format("delta").mode("append").option("mergeSchema", "true")
             .saveAsTable(f"silver.quarantine_{name}"))
            log_audit("03_Bronze_to_Silver", f"silver.quarantine_{name}", "silver",
                      quarantine.count(), "QUARANTINE", str(list(DQ_RULES[name])))

    metrics = merge_upsert(silver_tbl, clean, meta["business_keys"])
    enable_cdf(silver_tbl)
    log_audit("03_Bronze_to_Silver", silver_tbl, "silver",
              metrics["inserted"] + metrics["updated"], "OK", "fact/non-scd2 dim")
    print(f"· {name}: clean +{metrics['inserted']}/{metrics['updated']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## SCD2 dims: overlay planted dim_changes → apply_scd2

# CELL ********************

def apply_planted_changes(dim_df, table_name):
    """Overlay any landed `dim_changes` for this dimension onto the current image.

    Each change row is (table_name, business_key, attribute, old_value, new_value).
    We set attribute = new_value where the business key matches, producing the new
    image that `apply_scd2` will detect (hash change) and version.
    """
    if not spark.catalog.tableExists("landing.dim_changes"):
        return dim_df
    chg = spark.read.table("landing.dim_changes").where(F.col("table_name") == table_name)
    if chg.rdd.isEmpty():
        return dim_df
    bk_col = {"dim_store": "store_number", "dim_product": "sku",
              "dim_customer": "customer_business_key"}[table_name]
    rows = chg.select("business_key", "attribute", "new_value").collect()
    out = dim_df
    for r in rows:
        out = out.withColumn(
            r["attribute"],
            F.when(F.col(bk_col) == r["business_key"],
                   F.lit(r["new_value"]).cast(dict(out.dtypes)[r["attribute"]]))
             .otherwise(F.col(r["attribute"])))
    print(f"   {table_name}: overlaid {len(rows)} planted change(s)")
    return out

for meta in get_table_metadata(only_type="dim"):
    if not meta["is_scd2"]:
        continue
    name = meta["table_name"]
    bronze_tbl, silver_tbl = f"bronze.{name}", f"silver.{name}"
    if not spark.catalog.tableExists(bronze_tbl):
        continue

    # current full image from Bronze (SCD2 needs the whole row to hash-compare)
    img = cleanse(spark.read.table(bronze_tbl), name)
    img = apply_planted_changes(img, name)

    # DQ on dims that have rules (e.g. dim_product cost>list, dim_customer postal)
    if name in DQ_RULES:
        img, q = dq_split(img, DQ_RULES[name])
        if q is not None and not q.rdd.isEmpty():
            (q.withColumn("_quarantined_at", F.current_timestamp())
             .write.format("delta").mode("append").option("mergeSchema", "true")
             .saveAsTable(f"silver.quarantine_{name}"))

    res = apply_scd2(silver_tbl, img, meta["business_keys"], meta["track_attrs"], processing_date)
    enable_cdf(silver_tbl)
    log_audit("03_Bronze_to_Silver", silver_tbl, "silver",
              res["inserted"] + res["closed"], "OK",
              f"scd2 ins={res['inserted']} closed={res['closed']}")
    print(f"· {name}: SCD2 ins={res['inserted']} closed={res['closed']}")

print("Bronze → Silver complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
