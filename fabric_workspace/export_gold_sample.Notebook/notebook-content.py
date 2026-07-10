# Fabric notebook source

# METADATA ********************

# META {
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

# # _export_gold — sample Gold tables to Files/export as single Parquet (for local sandbox)
# Dims exported in full; facts sampled to keep the local import model snappy.

# CELL ********************

# PARAMETERS CELL
fact_sample = 200000

# CELL ********************

DIMS = ["dim_banner", "dim_category", "dim_date", "dim_employee",
        "dim_promotion", "dim_store", "dim_product", "dim_customer"]
FACTS = ["fact_sales_transaction", "fact_sales_line_item",
         "fact_inventory_snapshot", "fact_loyalty_event"]

for d in DIMS:
    (spark.read.table(f"gold.{d}")
        .coalesce(1)
        .write.mode("overwrite").parquet(f"Files/export/{d}"))
    print(f"exported {d} (full)")

for f in FACTS:
    (spark.read.table(f"gold.{f}")
        .limit(fact_sample)
        .coalesce(1)
        .write.mode("overwrite").parquet(f"Files/export/{f}"))
    print(f"exported {f} (sample {fact_sample})")

print("EXPORT COMPLETE -> Files/export")
