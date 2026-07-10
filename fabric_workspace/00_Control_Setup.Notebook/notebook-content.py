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

# # 00 · Control Setup — watermark, metadata registry, audit
# 
# Run **once per workspace** (idempotent — safe to re-run). Creates the three
# control tables the incremental engine is driven by:
# 
# | Table | Purpose |
# |---|---|
# | `control_table_metadata` | the table registry (business keys, type, SCD2 flag, tracked attrs, load order) — *this* is what makes the loop metadata-driven |
# | `control_watermark` | last `Processing_Date` successfully loaded per table → idempotent re-runs |
# | `control_load_audit` | per-run row counts + status for observability |
# 
# Attach the **single schema-enabled lakehouse** (`LH_SNDL`) as the default lakehouse
# for this notebook. The medallion layers are SCHEMAS; this notebook creates them and
# seeds the `control.*` tables. (Deployment rules rebind the lakehouse per stage — no
# workspace name in code.)

# CELL ********************

# PARAMETERS CELL (values supplied by the pipeline; defaults for interactive runs)
stage = "dev"
control_path = "Tables"
low_watermark = "1900-01-01"

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

# MARKDOWN ********************

# ## 0. Create the medallion schemas (idempotent)
# One schema-enabled lakehouse; each medallion layer is a schema.

# CELL ********************

for _schema in ["landing", "bronze", "silver", "gold", "control"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_schema}")
print("schemas ready: landing, bronze, silver, gold, control")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Table registry — the metadata that drives everything downstream
# Kept in code here for transparency; in a larger shop this would be loaded from
# `config/config.json` in Files. Order matters: dims before facts (FK resolution).

# CELL ********************

REGISTRY = [
    # name, type, business_keys, scd2, track_attrs, load_order
    ("dim_banner",              "dim",  ["banner_id"],                 False, [], 1),
    ("dim_category",            "dim",  ["category_id"],               False, [], 2),
    ("dim_date",                "dim",  ["date_id"],                   False, [], 3),
    ("dim_employee",            "dim",  ["employee_business_key"],     False, [], 4),
    ("dim_promotion",           "dim",  ["promotion_id"],              False, [], 5),
    ("dim_store",               "dim",  ["store_number"],              True,
        ["store_name", "square_footage", "store_type", "manager_employee_id", "district", "region", "close_date"], 6),
    ("dim_product",             "dim",  ["sku"],                       True,
        ["product_name", "list_price_cad", "cost_price_cad", "price_tier", "discontinue_date", "applicable_banner_ids"], 7),
    ("dim_customer",            "dim",  ["customer_business_key"],     True,
        ["loyalty_tier", "loyalty_points_balance", "home_postal_code", "home_city", "marketing_consent", "is_active"], 8),
    ("fact_sales_transaction",  "fact", ["transaction_business_key"],  False, [], 20),
    ("fact_sales_line_item",    "fact", ["line_item_id"],              False, [], 21),
    ("fact_inventory_snapshot", "fact", ["snapshot_id"],               False, [], 22),
    ("fact_loyalty_event",      "fact", ["loyalty_event_id"],          False, [], 23),
]

from pyspark.sql import functions as F

meta_df = spark.createDataFrame(
    [(n, t, bk, scd, attrs, order) for (n, t, bk, scd, attrs, order) in REGISTRY],
    "table_name string, table_type string, business_keys array<string>, is_scd2 boolean, "
    "track_attrs array<string>, load_order int",
)
meta_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("control.table_metadata")
print(f"control.table_metadata seeded with {meta_df.count()} tables")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Watermark — seed every table at the low-water mark (first run loads all history)

# CELL ********************

wm_df = spark.createDataFrame(
    [(n, low_watermark, None, 0) for (n, *_rest) in REGISTRY],
    "table_name string, last_processing_date string, last_run_timestamp timestamp, last_rows_affected long",
)
# create-if-missing only; never clobber a real watermark on re-run
if not spark.catalog.tableExists("control.watermark"):
    wm_df.write.format("delta").saveAsTable("control.watermark")
    print("control.watermark created + seeded")
else:
    existing = {r.table_name for r in spark.read.table("control.watermark").select("table_name").collect()}
    new = wm_df.where(~F.col("table_name").isin(list(existing)))
    if new.count():
        new.write.format("delta").mode("append").saveAsTable("control.watermark")
    print(f"control.watermark exists; added {new.count()} new table(s)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Audit table (create if missing)

# CELL ********************

if not spark.catalog.tableExists("control.load_audit"):
    spark.createDataFrame(
        [], "run_ts timestamp, notebook string, layer string, table_name string, "
            "rows_affected long, status string, message string"
    ).write.format("delta").saveAsTable("control.load_audit")
    print("control.load_audit created")

display(spark.read.table("control.table_metadata").orderBy("load_order"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
