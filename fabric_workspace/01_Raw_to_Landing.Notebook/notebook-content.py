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

# # 01 · Raw → Landing
# 
# Lands source files into Delta staging tables (`landing_<table>`), **stamping
# `Processing_Date` at ingestion** (it is *not* a column in the generated source —
# it's derived here, exactly per the spec). Two modes:
# 
# * **`bulk`** — one-pass historical backfill (everything ≤ 2025-12-21), stamped with a
#   single backfill `Processing_Date`.
# * **`daily`** — one folder from `08_daily_increments/<date>/`, stamped with that date.
# 
# Anti-patterns from the tutorial that are fixed here: no hardcoded `today_file`,
# `processed_Date`, storage-account name, or workspace — everything is a
# **pipeline parameter** and the *daily* date is chosen from the watermark.

# CELL ********************

# PARAMETERS CELL
stage = "dev"
mode = "bulk"                        # "bulk" | "daily"  (set to bulk for the initial backfill)
processing_date = "2025-12-21"       # bulk: the backfill stamp; daily: the folder date
source_files_path = "Files/source"   # bound per stage by the pipeline
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

# Which tables exist in each mode of the source layout.
DAILY_TABLES = ["fact_sales_transaction", "fact_sales_line_item", "fact_loyalty_event"]
BULK_TABLES = [m["table_name"] for m in get_table_metadata()]   # all tables

def _read_source(rel: str):
    """Read a source dataset (prefer Parquet; fall back to CSV)."""
    base = f"{source_files_path}/{rel}"
    try:
        return spark.read.parquet(base)
    except Exception:
        return spark.read.option("header", True).option("inferSchema", True).csv(base + ".csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Land each table, partitioned by Processing_Date

# CELL ********************

def land(table_name: str, source_rel: str, proc_date: str):
    df = _read_source(source_rel)
    if df.rdd.isEmpty():
        print(f"  {table_name}: source empty, skipped")
        return 0
    out = (df.withColumn("Processing_Date", F.lit(proc_date))
             .withColumn("_landed_at", F.current_timestamp()))
    (out.write.format("delta").mode("append")
        .partitionBy("Processing_Date")
        .option("mergeSchema", "true")
        .saveAsTable(f"landing.{table_name}"))
    n = out.count()
    log_audit("01_Raw_to_Landing", table_name, "landing", n, "OK", f"mode={mode} pd={proc_date}")
    print(f"  {table_name}: landed {n} rows @ Processing_Date={proc_date}")
    return n

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if mode == "bulk":
    # bulk backfill: dims + facts from the bulk export (04_parquet partitions)
    for t in BULK_TABLES:
        land(t, f"bulk/{t}", processing_date)
elif mode == "daily":
    # one day's increment folder: facts + the planted SCD2 dim_changes
    for t in DAILY_TABLES:
        land(t, f"daily/{processing_date}/{t}", processing_date)
    # dim_changes (SCD2 source) lands into a dedicated table for layer 03
    try:
        dc = _read_source(f"daily/{processing_date}/dim_changes")
        if not dc.rdd.isEmpty():
            (dc.withColumn("Processing_Date", F.lit(processing_date))
               .write.format("delta").mode("append").partitionBy("Processing_Date")
               .option("mergeSchema", "true").saveAsTable("landing.dim_changes"))
            print(f"  dim_changes: landed {dc.count()} SCD2 change(s)")
    except Exception as e:
        print(f"  dim_changes: none for {processing_date} ({e})")
else:
    raise ValueError(f"unknown mode {mode!r}")

print(f"Raw→Landing complete (mode={mode}, processing_date={processing_date})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
