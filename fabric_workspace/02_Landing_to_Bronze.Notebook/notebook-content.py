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

# # 02 · Landing → Bronze  (metadata-driven, MERGE + CDF + watermark)
# 
# Loops over **every table in the registry** (no per-table copy-paste). For each:
# 1. read only **new** landing partitions (`Processing_Date > watermark`),
# 2. **`MERGE`** into the Bronze Delta table on its business key (idempotent upsert),
# 3. ensure **Change Data Feed** is on (so Silver reads only deltas),
# 4. advance the **watermark** and write an **audit** row.
# 
# Bronze = typed raw, business-key-deduped, with ingestion lineage
# (`Processing_Date`, `_bronze_loaded_at`). Re-running a date is a safe no-op.

# CELL ********************

# PARAMETERS CELL
stage = "dev"
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
from pyspark.sql.window import Window

tables = get_table_metadata()
print(f"Processing {len(tables)} tables Landing → Bronze")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for meta in tables:
    name = meta["table_name"]
    bks = meta["business_keys"]
    landing_tbl = f"landing.{name}"
    bronze_tbl = f"bronze.{name}"

    if not spark.catalog.tableExists(landing_tbl):
        print(f"· {name}: no landing table yet, skip")
        continue

    wm = read_watermark(f"bronze_{name}")
    new = spark.read.table(landing_tbl).where(F.col("Processing_Date") > wm)
    if new.rdd.isEmpty():
        print(f"· {name}: nothing new since {wm}")
        continue

    # de-dup within the batch: keep the latest landed row per business key
    w = Window.partitionBy(*bks).orderBy(F.col("_landed_at").desc())
    deduped = (new.withColumn("_rn", F.row_number().over(w))
                  .where("_rn = 1").drop("_rn")
                  .withColumn("_bronze_loaded_at", F.current_timestamp()))

    metrics = merge_upsert(bronze_tbl, deduped, bks)
    max_pd = new.agg(F.max("Processing_Date")).head()[0]
    affected = metrics["inserted"] + metrics["updated"]
    update_watermark(f"bronze_{name}", str(max_pd), affected)
    log_audit("02_Landing_to_Bronze", bronze_tbl, "bronze", affected, "OK",
              f"ins={metrics['inserted']} upd={metrics['updated']} pd≤{max_pd}")
    print(f"· {name}: +{metrics['inserted']} ins / {metrics['updated']} upd  (watermark→{max_pd})")

print("Landing → Bronze complete.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
