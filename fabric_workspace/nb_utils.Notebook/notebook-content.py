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

# # nb_utils — shared incremental-engine helpers
# 
# `%run` this notebook from every layer notebook. It centralises the patterns the
# SNDL medallion is built on so each layer stays small and the engineering is
# **metadata-driven**, not copy-pasted:
# 
# * `read_watermark` / `update_watermark` — the `control_watermark` high-water mark
# * `log_audit` — per-run row counts & status into `control_load_audit`
# * `get_table_metadata` — the table registry (business keys, type, SCD2 flag, tracked attrs)
# * `merge_upsert` — idempotent Delta `MERGE` on business keys (facts + non-SCD2 dims)
# * `apply_scd2` — Change-Data-Feed-driven SCD2 on dimensions
# * `read_cdf` — read only changed rows from an upstream Delta table (`readChangeFeed`)
# * `dq_split` — soft-fail data-quality: returns (clean, quarantine)
# 
# **No hardcoded workspace names.** One schema-enabled lakehouse is the default for
# every layer notebook; the medallion layers are SCHEMAS — `bronze.<t>`, `silver.<t>`,
# `gold.dim_*/fact_*`, `control.*`, `landing.<t>` — so layer separation needs no
# separate lakehouses or name prefixes. Deployment rules rebind the one lakehouse per stage.

# CELL ********************

from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# These are injected by the caller's parameter cell (pipeline-bound per stage).
# Defaults keep the notebook runnable interactively against the attached lakehouse.
try:
    stage
except NameError:
    stage = "dev"
try:
    control_path  # where control tables live (a path or relative 'Tables')
except NameError:
    control_path = "Tables"

# Single schema-enabled lakehouse: medallion layers are SCHEMAS (bronze/silver/gold),
# not separate lakehouses or name prefixes. Control tables live in the `control` schema.
CTL_WATERMARK = "control.watermark"
CTL_METADATA = "control.table_metadata"
CTL_AUDIT = "control.load_audit"

def _tpath(base: str, name: str) -> str:
    """Join a base ('Tables' or an abfss path) with a table name."""
    return f"{base}/{name}" if base.startswith("abfss://") else name

# MARKDOWN ********************

# ## Control: watermark + audit

# CELL ********************

def read_watermark(table_name: str, default: str = "1900-01-01") -> str:
    """Return the last successfully-processed date for a table (idempotency anchor)."""
    try:
        df = spark.read.table(CTL_WATERMARK).where(F.col("table_name") == table_name)
        row = df.select("last_processing_date").head()
        return str(row[0]) if row and row[0] is not None else default
    except Exception:
        return default

def update_watermark(table_name: str, processing_date: str, rows_affected: int) -> None:
    """Advance the high-water mark for a table after a successful load (MERGE = idempotent)."""
    wm = spark.createDataFrame(
        [(table_name, processing_date, datetime.utcnow(), int(rows_affected))],
        "table_name string, last_processing_date string, last_run_timestamp timestamp, last_rows_affected long",
    )
    tgt = DeltaTable.forName(spark, CTL_WATERMARK)
    (tgt.alias("t").merge(wm.alias("s"), "t.table_name = s.table_name")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

def log_audit(notebook: str, table_name: str, layer: str, rows: int, status: str, message: str = "") -> None:
    rec = spark.createDataFrame(
        [(datetime.utcnow(), notebook, layer, table_name, int(rows), status, message)],
        "run_ts timestamp, notebook string, layer string, table_name string, rows_affected long, status string, message string",
    )
    rec.write.format("delta").mode("append").saveAsTable(CTL_AUDIT)

def get_table_metadata(only_type: str = None):
    """Return the table registry as a list of dicts (drives the metadata loop)."""
    df = spark.read.table(CTL_METADATA)
    if only_type:
        df = df.where(F.col("table_type") == only_type)
    return [r.asDict() for r in df.orderBy("load_order").collect()]

# MARKDOWN ********************

# ## Delta: CDF, MERGE upsert, SCD2

# CELL ********************

def enable_cdf(table_name: str) -> None:
    """Turn on Change Data Feed so the next layer can read only what changed."""
    spark.sql(f"ALTER TABLE {table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

def read_cdf(table_name: str, starting_version: int = None, starting_timestamp: str = None) -> DataFrame:
    """Read only changed rows from an upstream Delta table via Change Data Feed.

    Falls back to a full read if CDF isn't available yet (first run / table just created).
    """
    reader = spark.read.format("delta").option("readChangeFeed", "true")
    try:
        if starting_version is not None:
            reader = reader.option("startingVersion", starting_version)
        elif starting_timestamp is not None:
            reader = reader.option("startingTimestamp", starting_timestamp)
        else:
            reader = reader.option("startingVersion", 0)
        df = reader.table(table_name)
        # keep the latest image of inserts/updates; drop the pre-image of updates
        df = df.where(F.col("_change_type").isin("insert", "update_postimage"))
        # Force evaluation HERE so a first-run "change data not recorded for version 0"
        # error is caught by this except (Spark is lazy; otherwise it surfaces downstream
        # and crashes the caller instead of falling back to a full read).
        df.take(1)
        return df
    except Exception as e:
        print(f"  [read_cdf] CDF unavailable for {table_name} ({e}); full read.")
        return spark.read.table(table_name)

def current_version(table_name: str) -> int:
    try:
        return DeltaTable.forName(spark, table_name).history(1).head()["version"]
    except Exception:
        return 0

# CELL ********************

def merge_upsert(target_table: str, source_df: DataFrame, business_keys, update_cols=None) -> dict:
    """Idempotent Delta MERGE on business key(s). Creates the table on first run.

    Returns the operation metrics (inserted / updated). Used for facts and
    non-SCD2 dimensions. Re-running the same source is a no-op (safe retries).
    """
    bks = [business_keys] if isinstance(business_keys, str) else list(business_keys)
    if not spark.catalog.tableExists(target_table):
        source_df.write.format("delta").saveAsTable(target_table)
        enable_cdf(target_table)
        return {"inserted": source_df.count(), "updated": 0}

    cond = " AND ".join([f"t.{k} = s.{k}" for k in bks])
    tgt = DeltaTable.forName(spark, target_table)
    m = tgt.alias("t").merge(source_df.alias("s"), cond)
    if update_cols:
        m = m.whenMatchedUpdate(set={c: f"s.{c}" for c in update_cols})
    else:
        m = m.whenMatchedUpdateAll()
    m.whenNotMatchedInsertAll().execute()

    metrics = tgt.history(1).head()["operationMetrics"]
    return {
        "inserted": int(metrics.get("numTargetRowsInserted", 0)),
        "updated": int(metrics.get("numTargetRowsUpdated", 0)),
    }

# CELL ********************

def apply_scd2(target_table: str, source_df: DataFrame, business_keys, track_attrs, effective_date: str) -> dict:
    """Type-2 slowly-changing dimension via the two-step Delta MERGE pattern.

    Adds SCD2 housekeeping columns: scd_hash, effective_from, effective_to,
    is_current. When a tracked attribute changes for an existing business key, the
    current row is closed (effective_to set, is_current=false) and a new current
    row is inserted. New keys are inserted as current. Driven by CDF input.
    """
    bks = [business_keys] if isinstance(business_keys, str) else list(business_keys)
    attrs = list(track_attrs) if track_attrs else [c for c in source_df.columns if c not in bks]

    src = (source_df
           .withColumn("scd_hash", F.sha2(F.concat_ws("||", *[F.coalesce(F.col(a).cast("string"), F.lit("∅")) for a in attrs]), 256))
           .withColumn("effective_from", F.lit(effective_date).cast("date"))
           .withColumn("effective_to", F.lit("9999-12-31").cast("date"))
           .withColumn("is_current", F.lit(True)))

    if not spark.catalog.tableExists(target_table):
        src.write.format("delta").saveAsTable(target_table)
        enable_cdf(target_table)
        return {"inserted": src.count(), "updated": 0, "closed": 0}

    tgt = DeltaTable.forName(spark, target_table)
    key_cond = " AND ".join([f"t.{k} = s.{k}" for k in bks])

    # Step 1: rows whose hash changed -> stage them with a null merge key so the
    # MERGE inserts the NEW version while a second pass closes the OLD version.
    changed = (src.alias("s").join(
        tgt.toDF().where("is_current = true").alias("t"),
        on=[F.col(f"s.{k}") == F.col(f"t.{k}") for k in bks], how="inner")
        .where("s.scd_hash <> t.scd_hash").select("s.*"))

    # close out superseded current rows
    (tgt.alias("t").merge(
        changed.alias("s"), key_cond + " AND t.is_current = true")
        .whenMatchedUpdate(set={
            "is_current": F.lit(False),
            "effective_to": F.lit(effective_date).cast("date"),
        }).execute())
    closed = tgt.history(1).head()["operationMetrics"].get("numTargetRowsUpdated", 0)

    # insert brand-new keys + new versions of changed keys
    existing_current = tgt.toDF().where("is_current = true").select(*bks, "scd_hash")
    to_insert = (src.alias("s").join(existing_current.alias("t"),
                 on=[F.col(f"s.{k}") == F.col(f"t.{k}") for k in bks], how="left")
                 .where("t.scd_hash IS NULL OR s.scd_hash <> t.scd_hash").select("s.*"))
    to_insert.write.format("delta").mode("append").saveAsTable(target_table)
    inserted = to_insert.count()
    return {"inserted": inserted, "updated": 0, "closed": int(closed)}

# MARKDOWN ********************

# ## Data quality: soft-fail split (clean vs quarantine)

# CELL ********************

def dq_split(df: DataFrame, rules: dict):
    """Split a dataframe into (clean, quarantine) by a dict of {reason: condition_expr}.

    A row failing ANY rule is quarantined with a `dq_reasons` column listing the
    rules it broke — feeding the Silver quarantine table for review (catches the
    intentional dirty data: bad timestamps, qty=0, malformed postal codes, cost>list).
    """
    reason = F.array()
    for name, cond in rules.items():
        reason = F.when(F.expr(cond), F.array_union(reason, F.array(F.lit(name)))).otherwise(reason)
    tagged = df.withColumn("dq_reasons", reason)
    clean = tagged.where(F.size("dq_reasons") == 0).drop("dq_reasons")
    quarantine = tagged.where(F.size("dq_reasons") > 0)
    return clean, quarantine

print("nb_utils loaded: read/update_watermark, log_audit, get_table_metadata, "
      "enable_cdf, read_cdf, merge_upsert, apply_scd2, dq_split")
