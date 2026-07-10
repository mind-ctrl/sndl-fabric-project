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

# # 04 · Silver → Gold  (SCD2-aware star schema)
# 
# Builds the consumption-ready **star schema** the Direct Lake model sits on:
# 
# * **Dimensions** get a stable surrogate key `<dim>_sk`:
#   * non-SCD2 dims → one row per natural key;
#   * SCD2 dims (`dim_store`/`dim_product`/`dim_customer`) → **one row per version**,
#     `sk = xxhash64(business_key || effective_from)` (deterministic ⇒ idempotent MERGE).
# * **Facts** resolve each SCD2 dimension to the version **effective at the fact date**
#   (range join `effective_from ≤ date < effective_to`), so a sale before a price
#   change keeps its old price — true Type-2 reporting, not Type-1 overwrite.
# * Reads Silver via **CDF** where possible; **MERGE** into Gold (idempotent).
# 
# Gold is written as Delta here (its SQL endpoint = the Gold Warehouse for Power BI).

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

# allow MERGE to add new Gold columns (e.g. enrollment_month) to existing tables
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# natural id carried on facts -> (silver dim table, business key)
SCD2_DIMS = {
    "store":    ("silver.dim_store",    "store_number",            "store_id"),
    "product":  ("silver.dim_product",  "sku",                     "product_id"),
    "customer": ("silver.dim_customer", "customer_business_key",   "customer_id"),
}

def _sk_expr(*cols):
    return F.xxhash64(F.concat_ws("||", *[F.col(c).cast("string") for c in cols]))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Gold dimensions (surrogate keys)

# CELL ********************

# --- non-SCD2 dims: sk = natural key ---
NON_SCD2 = {
    "dim_banner": "banner_id", "dim_category": "category_id", "dim_date": "date_id",
    "dim_employee": "employee_id", "dim_promotion": "promotion_id",
}
for name, idcol in NON_SCD2.items():
    s = f"silver.{name}"
    if not spark.catalog.tableExists(s):
        continue
    g = spark.read.table(s).withColumn(name.replace("dim_", "") + "_sk", F.col(idcol))
    merge_upsert(f"gold.{name}", g, name.replace("dim_", "") + "_sk")
    print(f"· {name}: {g.count()} rows")

# --- SCD2 dims: sk per version ---
for short, (silver_tbl, bk, idcol) in SCD2_DIMS.items():
    if not spark.catalog.tableExists(silver_tbl):
        continue
    g = (spark.read.table(silver_tbl)
         .withColumn(f"{short}_sk", _sk_expr(bk, "effective_from")))
    if short == "customer" and "enrollment_date" in g.columns:
        # cohort axis pushed down to Gold (Direct Lake can't host calc columns)
        g = g.withColumn("enrollment_month", F.trunc(F.col("enrollment_date"), "month"))
    merge_upsert(f"gold.dim_{short}", g, f"{short}_sk")
    print(f"· dim_{short}: {g.count()} version-rows (current = "
          f"{g.where('is_current').count()})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Gold facts — resolve SCD2 surrogate keys by effective-at-date

# CELL ********************

def resolve_scd2_sk(fact, date_col, short, idcol):
    """Add <short>_sk to a fact by joining the dimension version effective at date_col."""
    dim_tbl = f"gold.dim_{short}"
    if not spark.catalog.tableExists(dim_tbl):
        return fact
    dim = (spark.read.table(dim_tbl)
           .select(F.col(f"{short}_sk"),
                   F.col(idcol).alias("_did"),
                   F.date_format("effective_from", "yyyyMMdd").cast("int").alias("_ef"),
                   F.date_format("effective_to", "yyyyMMdd").cast("int").alias("_et")))
    joined = fact.join(
        dim,
        (fact[idcol] == dim["_did"]) & (fact[date_col] >= dim["_ef"]) & (fact[date_col] < dim["_et"]),
        "left",
    ).drop("_did", "_ef", "_et")
    return joined

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# fact_sales_transaction: store, customer (by transaction_date_id)
if spark.catalog.tableExists("silver.fact_sales_transaction"):
    f = read_cdf("silver.fact_sales_transaction").drop("_change_type", "_commit_version", "_commit_timestamp")
    if not f.rdd.isEmpty():
        f = resolve_scd2_sk(f, "transaction_date_id", "store", "store_id")
        f = resolve_scd2_sk(f, "transaction_date_id", "customer", "customer_id")
        m = merge_upsert("gold.fact_sales_transaction", f, "transaction_business_key")
        log_audit("04_Silver_to_Gold", "fact_sales_transaction", "gold", m["inserted"] + m["updated"], "OK")
        print(f"· fact_sales_transaction: +{m['inserted']}/{m['updated']}")

# fact_sales_line_item: store, product, customer (by transaction_date_id)
if spark.catalog.tableExists("silver.fact_sales_line_item"):
    f = read_cdf("silver.fact_sales_line_item").drop("_change_type", "_commit_version", "_commit_timestamp")
    if not f.rdd.isEmpty():
        f = resolve_scd2_sk(f, "transaction_date_id", "store", "store_id")
        f = resolve_scd2_sk(f, "transaction_date_id", "product", "product_id")
        f = resolve_scd2_sk(f, "transaction_date_id", "customer", "customer_id")
        m = merge_upsert("gold.fact_sales_line_item", f, "line_item_id")
        log_audit("04_Silver_to_Gold", "fact_sales_line_item", "gold", m["inserted"] + m["updated"], "OK")
        print(f"· fact_sales_line_item: +{m['inserted']}/{m['updated']}")

# fact_inventory_snapshot: store, product (by snapshot_date_id)
if spark.catalog.tableExists("silver.fact_inventory_snapshot"):
    f = read_cdf("silver.fact_inventory_snapshot").drop("_change_type", "_commit_version", "_commit_timestamp")
    if not f.rdd.isEmpty():
        f = resolve_scd2_sk(f, "snapshot_date_id", "store", "store_id")
        f = resolve_scd2_sk(f, "snapshot_date_id", "product", "product_id")
        m = merge_upsert("gold.fact_inventory_snapshot", f, "snapshot_id")
        log_audit("04_Silver_to_Gold", "fact_inventory_snapshot", "gold", m["inserted"] + m["updated"], "OK")
        print(f"· fact_inventory_snapshot: +{m['inserted']}/{m['updated']}")

# fact_loyalty_event: customer (by event_date_id)
if spark.catalog.tableExists("silver.fact_loyalty_event"):
    f = read_cdf("silver.fact_loyalty_event").drop("_change_type", "_commit_version", "_commit_timestamp")
    if not f.rdd.isEmpty():
        f = resolve_scd2_sk(f, "event_date_id", "customer", "customer_id")
        m = merge_upsert("gold.fact_loyalty_event", f, "loyalty_event_id")
        log_audit("04_Silver_to_Gold", "fact_loyalty_event", "gold", m["inserted"] + m["updated"], "OK")
        print(f"· fact_loyalty_event: +{m['inserted']}/{m['updated']}")

print("Silver → Gold complete. Star schema ready for Direct Lake.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
