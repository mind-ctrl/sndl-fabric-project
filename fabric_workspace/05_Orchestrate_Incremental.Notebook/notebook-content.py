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

# # 05 · Orchestrate Incremental
# 
# The driver that ties the medallion together. Two modes:
# 
# * **`bulk`** — one-pass historical backfill: Landing → Bronze → Silver → Gold.
# * **`daily`** — process the **next unprocessed day** (chosen from the watermark)
#   through all four layers, then advance. Re-runnable and idempotent.
# 
# In Fabric this notebook is typically replaced by a **Data Pipeline** (see
# `pipelines/`) for retries/alerting, but the same logic lives here so the whole
# flow can be driven from one place and unit-run. It calls the layer notebooks with
# `notebookutils.notebook.run(...)` — no notebook hardcodes a workspace.

# CELL ********************

# PARAMETERS CELL
stage = "dev"
mode = "daily"                       # "bulk" | "daily"
bulk_processing_date = "2025-12-21"  # backfill stamp for the historical load
daily_start = "2025-12-22"
daily_end = "2025-12-31"
control_path = "Tables"

# CELL ********************

%run nb_utils

# CELL ********************

from datetime import date, timedelta

try:
    import notebookutils  # Fabric runtime
    run_nb = lambda nb, params: notebookutils.notebook.run(nb, 1800, params)
except Exception:
    # local/dev shim so the orchestration logic is testable without the Fabric runtime
    def run_nb(nb, params):
        print(f"   [shim] would run {nb} with {params}")
        return "shim-ok"

def run_layers(mode_, proc_date):
    print(f"\n=== {mode_} :: {proc_date} ===")
    run_nb("01_Raw_to_Landing",   {"stage": stage, "mode": mode_, "processing_date": proc_date})
    run_nb("02_Landing_to_Bronze", {"stage": stage})
    run_nb("03_Bronze_to_Silver",  {"stage": stage, "processing_date": proc_date})
    run_nb("04_Silver_to_Gold",    {"stage": stage})

# MARKDOWN ********************

# ## Next-date selection from the watermark (idempotent daily loop)

# CELL ********************

def next_daily_date():
    """The earliest day in [daily_start, daily_end] not yet reflected in the
    Gold transaction watermark. Returns None when the window is fully loaded."""
    wm = read_watermark("fact_sales_transaction", default=bulk_processing_date)
    start = date.fromisoformat(daily_start)
    end = date.fromisoformat(daily_end)
    cur = start
    while cur <= end:
        if str(cur) > wm:
            return str(cur)
        cur += timedelta(days=1)
    return None

# CELL ********************

if mode == "bulk":
    run_layers("bulk", bulk_processing_date)
    print("\nBulk backfill complete.")
elif mode == "daily":
    nd = next_daily_date()
    if nd is None:
        print("Daily window already fully loaded — nothing to do.")
    else:
        run_layers("daily", nd)
        print(f"\nDaily increment {nd} complete. Re-run to load the next day.")
else:
    raise ValueError(f"unknown mode {mode!r}")

# MARKDOWN ********************

# ## Load summary (latest audit rows)

# CELL ********************

if spark.catalog.tableExists("control.load_audit"):
    from pyspark.sql import functions as F
    display(spark.read.table("control.load_audit").orderBy(F.col("run_ts").desc()).limit(25))
