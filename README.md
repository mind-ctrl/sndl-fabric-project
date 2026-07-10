<h1 align="center">SNDL Retail Intelligence Platform</h1>

<p align="center">
  End-to-end <b>Microsoft Fabric</b> analytics for a multi-banner cannabis and liquor retailer.<br/>
  Medallion lakehouse, Direct Lake semantic model, and executive Power BI reporting, built entirely as code.
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Microsoft%20Fabric-0078D4?style=for-the-badge&logo=microsoft&logoColor=white" alt="Fabric"/>
  <img src="https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black" alt="Power BI"/>
  <img src="https://img.shields.io/badge/Delta%20Lake-003366?style=for-the-badge&logo=databricks&logoColor=white" alt="Delta"/>
  <img src="https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="PySpark"/>
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/DAX%20%2F%20TMDL-512BD4?style=for-the-badge" alt="DAX"/>
</p>

> [!NOTE]
> **This is a fictional demonstration project.** It was self-initiated and built around a made-up scenario inspired by SNDL Inc., a real Canadian cannabis and liquor retailer. It was **not** commissioned by, built for, or affiliated with SNDL, and it uses **100% synthetic data**. Company names and banners appear only to make the scenario realistic.

---

## Contents

- [Overview](#overview)
- [Key capabilities](#key-capabilities)
- [Architecture](#architecture)
- [Data](#data)
- [Semantic model](#semantic-model)
- [Reporting](#reporting)
- [Repository layout](#repository-layout)
- [Notebooks](#notebooks)
- [Running it](#running-it)
- [CI/CD](#cicd)
- [Tech stack](#tech-stack)
- [Status](#status)

---

## Overview

A retail analytics platform built on Microsoft Fabric for a large Canadian cannabis and liquor retailer. It ingests point-of-sale, inventory, and loyalty data, moves it through a Bronze / Silver / Gold medallion, and serves a governed Direct Lake semantic model to an executive Power BI report.

The dataset is **fully synthetic**, generated deterministically from a seed. The scenario is a fictional one inspired by **SNDL Inc.** and its six retail banners: **Value Buds**, **Spiritleaf**, and **Cost Cannabis** on the cannabis side, and **Wine and Beyond**, **Liquor Depot**, and **Ace Liquor** on the liquor side, together with a lineup of cannabis brands and the **Rise Rewards** loyalty program. No real company data is used, and the figures are illustrative sample-scale values.

Every artifact is plain text and version controlled: notebooks as `.py`, the semantic model as TMDL, the report as PBIR, and pipelines as JSON.

## Key capabilities

| Area | What it does |
| :--- | :--- |
| **Medallion lakehouse** | Landing, Bronze, Silver, and Gold layers in a single schema-enabled lakehouse (`LH_SNDL`). |
| **Incremental loading** | A `control_watermark` table plus a metadata registry drive a generic per-table loop; every write is a Delta `MERGE`, so re-runs stay idempotent. |
| **Change Data Feed** | Silver to Gold reads only changed rows via Delta CDF. |
| **SCD2 history** | Store, product, and customer dimensions keep one row per version; facts resolve the version effective at the transaction date. |
| **Data quality gates** | Rule-based checks route bad rows to a quarantine table instead of failing the run. |
| **Governed model** | Direct Lake semantic model with standardized measures, a calculation group, a field parameter, RLS, and OLS. |
| **CI/CD** | Git-tracked items promoted across Dev, Test, and Prod workspaces. |

## Architecture

```
   Source files (CSV / Parquet)
   bulk backfill  +  trailing daily increments
                │
                ▼
        ┌───────────────┐   Processing_Date stamped on arrival
        │    LANDING    │
        └───────────────┘
                │  metadata loop · Delta MERGE · Change Data Feed enabled
                ▼
        ┌───────────────┐
        │    BRONZE     │   raw, typed, deduplicated
        └───────────────┘
                │  CDF read · data-quality quarantine · SCD2 on dimensions
                ▼
        ┌───────────────┐
        │    SILVER     │   cleansed, conformed, historized
        └───────────────┘
                │  surrogate keys · effective-at-date fact joins
                ▼
        ┌───────────────┐
        │     GOLD      │   dimensional star schema
        └───────────────┘
                │
                ▼
     Direct Lake semantic model  ─►  Power BI report

   control_watermark + metadata registry keep every run idempotent
```

All layers live in one **schema-enabled lakehouse** using `landing`, `bronze`, `silver`, `gold`, and `control` schemas, rather than three separate lakehouses.

## Data

Generated with a fixed seed over the window **2024-01-01 to 2025-12-31**, with a daily-increment demonstration across the last ten days of 2025.

Fact tables cover sales transactions, sales line items, inventory snapshots, and loyalty events. Dimensions: `dim_date`, `dim_store` (359 stores across 167 liquor and 192 cannabis locations), `dim_product`, `dim_customer`, `dim_employee` (about 2,600), `dim_promotion`, `dim_banner` (6), and `dim_category`. Age gates, purchase limits, and tax rules follow per-province rules for both cannabis and liquor.

<details>
<summary><b>Headline KPIs (synthetic, sample scale)</b></summary>

| Metric | Value |
| :--- | ---: |
| Net revenue | $346.95M |
| Gross revenue (incl. tax) | $372.42M |
| Average transaction value | $62.07 |
| Units per transaction | 2.41 |
| Loyalty transaction share | 62.6% |
| Blended gross margin | 23.8% |

</details>

## Semantic model

A governed **Direct Lake** model (`SNDL_Sales`) over the Gold star schema.

- About **70 DAX measures** covering sales, margin, loyalty, cohort and retention, and time intelligence (prior year, year over year, same-store).
- A **calculation group** (`Time Intelligence`) and a **field parameter** metric switcher (`Metric Selection`).
- **Row-Level Security**: `RLS Alberta Region`, `RLS Cannabis Banners`, and `RLS Dynamic Banner by UPN` (dynamic via `USERPRINCIPALNAME`).
- **Object-Level Security**: `OLS Analyst No PII` hides customer PII columns.
- The cohort axis (`enrollment_month`) is materialized as a physical Gold column so Direct Lake can host it.

## Reporting

A thin **PBIR** report with five pages: Home, Executive Overview, Sales and Margin, Customer and Loyalty, and Product and Inventory. It includes a custom navigation rail, KPI cards with sparklines, a gross-margin waterfall, a cohort-retention heatmap built in Deneb (Vega-Lite), and report-scoped SVG measures, all on a consistent theme.

Three sourcing variants of the same report share the model definition:

| Variant | Source | Use |
| :--- | :--- | :--- |
| **Live** | Direct Lake, live connection | Full data against the published model |
| **Import** | Gold SQL endpoint | Self-contained file for sharing without a running capacity |
| **Local** | Local Parquet sample (~10%) | Offline development with no cloud dependency |

## Repository layout

```
.
├── fabric_workspace/                 Fabric build (as code)
│   ├── nb_utils.Notebook/            shared engine: watermark, MERGE, SCD2, CDF, DQ, audit
│   ├── 00_Control_Setup … 05_Orchestrate_Incremental   medallion notebooks
│   ├── SNDL_Sales.SemanticModel/     Direct Lake model (TMDL)
│   ├── SNDL_Sales.Report/            PBIR report + design notes
│   ├── LH_SNDL.Lakehouse/            schema-enabled lakehouse (definition)
│   ├── deployment/                   deployment rules and CI/CD notes
│   └── README.md                     notebook walkthrough
├── sndl_synthetic_data/              data generation
│   ├── 07_generation_code/           Python generator (seed based, uv)
│   ├── 01_research_findings/         domain facts, regulatory rules, generation plan
│   ├── 02_schemas/                   JSON schemas and ERD
│   └── 06_kpi_validation/            KPI reconciliation
└── SNDL_PET_PROJECT_DATA_GENERATION_SPEC.md   data specification
```

> Generated CSV and Parquet data, virtual environments, and Power BI caches are excluded from source control.

## Notebooks

| Notebook | Responsibility |
| :--- | :--- |
| `nb_utils` | Shared engine: watermark read and write, Delta MERGE, SCD2, CDF read, DQ checks, audit logging |
| `00_Control_Setup` | Create and seed `control_watermark`, the table metadata registry, and audit tables |
| `01_Raw_to_Landing` | Stamp `Processing_Date` on arrival (bulk or daily mode) |
| `02_Landing_to_Bronze` | Metadata loop, MERGE, enable Change Data Feed, advance the watermark |
| `03_Bronze_to_Silver` | CDF read, DQ quarantine, SCD2 dimensions, MERGE facts |
| `04_Silver_to_Gold` | Build the SCD2-aware star schema with surrogate keys and effective-at-date joins |
| `05_Orchestrate_Incremental` | Orchestrate the layers and select the next unprocessed day from the watermark |

## Running it

1. Create Dev, Test, and Prod workspaces on a Fabric capacity, each with the `LH_SNDL` lakehouse.
2. Generate data and upload it to the lakehouse landing area:
   ```bash
   cd sndl_synthetic_data/07_generation_code
   uv run generate_all.py --stage dev
   ```
3. Run `00_Control_Setup`, then the bulk backfill.
4. Run the daily-incremental orchestrator until the window completes.
5. Refresh the `SNDL_Sales` Direct Lake model and open the report.
6. Connect the workspace to Git and promote Dev to Test to Prod.

## CI/CD

Every item is plain text (notebooks, TMDL, PBIR, pipeline JSON) and synced through Fabric Git integration. Promotion runs across three stages (Dev, Test, Prod) with deployment rules that repoint the model source per stage. Per-stage data scale is parameterized so lower environments load a fraction of the data.

## Tech stack

| Area | Tools |
| :--- | :--- |
| Platform | Microsoft Fabric (OneLake, Lakehouse, Direct Lake, SQL analytics endpoint) |
| Engineering | PySpark, Delta Lake (MERGE, Change Data Feed), `notebookutils` |
| Modeling | TMDL, DAX, RLS and OLS, calculation groups, field parameters |
| Reporting | Power BI (PBIR), Deneb (Vega-Lite), SVG measures |
| Data generation | Python 3.10+, pandas, numpy, pyarrow, faker, managed with uv |
| DevOps | Git, Fabric deployment pipelines |

## Status

- Research, data generation, and validation complete.
- Medallion deployed and run end to end; Gold star schema serving the Direct Lake model.
- Semantic model published, refreshed, and serving full data.
- Workspace items under Git version control.
- Daily-increment demonstration and Dev to Test to Prod pipeline in progress.

---

<sub>All data is synthetic and generated for demonstration. This project is independent and not affiliated with or endorsed by SNDL Inc.</sub>
