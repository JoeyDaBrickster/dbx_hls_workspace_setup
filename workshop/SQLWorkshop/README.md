# HLS SQL Workshop

This folder contains the **HLS Data Warehousing / SQL Workshop** for Databricks, recreated per the SQL Workshop Setup instruction set. It enables analysts to discover patient claims data in Unity Catalog, run parameterized queries, use AI model scoring in SQL, build dashboards, and use Genie for natural-language analytics.

## Structure

- **docs/** — Setup runbook, analyst lab guide, validation checklist, Genie config
- **notebooks/students/** — Databricks notebooks for workshop participants
- **notebooks/instructors/** — Driver and automation notebooks for instructors
- **scripts/** — Python code to create SQL warehouse, dashboards, and Genie spaces via Databricks SDK
- **instructions/** — Markdown instructions for each part of the workshop

## Quick Start

1. **Instructors:** Follow [docs/SETUP_RUNBOOK.md](docs/SETUP_RUNBOOK.md) to prepare the workspace, then run the driver notebook in `notebooks/instructors/`.
2. **Students:** Follow [docs/ANALYST_LAB_GUIDE.md](docs/ANALYST_LAB_GUIDE.md) and use the notebooks in `notebooks/students/`.

## Prerequisites

- Databricks workspace (CloudLabs, Databricks Express, or customer workspace).
- **BuildTheWorkspace.ipynb** has been run (in `dbx_hls_workspace_setup/`) so that `healthcare_dev.bronze`, `healthcare_dev.silver`, and `healthcare_dev.gold` are populated, including reference ingestion, Bronze, Silver, Gold dimensional tables, and Gold CMS tables (e.g. dim_cms_beneficiary, fact_cms_outpatient_claims, fact_cms_inpatient_claims). The notebook uses config: catalog `healthcare_dev`, volume_source `/Volumes/hls_bronze/hls_bronze/raw/syntheticData`, and reference volumes under `/Volumes/hls_bronze/hls_bronze/raw/reference/`.
- SQL warehouse created (e.g. `SQL_Workshop_Serverless_Warehouse`).
