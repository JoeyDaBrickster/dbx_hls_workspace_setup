# SQL Workshop Validation Checklist

Use this checklist to confirm the environment is ready before the workshop.

## SQL Warehouse

- [ ] A SQL warehouse named **SQL_Workshop_Serverless_Warehouse** exists (or the name configured for the workshop).
- [ ] Warehouse is **Started** and can be selected in the SQL Editor.
- [ ] Run a simple query (e.g. `SELECT 1`) in a new SQL query to confirm connectivity.

## Unity Catalog

- [ ] Catalog **healthcare_dev** exists (as in BuildTheWorkspace.ipynb).
- [ ] Schema **gold** exists and contains the tables produced by BuildTheWorkspace: from `run_gold_builds_in_order` (e.g. dim_date, dim_provider) and from `run_cms_builds_in_order` (dim_cms_beneficiary, dim_cms_provider, fact_cms_outpatient_claims, fact_cms_inpatient_claims, and other CMS gold tables).
- [ ] Workshop schema (e.g. **cms**) exists and contains:
  - [ ] **fact_patient_claims** (view or table)
  - [ ] **rprt_patient_claims** (view or table)
  - [ ] **dim_beneficiary**, **dim_provider**, **dim_date**, **dim_diagnosis** (views or tables)
- [ ] Schema **ai** exists (for feature table / model registration; optional for ML step).

**Quick check (replace catalog/schema as needed):**

```sql
USE CATALOG healthcare_dev;
USE SCHEMA cms;
SHOW TABLES;
SELECT COUNT(*) FROM fact_patient_claims;
SELECT COUNT(*) FROM rprt_patient_claims;
```

## Gold Tables and Reporting Table

- [ ] `fact_patient_claims` returns rows and has expected columns (e.g. beneficiary_key, claim_start_date, claim amount).
- [ ] `rprt_patient_claims` returns rows and can be used as a dashboard source.

## Model Serving Endpoint

- [ ] Endpoint **predict_claim_amount** exists under **Serving** (or documented alternative).
- [ ] Endpoint status is **Ready**.
- [ ] Sample scoring query (see student notebook or GENIE_SPACE_CONFIG) runs without error (optional; can be skipped if endpoint is not yet deployed).

## Dashboards

- [ ] You can create a new dashboard and add a panel using `rprt_patient_claims`.
- [ ] Filters can be added and applied to the dashboard.

## Genie Space

- [ ] Genie Space is created and configured per **docs/GENIE_SPACE_CONFIG.md**.
- [ ] Default warehouse is attached; gold/cms tables are selected.
- [ ] Behavior rules are set (e.g. "local" = NY, state abbreviations, *_flag as boolean).
- [ ] Sample question "Claims for patients who saw a local provider" returns results and reflects the "local = NY" convention.

## Repo and Notebooks

- [ ] Repo is present with **dbx_hls_workspace_setup** (so that BuildTheWorkspace.ipynb and workspaceSetup are available) and **dbx_hls_workspace_setup/workshop/SQL Workshop/**.
- [ ] Student notebooks open and run in the correct order (01–06).
- [ ] Instructor driver notebook (01_workshop_driver.ipynb) runs and creates workshop views in `healthcare_dev.cms` and optionally the SQL warehouse.
