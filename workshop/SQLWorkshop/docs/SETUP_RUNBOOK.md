# SQL Workshop Setup Runbook

End-to-end setup for running the HLS Data Warehousing / SQL Workshop in a new Databricks workspace.

## 1. Workspace Prerequisites

- **Options:** CloudLabs, Databricks Express, or an existing customer workspace.
- Ensure you have permissions to: create catalogs/schemas, create SQL warehouses, run jobs, create dashboards, and (if used) create Genie spaces.

## 2. Data Pipeline (Build the Gold Layer)

The workshop expects Unity Catalog objects populated by **BuildTheWorkspace.ipynb** in the same workspace setup package:

- **Notebook location:** `dbx_hls_workspace_setup/BuildTheWorkspace.ipynb` (the notebook’s folder must contain the `workspaceSetup` package so imports resolve).
- **Config (as in the notebook):**
  - `catalog="healthcare_dev"`
  - `bronze_schema="bronze"`, `silver_schema="silver"`, `gold_schema="gold"`
  - `volume_source="/Volumes/hls_bronze/hls_bronze/raw/syntheticData"`
  - `source_system="SYNTHEA"`
- **Reference data:** The notebook also sets a reference config and runs `run_reference_ingestion` using volumes under `/Volumes/hls_bronze/hls_bronze/raw/reference/` (e.g. `hcpcs`, `icd10cm`, `ndc`, `snomed_icd10cm`, `rxnorm`, `rxnorm_prescribe`). Ensure these reference volumes exist and are populated before running the pipeline; silver lookups depend on them.
- **Cell order:** Run cells in order: (1) path setup, (2) imports, (3) config, (4) setup catalog/schemas and discover states, (5) reference config, (6) reference ingestion, (7) Bronze stage (“Bronzer” cell), (8) Silver stage—the cell titled “Silvery” currently runs the same bronze code; to run the Silver stage and define `silver_results` for the summary, run `run_all_silver_transforms(spark, config)` and build `silver_results` in a separate cell before the Gold stage, (9) Gold stage (“Golden” cell: `run_gold_builds_in_order`), (10) Gold CMS stage (`run_cms_builds_in_order`), (11) Summary.

**Expected duration:** ~1 hour for full pipeline. Validation checkpoints:
- After bronze: tables in `healthcare_dev.bronze` (e.g. patients, encounters, claims, plus reference tables).
- After silver: tables in `healthcare_dev.silver` (e.g. v_patient_flattened, v_encounter_flattened, v_condition_flattened).
- After gold: tables in `healthcare_dev.gold` from `run_gold_builds_in_order` (dim_date, dim_patient, dim_provider, fact_encounter, etc.) and from `run_cms_builds_in_order` (dim_cms_beneficiary, dim_cms_provider, fact_cms_outpatient_claims, fact_cms_inpatient_claims, and other CMS gold tables).

## 3. Git Integration and Repos

- Create a Git credential (PAT-based) in the workspace: **Settings → Git integration → Add credential.**
- **Repos:** Create a Repo linked to this repository (or a fork). Clone so that the repo root (e.g. `healthcaredemo`) is available in the workspace.
- Ensure **dbx_hls_workspace_setup/workshop/SQL Workshop** is present so instructor notebooks and scripts can be run from there (same repo that contains `BuildTheWorkspace.ipynb`).

## 4. Run the Workshop Driver (Instructor)

- Navigate to **dbx_hls_workspace_setup/workshop/SQL Workshop/notebooks/instructors/01_workshop_driver.ipynb**.
- Set widgets to match the BuildTheWorkspace config: **catalog** = `healthcare_dev`, **workshop_schema** = `cms`, **gold_schema** = `gold`; set **create_warehouse** and **create_views** as needed.
- Run the driver. It will:
  - Create schema `ai` and `cms` if they do not exist.
  - Create workshop views in `cms` (e.g. `fact_patient_claims`, `rprt_patient_claims`, `dim_beneficiary`, `dim_provider`, `dim_date`, `dim_diagnosis`) that reference the gold layer.
  - Optionally create the SQL warehouse via SDK (if enabled).
- Note the output: catalog, schemas, and object names created.

## 5. SQL Warehouse

- **Name:** `SQL_Workshop_Serverless_Warehouse`.
- **Manual creation (if not created by driver):**
  - Go to **SQL Warehouses** in the workspace.
  - Create warehouse → Name: `SQL_Workshop_Serverless_Warehouse`, Size: **Small**, enable **Serverless**.
  - Set min/max clusters (e.g. 0–2 or 1–5) and auto-stop (e.g. 10 minutes).
- Confirm the warehouse is **Started** and usable from the SQL Editor.

## 6. Unity Catalog Objects

Verify:

- **Catalog:** e.g. `healthcare_dev` (or your chosen catalog).
- **Schemas:** `ai`, `gold`, and workshop schema `cms`.
- **Gold:** At least `dim_cms_beneficiary`, `dim_cms_provider`, `fact_cms_outpatient_claims`, `fact_cms_inpatient_claims`, `dim_date`, `dim_provider` (names may vary; workshop views in `cms` map to these).
- **Workshop schema (cms):** `fact_patient_claims`, `rprt_patient_claims`, `dim_beneficiary`, `dim_provider`, `dim_date`, `dim_diagnosis` (as views or tables).
- **Volumes/Models:** Per your pipeline; no additional volume required for the analyst lab if data is already in tables.

## 7. Model Serving Endpoint (predict_claim_amount)

- The lab uses an endpoint named **predict_claim_amount** for batch scoring in SQL (e.g. `ai_query`-style calls).
- **If the pipeline created it:** Confirm in **Serving** that the endpoint exists and is **Ready**.
- **Manual path:** See `docs/GENIE_SPACE_CONFIG.md` and workshop ML instructions for training/registering a model and creating a serving endpoint. Document the endpoint name and verify it is ready before the ML scoring lab step.

## 8. Dashboards

- The analyst guide has students create a dashboard from `rprt_patient_claims` (claim amount by year/gender, filters, etc.).
- Optionally, use the script in `scripts/create_dashboard.py` (run from an instructor notebook or job) to pre-create a sample dashboard; document the dashboard ID/URL for students.

## 9. Genie Space

- Genie spaces often require manual creation. Follow **docs/GENIE_SPACE_CONFIG.md** for:
  - Creating a Genie Space.
  - Attaching the default SQL warehouse and the gold/cms tables.
  - Configuring behavior rules (e.g. "local" = NY, state abbreviations, *_flag as boolean, default claim amount/date fields, diagnosis descriptions).
  - Sample questions to validate the space.

## 10. Validation

Run through **docs/VALIDATION_CHECKLIST.md** and fix any missing items before the workshop.

## Security

- Do not hardcode tokens or secrets. Use Databricks secrets or environment variables for any credentials (e.g. PAT, cloud storage).
- Ensure only intended users have access to the catalog/schemas and the SQL warehouse.
