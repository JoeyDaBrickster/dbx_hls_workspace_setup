# Instructor Setup

## Objective

Prepare the workspace so students can run the analyst lab (discover data, query, parameter, ML scoring, dashboard, Genie).

## Prerequisites

- Databricks workspace with Unity Catalog.
- **BuildTheWorkspace.ipynb** (in `dbx_hls_workspace_setup/`) has been run so the gold layer exists. The notebook uses catalog `healthcare_dev`, schemas `bronze` / `silver` / `gold`, volume_source `/Volumes/hls_bronze/hls_bronze/raw/syntheticData`, and reference volumes under `/Volumes/hls_bronze/hls_bronze/raw/reference/`. Required gold tables for the workshop include: `dim_cms_beneficiary`, `dim_cms_provider`, `fact_cms_outpatient_claims`, `fact_cms_inpatient_claims`, `dim_date` (and optionally `dim_provider` from the dimensional gold build).

## Steps

1. **Run the driver notebook**
   - Open **dbx_hls_workspace_setup/workshop/SQL Workshop/notebooks/instructors/01_workshop_driver.ipynb**.
   - Set widgets to match BuildTheWorkspace: catalog = `healthcare_dev`, workshop_schema = `cms`, gold_schema = `gold`.
   - Set “Create workshop views” = true; optionally “Create SQL warehouse” and “Create sample dashboard”.
   - Run all cells. Confirm no errors and note the summary output.

2. **Verify SQL warehouse**
   - Ensure **SQL_Workshop_Serverless_Warehouse** exists and is started (create manually if the driver did not create it). See **docs/SETUP_RUNBOOK.md**.

3. **Verify UC objects**
   - Run the checks in **docs/VALIDATION_CHECKLIST.md** (schemas, views, row counts).

4. **Model endpoint (optional)**
   - If the lab includes ML scoring, ensure the **predict_claim_amount** endpoint exists and is Ready, or document the manual path for students.

5. **Genie Space**
   - Create the Genie Space manually per **docs/GENIE_SPACE_CONFIG.md** (behavior rules, tables, sample questions). Share the Space name/link with students.

6. **Share with students**
   - Catalog and schema names (e.g. `healthcare_dev`, `cms`).
   - Link to **docs/ANALYST_LAB_GUIDE.md** and the student notebooks in **notebooks/students/**.
   - Link to **instructions/** for per-part instructions.
