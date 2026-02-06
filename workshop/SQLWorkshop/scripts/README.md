# SQL Workshop Scripts

Python scripts used by the instructor driver to set up the workshop. They assume the gold layer from **BuildTheWorkspace.ipynb** (in `dbx_hls_workspace_setup/`) already exists: catalog `healthcare_dev`, schema `gold`, with dim_cms_beneficiary, dim_cms_provider, fact_cms_*, dim_date. Run from a Databricks notebook (with `spark` and `dbutils` in scope) or from a job with the Databricks SDK configured.

| Script | Purpose |
|--------|---------|
| **create_sql_warehouse.py** | Create the serverless SQL warehouse `SQL_Workshop_Serverless_Warehouse` via the Databricks SDK. |
| **create_workshop_views.py** | Create schemas `cms` and `ai`, and views: `fact_patient_claims`, `rprt_patient_claims`, `dim_beneficiary`, `dim_provider`, `dim_date`, `dim_diagnosis` in the workshop schema. |
| **create_dashboard.py** | Create a sample dashboard and saved queries using `rprt_patient_claims`; panels can be added manually in the UI. |
| **genie_space_config.py** | Configuration and behavior rules for the Genie Space (see docs/GENIE_SPACE_CONFIG.md). No secrets; use for documentation or future API automation. |

**Security:** Do not hardcode tokens or secrets. Use Databricks default credential chain or environment variables.
