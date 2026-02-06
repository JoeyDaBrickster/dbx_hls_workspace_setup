# SQL Workshop Analyst Lab Guide

This guide walks analysts through the HLS SQL Workshop using Unity Catalog, Databricks SQL, and Genie. Use the catalog and schema names from the workspace build: **healthcare_dev** (catalog) and **cms** (workshop schema), as set in BuildTheWorkspace.ipynb and the workshop driver.

## 1. Discover Data

- In the **Databricks workspace**, open **Catalog Explorer** (or **Data**).
- Search for **patient claims**. Locate the fact table (e.g. `{catalog}.{schema}.fact_patient_claims`).
- Open the table and review:
  - **Metadata:** Column names, types, comments.
  - **Relationships:** Foreign keys to `dim_beneficiary`, `dim_provider`, `dim_date`, `dim_diagnosis` (if shown).
  - **Lineage:** Upstream sources and downstream usage.

## 2. Query and Visualize

- Go to **SQL Editor**. Create a new query.
- Write a query that joins `fact_patient_claims` to `dim_beneficiary` (and optionally `dim_date`) to get totals by **year**, **state**, and **gender**.
- Use the assistant (if available) to help with the join and aggregation.
- Run the query. From the results, create a **visualization:** Bar chart with X = state, Y = sum of total claim amount.
- Save the query with a descriptive name (e.g. "Claim totals by state and year").

## 3. Query Parameter

- Create a **new** SQL query.
- Add a **Year** parameter (e.g. in the SQL editor or via a widget) and filter `claim_start_date` or the date dimension by that year.
- Run the query for one year; change the parameter and refresh to confirm it updates.

## 4. ML Scoring (Batch Prediction in SQL)

- The workshop uses a model serving endpoint **predict_claim_amount** for batch scoring.
- In SQL, use the `ai_query`-style pattern to pass a features struct (e.g. built from `beneficiary_code` or beneficiary attributes) and receive predictions. Refer to the student notebook **04_ml_scoring.sql** for the exact pattern.
- **Note:** Large batches can take time; start with a small limit (e.g. 10–100 rows) to verify.

## 5. Dashboard

- Create a **new dashboard** in Databricks SQL.
- Add a data source: the reporting table **rprt_patient_claims** (e.g. `{catalog}.{schema}.rprt_patient_claims`).
- Use the assistant to generate:
  - A panel: **Claim amount by year and gender** (e.g. trend or bar chart).
  - A second panel: **Claim amount by state** (bar chart).
- Add a **Gender** filter (and optionally State or Year) to the dashboard.
- Publish the dashboard and interact with the filters to confirm they drive the visuals.

## 6. Genie

- Open the **Genie** Space configured by your instructor (see **docs/GENIE_SPACE_CONFIG.md** for name and behavior rules).
- Ask: **"Claims for patients who saw a local provider."**
- Inspect how "local" is interpreted (e.g. NY); review the generated SQL and any auto-visualizations.
- Ask follow-up questions (e.g. by state, by diagnosis, by year) and compare with your earlier SQL and dashboard results.
