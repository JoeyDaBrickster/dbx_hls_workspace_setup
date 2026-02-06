# Part 1 – Discover Data

## Objective

Find the patient claims fact table in Unity Catalog and inspect its metadata, relationships, and lineage.

## Prerequisites

- Workshop driver has been run (schemas and views exist).
- You have SELECT on the workshop schema (e.g. `cms`).

## Instructions

1. Open **Catalog Explorer** (or **Data** in the left sidebar).
2. Navigate to the workshop catalog and schema (e.g. `healthcare_dev` → `cms`).
3. Search or browse for **fact_patient_claims** and open it.
4. Review:
   - **Columns:** Names, types, and any comments.
   - **Relationships:** Foreign keys to `dim_beneficiary`, `dim_provider`, `dim_date`, `dim_diagnosis`.
   - **Lineage:** Upstream sources and downstream objects.
5. Optionally run the notebook **01_discover_data.ipynb** to run `DESCRIBE TABLE fact_patient_claims` and a row count.

## Success criteria

- You can locate `fact_patient_claims` and see its columns and relationships.
- You understand how it links to beneficiary and provider dimensions.
