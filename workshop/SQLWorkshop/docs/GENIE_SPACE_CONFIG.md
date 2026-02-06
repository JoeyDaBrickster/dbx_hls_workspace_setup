# Genie Space Configuration for SQL Workshop

This document specifies how to create and configure a Genie Space so that analysts can ask natural-language questions over the workshop gold layer. Genie Spaces may require manual creation in the workspace UI.

## Required Objects

- **Default SQL warehouse:** Use the workshop warehouse (e.g. **SQL_Workshop_Serverless_Warehouse**).
- **Tables to attach:** Select the workshop tables that the Space will query. With the default BuildTheWorkspace/driver setup (catalog `healthcare_dev`, schema `cms`), attach at least:
  - `healthcare_dev.cms.fact_patient_claims`
  - `healthcare_dev.cms.rprt_patient_claims`
  - `healthcare_dev.cms.dim_beneficiary`
  - `healthcare_dev.cms.dim_provider`
  - `healthcare_dev.cms.dim_date`
  - `healthcare_dev.cms.dim_diagnosis` (if present)

Using the workshop schema **cms** matches the analyst guide and the views created by the workshop driver from `healthcare_dev.gold`.

## Behavior Rules (Conventions)

Configure the following behavior rules so Genie interprets questions consistently:

1. **Local provider:** When the user says "local" or "local provider," interpret this as **New York (NY)**. Use state code `NY` in filters on provider or beneficiary state.
2. **State abbreviations:** Always use two-letter US state abbreviations (e.g. NY, CA, TX) in filters and in generated SQL.
3. **Flag fields:** Treat columns whose names end in `_flag` or contain "ind" as **boolean** (e.g. 0/1 or Y/N). Use them in WHERE clauses as appropriate when users ask about "flagged" or "indicator" data.
4. **Claim amount and date defaults:** When the user asks about "claims" or "claim amount" without specifying a measure, use the primary claim amount column (e.g. `claim_payment_amount`, `clm_pmt_amt`, or the column name present in `fact_patient_claims`). For dates, use the main service/claim date column (e.g. `claim_start_date`, `clm_from_dt`) for filtering by year or date range.
5. **Diagnosis descriptions:** When the user refers to diagnoses by name, join to `dim_diagnosis` (or the diagnosis dimension) and use the description/code columns to filter or display. Prefer human-readable descriptions in answers when available.

## Sample Questions

Use these to validate the Genie Space after configuration:

- "Claims for patients who saw a local provider."
- "Total claim amount by state."
- "How many claims in New York in 2020?"
- "Claim amounts by year and gender."
- "Show me diagnosis descriptions for the top 10 claim amounts."

After asking "Claims for patients who saw a local provider," verify that the underlying SQL filters by provider or beneficiary state = 'NY' (or equivalent) and that the results reflect that convention.
