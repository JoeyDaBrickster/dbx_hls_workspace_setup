# Part 2 – Query and Visualize

## Objective

Write a SQL query that joins the fact table to dimensions, aggregates by year/state/gender, and create a bar chart from the results.

## Prerequisites

- Part 1 completed (you know where `fact_patient_claims` and `dim_beneficiary` are).
- SQL warehouse is started.

## Instructions

1. Open **SQL Editor** and create a new query.
2. Write a query that:
   - Joins `fact_patient_claims` to `dim_beneficiary`.
   - Aggregates by year (from `claim_start_date`), state, and gender.
   - Computes sum of claim amount and count of claims.
3. Run the query and confirm results.
4. Add a **visualization:** Bar chart with X = state, Y = sum of total claim amount (you can use the simpler “totals by state” query in the notebook).
5. **Save** the query with a descriptive name (e.g. “Claim totals by state and year”).

## Success criteria

- Query runs and returns totals by year, state, and gender.
- A bar chart is created and saved with the query.
