# Part 4 – ML Scoring (Batch Prediction in SQL)

## Objective

Call the **predict_claim_amount** model serving endpoint from SQL using an `ai_query`-style pattern (or equivalent in your workspace).

## Prerequisites

- The **predict_claim_amount** endpoint exists and is **Ready** (see SETUP_RUNBOOK and instructor).
- You have permission to call the endpoint from SQL.

## Instructions

1. Open the notebook **04_ml_scoring.ipynb** or create a new SQL query.
2. Build a small result set (e.g. distinct beneficiaries with a LIMIT of 10–100).
3. Use the documented SQL function to call the endpoint (e.g. `ai_query('predict_claim_amount', features_struct)`) and add a prediction column. The exact function name and argument shape depend on how the model was registered.
4. Run the query; confirm predictions are returned.
5. **Note:** Large batches can take a long time; keep the row set small for the lab.

## Success criteria

- A SQL query that calls the endpoint runs without error.
- At least one prediction column is returned (or the placeholder is replaced with the real call once the endpoint is ready).

## If the endpoint is not ready

- Leave the placeholder in the notebook and document the steps to enable it (see SETUP_RUNBOOK and GENIE_SPACE_CONFIG).
