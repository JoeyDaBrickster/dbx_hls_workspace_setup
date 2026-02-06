# Part 5 – Dashboard

## Objective

Create a dashboard from the reporting table **rprt_patient_claims**, with panels and filters.

## Prerequisites

- `rprt_patient_claims` exists and returns rows.
- SQL warehouse is available for the dashboard data source.

## Instructions

1. Go to **SQL → Dashboards** and click **Create dashboard**.
2. Add a **panel** and create (or attach) a query:
   - **Panel 1:** Claim amount by year and gender (trend or line chart).
   - **Panel 2:** Claim amount by state (bar chart).
3. Add a **Gender** filter to the dashboard (and optionally State or Year).
4. **Publish** the dashboard.
5. Interact with the filters and confirm the panels update.

Use the queries in **05_dashboard.ipynb** as a starting point.

## Success criteria

- Dashboard has at least two panels backed by `rprt_patient_claims`.
- At least one filter (e.g. Gender) is applied and drives the visuals.
