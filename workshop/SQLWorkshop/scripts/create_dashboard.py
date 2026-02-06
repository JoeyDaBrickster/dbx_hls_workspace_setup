"""
Create a sample SQL Workshop dashboard via Databricks SDK.
Run from a Databricks notebook or environment with WorkspaceClient configured.
Dashboard uses the reporting table rprt_patient_claims; warehouse_id must be set.
Widgets may need to be added manually in the UI; this script creates the dashboard and saved queries.
"""
from databricks.sdk import WorkspaceClient


def create_workshop_dashboard(
    warehouse_id: str,
    catalog: str = "healthcare_dev",
    schema: str = "cms",
    dashboard_name: str = "SQL Workshop - Patient Claims",
) -> str:
    """
    Create a dashboard and saved queries for the workshop.
    Add panels manually: open the dashboard, Add panel, and attach the saved queries.
    Returns dashboard ID.
    """
    w = WorkspaceClient()
    full_table = f"{catalog}.{schema}.rprt_patient_claims"

    # Saved query: Claim amount by year and gender
    q1 = w.sql.queries.create(
        name="Workshop - Claim amount by year and gender",
        data_source_id=warehouse_id,
        query=f"""
SELECT
    YEAR(claim_start_date) AS year,
    gender,
    SUM(claim_payment_amount) AS total_claim_amount,
    COUNT(*) AS claim_count
FROM {full_table}
WHERE claim_start_date IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2
""",
    ).result()
    print(f"Created query: {q1.name} (id: {q1.id})")

    # Saved query: Claim amount by state
    q2 = w.sql.queries.create(
        name="Workshop - Claim amount by state",
        data_source_id=warehouse_id,
        query=f"""
SELECT
    state,
    SUM(claim_payment_amount) AS total_claim_amount,
    COUNT(*) AS claim_count
FROM {full_table}
WHERE state IS NOT NULL
GROUP BY state
ORDER BY total_claim_amount DESC
""",
    ).result()
    print(f"Created query: {q2.name} (id: {q2.id})")

    # Create empty dashboard; add panels in UI from these queries
    dash = w.sql.dashboards.create(
        name=dashboard_name,
        dashboard_filters_enabled=True,
        tags=["sql_workshop"],
    ).result()
    print(f"Created dashboard: {dashboard_name} (id: {dash.id})")
    print("Add panels manually: open dashboard -> Add panel -> choose the saved queries above.")
    return dash.id


if __name__ == "__main__":
    import os
    wh_id = os.environ.get("WORKSHOP_WAREHOUSE_ID")
    if not wh_id:
        raise ValueError("Set WORKSHOP_WAREHOUSE_ID or pass warehouse_id")
    create_workshop_dashboard(warehouse_id=wh_id)
