"""
Create the SQL Workshop serverless warehouse via Databricks SDK.
Run from a Databricks notebook or environment with WorkspaceClient configured.
Do not hardcode tokens; use default credential chain or env.
"""
from databricks.sdk import WorkspaceClient

WORKSHOP_WAREHOUSE_NAME = "SQL_Workshop_Serverless_Warehouse"


def create_workshop_warehouse(
    name: str = WORKSHOP_WAREHOUSE_NAME,
    cluster_size: str = "Small",
    max_num_clusters: int = 5,
    auto_stop_mins: int = 10,
    enable_serverless: bool = True,
) -> str:
    """Create or reuse the workshop SQL warehouse. Returns warehouse ID."""
    w = WorkspaceClient()
    try:
        created = w.warehouses.create(
            name=name,
            cluster_size=cluster_size,
            max_num_clusters=max_num_clusters,
            auto_stop_mins=auto_stop_mins,
            enable_serverless_compute=enable_serverless,
        ).result()
        print(f"Created serverless SQL warehouse: {name} (id: {created.id})")
        return created.id
    except Exception as e:
        if "already exists" in str(e).lower() or "RESOURCE_ALREADY_EXISTS" in str(e):
            # Find existing by name
            for wh in w.warehouses.list():
                if wh.name == name:
                    print(f"SQL warehouse already exists: {name} (id: {wh.id})")
                    return wh.id
        raise e


if __name__ == "__main__":
    create_workshop_warehouse()
