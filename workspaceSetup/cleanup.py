"""
Pipeline Cleanup

Remove all tables, schemas, and optionally the catalog created by the pipeline.

WARNING: This module will permanently delete data. Use with caution.
"""

from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

from pyspark.sql import SparkSession


@dataclass(frozen=True)
class CleanupConfig:
    """Configuration for cleanup operations."""
    catalog: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    cleanup_level: str  # 'tables_only', 'schemas', 'catalog'
    dry_run: bool
    
    @property
    def schemas(self) -> Tuple[str, ...]:
        return (self.bronze_schema, self.silver_schema, self.gold_schema)


@dataclass
class CleanupResult:
    """Result of cleanup operation."""
    action: str
    object_type: str
    object_name: str
    success: bool
    message: str = ""


def create_cleanup_config(
    catalog: str = "healthcare_dev",
    bronze_schema: str = "bronze",
    silver_schema: str = "silver",
    gold_schema: str = "gold",
    cleanup_level: str = "tables_only",
    dry_run: bool = True
) -> CleanupConfig:
    """Create cleanup configuration."""
    return CleanupConfig(
        catalog=catalog,
        bronze_schema=bronze_schema,
        silver_schema=silver_schema,
        gold_schema=gold_schema,
        cleanup_level=cleanup_level,
        dry_run=dry_run
    )


def catalog_exists(spark: SparkSession, catalog_name: str) -> bool:
    """Check if a catalog exists."""
    try:
        catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]
        return catalog_name in catalogs
    except Exception:
        return False


def schema_exists(spark: SparkSession, catalog_name: str, schema_name: str) -> bool:
    """Check if a schema exists in a catalog."""
    try:
        spark.sql(f"USE CATALOG {catalog_name}")
        schemas = [row.databaseName for row in spark.sql("SHOW SCHEMAS").collect()]
        return schema_name in schemas
    except Exception:
        return False


def list_tables_in_schema(spark: SparkSession, catalog_name: str, schema_name: str) -> List[str]:
    """List all tables in a schema."""
    try:
        spark.sql(f"USE CATALOG {catalog_name}")
        spark.sql(f"USE SCHEMA {schema_name}")
        tables = spark.sql("SHOW TABLES").collect()
        return [row.tableName for row in tables]
    except Exception as e:
        print(f"Warning: Could not list tables in {catalog_name}.{schema_name}: {e}")
        return []


def get_table_row_count(spark: SparkSession, catalog_name: str, schema_name: str, table_name: str) -> int:
    """Get row count for a table."""
    try:
        return spark.table(f"{catalog_name}.{schema_name}.{table_name}").count()
    except Exception:
        return -1


def discover_all_objects(spark: SparkSession, config: CleanupConfig) -> Dict[str, List[str]]:
    """Discover all objects that would be cleaned up."""
    objects = {}
    
    if not catalog_exists(spark, config.catalog):
        print(f"Catalog '{config.catalog}' does not exist.")
        return objects
    
    for schema in config.schemas:
        if schema_exists(spark, config.catalog, schema):
            tables = list_tables_in_schema(spark, config.catalog, schema)
            objects[schema] = tables
        else:
            objects[schema] = []
    
    return objects


def drop_table(spark: SparkSession, catalog: str, schema: str, table: str, dry_run: bool) -> CleanupResult:
    """Drop a single table."""
    full_name = f"{catalog}.{schema}.{table}"
    
    if dry_run:
        return CleanupResult(
            action="DROP TABLE",
            object_type="table",
            object_name=full_name,
            success=True,
            message="[DRY RUN] Would drop"
        )
    
    try:
        spark.sql(f"DROP TABLE IF EXISTS {full_name}")
        return CleanupResult(
            action="DROP TABLE",
            object_type="table",
            object_name=full_name,
            success=True,
            message="Dropped"
        )
    except Exception as e:
        return CleanupResult(
            action="DROP TABLE",
            object_type="table",
            object_name=full_name,
            success=False,
            message=str(e)
        )


def drop_schema(spark: SparkSession, catalog: str, schema: str, dry_run: bool) -> CleanupResult:
    """Drop a schema and all its contents."""
    full_name = f"{catalog}.{schema}"
    
    if dry_run:
        return CleanupResult(
            action="DROP SCHEMA",
            object_type="schema",
            object_name=full_name,
            success=True,
            message="[DRY RUN] Would drop with CASCADE"
        )
    
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS {full_name} CASCADE")
        return CleanupResult(
            action="DROP SCHEMA",
            object_type="schema",
            object_name=full_name,
            success=True,
            message="Dropped with CASCADE"
        )
    except Exception as e:
        return CleanupResult(
            action="DROP SCHEMA",
            object_type="schema",
            object_name=full_name,
            success=False,
            message=str(e)
        )


def drop_catalog(spark: SparkSession, catalog: str, dry_run: bool) -> CleanupResult:
    """Drop a catalog and all its contents."""
    if dry_run:
        return CleanupResult(
            action="DROP CATALOG",
            object_type="catalog",
            object_name=catalog,
            success=True,
            message="[DRY RUN] Would drop with CASCADE"
        )
    
    try:
        spark.sql(f"DROP CATALOG IF EXISTS {catalog} CASCADE")
        return CleanupResult(
            action="DROP CATALOG",
            object_type="catalog",
            object_name=catalog,
            success=True,
            message="Dropped with CASCADE"
        )
    except Exception as e:
        return CleanupResult(
            action="DROP CATALOG",
            object_type="catalog",
            object_name=catalog,
            success=False,
            message=str(e)
        )


def run_cleanup(
    spark: SparkSession,
    config: CleanupConfig,
    discovered: Optional[Dict[str, List[str]]] = None
) -> List[CleanupResult]:
    """Execute cleanup based on configuration."""
    if discovered is None:
        discovered = discover_all_objects(spark, config)
    
    results = []
    
    print("\n" + "="*60)
    print(f"EXECUTING CLEANUP - Level: {config.cleanup_level.upper()}")
    if config.dry_run:
        print("*** DRY RUN MODE - No actual changes ***")
    print("="*60)
    
    if config.cleanup_level == "catalog":
        print(f"\nDropping catalog: {config.catalog}")
        result = drop_catalog(spark, config.catalog, config.dry_run)
        results.append(result)
        status = "OK" if result.success else "FAILED"
        print(f"  {status}: {result.message}")
        
    elif config.cleanup_level == "schemas":
        for schema in config.schemas:
            print(f"\nDropping schema: {config.catalog}.{schema}")
            result = drop_schema(spark, config.catalog, schema, config.dry_run)
            results.append(result)
            status = "OK" if result.success else "FAILED"
            print(f"  {status}: {result.message}")
            
    else:  # tables_only
        for schema, tables in discovered.items():
            if tables:
                print(f"\nDropping tables in {config.catalog}.{schema}:")
                for table in sorted(tables):
                    result = drop_table(spark, config.catalog, schema, table, config.dry_run)
                    results.append(result)
                    status = "OK" if result.success else "FAILED"
                    print(f"  {table}: {status} - {result.message}")
    
    return results


def print_cleanup_summary(config: CleanupConfig, results: List[CleanupResult]) -> None:
    """Print cleanup summary."""
    print("\n" + "="*60)
    print("CLEANUP SUMMARY")
    print("="*60)
    
    if config.dry_run:
        print("\n*** THIS WAS A DRY RUN - NO CHANGES WERE MADE ***")
        print("Set 'dry_run' to False to execute the cleanup.\n")
    
    successful = [r for r in results if r.success]
    failed = [r for r in results if not r.success]
    
    print(f"Total operations: {len(results)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")
    
    if failed:
        print("\nFailed operations:")
        for r in failed:
            print(f"  - {r.action} {r.object_name}: {r.message}")
    
    by_type = {}
    for r in successful:
        by_type.setdefault(r.object_type, []).append(r)
    
    if by_type:
        print("\nObjects affected:")
        for obj_type, type_results in by_type.items():
            print(f"  {obj_type}s: {len(type_results)}")
    
    print("\n" + "="*60)


def print_manual_cleanup_commands(config: CleanupConfig, discovered: Dict[str, List[str]]) -> None:
    """Print manual SQL cleanup commands."""
    print("\n" + "="*60)
    print("MANUAL CLEANUP COMMANDS")
    print("Copy and run these SQL commands if needed:")
    print("="*60)
    
    print("\n-- Drop all tables (preserves schemas):")
    for schema, tables in discovered.items():
        for table in sorted(tables):
            print(f"DROP TABLE IF EXISTS {config.catalog}.{schema}.{table};")
    
    print("\n-- Drop schemas (removes all tables):")
    for schema in config.schemas:
        print(f"DROP SCHEMA IF EXISTS {config.catalog}.{schema} CASCADE;")
    
    print("\n-- Drop entire catalog (removes everything):")
    print(f"DROP CATALOG IF EXISTS {config.catalog} CASCADE;")
    
    print("\n" + "="*60)


def cleanup(
    spark: SparkSession,
    catalog: str = "healthcare_dev",
    bronze_schema: str = "bronze",
    silver_schema: str = "silver",
    gold_schema: str = "gold",
    cleanup_level: str = "tables_only",
    dry_run: bool = True
) -> List[CleanupResult]:
    """Main cleanup function.
    
    Args:
        spark: SparkSession
        catalog: Catalog name
        bronze_schema: Bronze schema name
        silver_schema: Silver schema name
        gold_schema: Gold schema name
        cleanup_level: 'tables_only', 'schemas', or 'catalog'
        dry_run: If True, only preview changes without executing
    
    Returns:
        List of CleanupResult
    """
    config = create_cleanup_config(
        catalog=catalog,
        bronze_schema=bronze_schema,
        silver_schema=silver_schema,
        gold_schema=gold_schema,
        cleanup_level=cleanup_level,
        dry_run=dry_run
    )
    
    print("="*60)
    print("CLEANUP CONFIGURATION")
    print("="*60)
    print(f"Catalog:       {config.catalog}")
    print(f"Schemas:       {', '.join(config.schemas)}")
    print(f"Cleanup Level: {config.cleanup_level}")
    print(f"Dry Run:       {config.dry_run}")
    if config.dry_run:
        print("\n*** DRY RUN MODE - No changes will be made ***")
    print("="*60)
    
    # Discover objects
    discovered = discover_all_objects(spark, config)
    
    print("\nDiscovered Objects:")
    print("-" * 60)
    total_tables = 0
    for schema, tables in discovered.items():
        print(f"\n{config.catalog}.{schema}:")
        if tables:
            for table in sorted(tables):
                row_count = get_table_row_count(spark, config.catalog, schema, table)
                count_str = f"{row_count:,} rows" if row_count >= 0 else "unknown"
                print(f"  - {table} ({count_str})")
                total_tables += 1
        else:
            print("  (no tables or schema does not exist)")
    
    print(f"\nTotal tables found: {total_tables}")
    
    # Execute cleanup
    results = run_cleanup(spark, config, discovered)
    
    # Print summary
    print_cleanup_summary(config, results)
    
    # Print manual commands
    if not config.dry_run:
        print_manual_cleanup_commands(config, discovered)
    
    return results


# Entry point for running as a script
if __name__ == "__main__":
    # When running as a script in Databricks, spark is available globally
    # Default to dry_run=True for safety
    results = cleanup(spark, dry_run=True)
