"""
Pipeline Configuration

Shared configuration and utility functions for the Synthea Healthcare Data Pipeline.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Callable, Any, TypeVar, Tuple
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Type variable for generic functions
T = TypeVar('T')


@dataclass(frozen=True)
class PipelineConfig:
    """Immutable configuration for the data pipeline."""
    catalog: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    volume_source: str
    source_system: str
    
    @property
    def bronze_full_path(self) -> str:
        return f"{self.catalog}.{self.bronze_schema}"
    
    @property
    def silver_full_path(self) -> str:
        return f"{self.catalog}.{self.silver_schema}"
    
    @property
    def gold_full_path(self) -> str:
        return f"{self.catalog}.{self.gold_schema}"


@dataclass(frozen=True)
class TableDefinition:
    """Definition of a source table."""
    name: str
    csv_file: str
    primary_key: Optional[str]
    category: str


# All 18 Synthea tables
TABLE_DEFINITIONS: Tuple[TableDefinition, ...] = (
    TableDefinition("patients", "patients.csv", "Id", "clinical"),
    TableDefinition("encounters", "encounters.csv", "Id", "clinical"),
    TableDefinition("conditions", "conditions.csv", None, "clinical"),
    TableDefinition("observations", "observations.csv", None, "clinical"),
    TableDefinition("procedures", "procedures.csv", None, "clinical"),
    TableDefinition("medications", "medications.csv", None, "clinical"),
    TableDefinition("immunizations", "immunizations.csv", None, "clinical"),
    TableDefinition("allergies", "allergies.csv", None, "clinical"),
    TableDefinition("careplans", "careplans.csv", "Id", "clinical"),
    TableDefinition("devices", "devices.csv", None, "clinical"),
    TableDefinition("imaging_studies", "imaging_studies.csv", None, "clinical"),
    TableDefinition("supplies", "supplies.csv", None, "clinical"),
    TableDefinition("organizations", "organizations.csv", "Id", "administrative"),
    TableDefinition("providers", "providers.csv", "Id", "administrative"),
    TableDefinition("payers", "payers.csv", "Id", "administrative"),
    TableDefinition("claims", "claims.csv", "Id", "financial"),
    TableDefinition("claims_transactions", "claims_transactions.csv", "Id", "financial"),
    TableDefinition("payer_transitions", "payer_transitions.csv", None, "financial"),
)

BRONZE_TABLE_PROPERTIES = {
    "quality": "bronze",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true"
}

SILVER_TABLE_PROPERTIES = {
    "quality": "silver",
    "delta.enableChangeDataFeed": "true",
    "delta.autoOptimize.optimizeWrite": "true"
}

GOLD_TABLE_PROPERTIES = {
    "quality": "gold",
    "delta.enableChangeDataFeed": "true",
    "delta.autoOptimize.optimizeWrite": "true"
}


def create_config_from_widgets(dbutils) -> PipelineConfig:
    """Create config from Databricks widgets."""
    return PipelineConfig(
        catalog=dbutils.widgets.get("catalog"),
        bronze_schema=dbutils.widgets.get("bronze_schema"),
        silver_schema=dbutils.widgets.get("silver_schema"),
        gold_schema=dbutils.widgets.get("gold_schema"),
        volume_source=dbutils.widgets.get("volume_source"),
        source_system=dbutils.widgets.get("source_system")
    )


def create_config(
    catalog: str = "healthcare_dev",
    bronze_schema: str = "bronze",
    silver_schema: str = "silver",
    gold_schema: str = "gold",
    volume_source: str = "/Volumes/hls_bronze/hls_bronze/raw/syntheticData",
    source_system: str = "SYNTHEA"
) -> PipelineConfig:
    """Create config with explicit parameters."""
    return PipelineConfig(
        catalog=catalog,
        bronze_schema=bronze_schema,
        silver_schema=silver_schema,
        gold_schema=gold_schema,
        volume_source=volume_source,
        source_system=source_system
    )


def setup_widgets(dbutils) -> None:
    """Setup Databricks widgets with default values."""
    dbutils.widgets.text("catalog", "healthcare_dev", "Catalog Name")
    dbutils.widgets.text("bronze_schema", "bronze", "Bronze Schema")
    dbutils.widgets.text("silver_schema", "silver", "Silver Schema")
    dbutils.widgets.text("gold_schema", "gold", "Gold Schema")
    dbutils.widgets.text("volume_source", "/Volumes/hls_bronze/hls_bronze/raw/syntheticData", "Volume Source Path")
    dbutils.widgets.text("source_system", "SYNTHEA", "Source System Identifier")


# Functional utilities
def pipe(initial: T, *functions: Callable[[T], T]) -> T:
    """Pipe a value through multiple functions left-to-right."""
    return reduce(lambda acc, f: f(acc), functions, initial)


def compose(*functions: Callable[[T], T]) -> Callable[[T], T]:
    """Compose multiple functions right-to-left."""
    def composed(x: T) -> T:
        return reduce(lambda acc, f: f(acc), reversed(functions), x)
    return composed


# DataFrame transformation helpers
def add_audit_columns(source_system: str, table_name: str) -> Callable[[DataFrame], DataFrame]:
    """Add audit columns to a DataFrame."""
    def transform(df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("_load_dts", F.current_timestamp())
            .withColumn("_file_name", F.col("_metadata.file_path"))
            .withColumn("_rec_src", F.lit(f"{source_system}.{table_name.upper()}"))
        )
    return transform


def add_state_column() -> Callable[[DataFrame], DataFrame]:
    """Extract state from file path."""
    def transform(df: DataFrame) -> DataFrame:
        return df.withColumn(
            "_state",
            F.regexp_extract(F.col("_metadata.file_path"), r"/([^/]+)/output/csv/", 1)
        )
    return transform


def standardize_column_names() -> Callable[[DataFrame], DataFrame]:
    """Standardize column names (spaces to underscores)."""
    def transform(df: DataFrame) -> DataFrame:
        for col_name in df.columns:
            new_name = col_name.replace(" ", "_").replace("-", "_")
            if new_name != col_name:
                df = df.withColumnRenamed(col_name, new_name)
        return df
    return transform


# Path helpers
def get_csv_path(config: PipelineConfig, state: str, csv_file: str) -> str:
    """Get full path to a CSV file for a given state."""
    return f"{config.volume_source}/{state}/output/csv/{csv_file}"


def get_all_csv_paths(config: PipelineConfig, csv_file: str) -> str:
    """Get glob pattern for a CSV file across all states."""
    return f"{config.volume_source}/*/output/csv/{csv_file}"


def get_full_table_name(config: PipelineConfig, layer: str, table_name: str) -> str:
    """Get fully qualified table name."""
    schema_map = {
        "bronze": config.bronze_schema,
        "silver": config.silver_schema,
        "gold": config.gold_schema
    }
    schema = schema_map.get(layer, layer)
    return f"{config.catalog}.{schema}.{table_name}"


# Setup functions
def discover_state_directories(dbutils, volume_path: str) -> List[str]:
    """Discover all state directories in the volume."""
    try:
        entries = dbutils.fs.ls(volume_path)
        states = [
            entry.name.rstrip('/') 
            for entry in entries 
            if entry.isDir() and not entry.name.startswith('.')
        ]
        return sorted(states)
    except Exception as e:
        print(f"Error discovering states: {e}")
        return []


def setup_catalog_and_schemas(spark: SparkSession, config: PipelineConfig) -> None:
    """Create catalog and schemas if they don't exist."""
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {config.catalog}")
    spark.sql(f"USE CATALOG {config.catalog}")
    
    for schema in [config.bronze_schema, config.silver_schema, config.gold_schema]:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    
    print(f"Catalog and schemas ready: {config.catalog}.{{{config.bronze_schema}, {config.silver_schema}, {config.gold_schema}}}")


def get_tables_by_category(category: str) -> Tuple[TableDefinition, ...]:
    """Filter tables by category."""
    return tuple(t for t in TABLE_DEFINITIONS if t.category == category)


def print_config_summary(config: PipelineConfig, states_count: int = 0) -> None:
    """Print configuration summary."""
    print("\n" + "="*60)
    print("PIPELINE CONFIGURATION SUMMARY")
    print("="*60)
    print(f"Catalog:        {config.catalog}")
    print(f"Bronze Schema:  {config.bronze_full_path}")
    print(f"Silver Schema:  {config.silver_full_path}")
    print(f"Gold Schema:    {config.gold_full_path}")
    print(f"Volume Source:  {config.volume_source}")
    print(f"Source System:  {config.source_system}")
    print(f"States Found:   {states_count}")
    print("="*60)
