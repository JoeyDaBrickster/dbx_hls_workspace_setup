"""
Reference Data Configuration

Configuration and utilities for loading healthcare reference data 
(HCPCS, ICD-10-CM, NDC, SNOMED, RxNorm) into Unity Catalog.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Callable, TypeVar
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


T = TypeVar('T')


@dataclass(frozen=True)
class ReferenceConfig:
    """Immutable configuration for reference data loading."""
    catalog: str
    schema: str
    
    # Volume paths for each reference data source
    hcpcs_volume: str
    icd10cm_volume: str
    ndc_volume: str
    snomed_icd10cm_volume: str
    rxnorm_volume: str
    rxnorm_prescribe_volume: str
    
    source_system: str = "REFERENCE"
    
    @property
    def full_schema_path(self) -> str:
        return f"{self.catalog}.{self.schema}"


@dataclass
class ReferenceLoadResult:
    """Result of a reference data load operation."""
    table_name: str
    full_table_name: str
    row_count: int
    elapsed_seconds: float
    success: bool
    error: Optional[str] = None


REFERENCE_TABLE_PROPERTIES = {
    "quality": "reference",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true"
}


def create_reference_config(
    catalog: str = "healthcare_dev",
    schema: str = "bronze",
    hcpcs_volume: str = "/Volumes/healthcare_dev/landing/reference/hcpcs",
    icd10cm_volume: str = "/Volumes/healthcare_dev/landing/reference/icd10cm",
    ndc_volume: str = "/Volumes/healthcare_dev/landing/reference/ndc",
    snomed_icd10cm_volume: str = "/Volumes/healthcare_dev/landing/reference/snomed_icd10cm",
    rxnorm_volume: str = "/Volumes/healthcare_dev/landing/reference/rxnorm",
    rxnorm_prescribe_volume: str = "/Volumes/healthcare_dev/landing/reference/rxnorm_prescribe"
) -> ReferenceConfig:
    """Create reference config with explicit parameters."""
    return ReferenceConfig(
        catalog=catalog,
        schema=schema,
        hcpcs_volume=hcpcs_volume,
        icd10cm_volume=icd10cm_volume,
        ndc_volume=ndc_volume,
        snomed_icd10cm_volume=snomed_icd10cm_volume,
        rxnorm_volume=rxnorm_volume,
        rxnorm_prescribe_volume=rxnorm_prescribe_volume
    )


def create_reference_config_from_widgets(dbutils) -> ReferenceConfig:
    """Create reference config from Databricks widgets."""
    return ReferenceConfig(
        catalog=dbutils.widgets.get("catalog"),
        schema=dbutils.widgets.get("bronze_schema"),
        hcpcs_volume=dbutils.widgets.get("hcpcs_volume"),
        icd10cm_volume=dbutils.widgets.get("icd10cm_volume"),
        ndc_volume=dbutils.widgets.get("ndc_volume"),
        snomed_icd10cm_volume=dbutils.widgets.get("snomed_icd10cm_volume"),
        rxnorm_volume=dbutils.widgets.get("rxnorm_volume"),
        rxnorm_prescribe_volume=dbutils.widgets.get("rxnorm_prescribe_volume")
    )


def setup_reference_widgets(dbutils) -> None:
    """Setup Databricks widgets with default values for reference data."""
    dbutils.widgets.text("catalog", "healthcare_dev", "Catalog Name")
    dbutils.widgets.text("bronze_schema", "bronze", "Bronze Schema")
    dbutils.widgets.text("hcpcs_volume", "/Volumes/healthcare_dev/landing/reference/hcpcs", "HCPCS Volume Path")
    dbutils.widgets.text("icd10cm_volume", "/Volumes/healthcare_dev/landing/reference/icd10cm", "ICD-10-CM Volume Path")
    dbutils.widgets.text("ndc_volume", "/Volumes/healthcare_dev/landing/reference/ndc", "NDC Volume Path")
    dbutils.widgets.text("snomed_icd10cm_volume", "/Volumes/healthcare_dev/landing/reference/snomed_icd10cm", "SNOMED-ICD10CM Volume Path")
    dbutils.widgets.text("rxnorm_volume", "/Volumes/healthcare_dev/landing/reference/rxnorm", "RxNorm Volume Path")
    dbutils.widgets.text("rxnorm_prescribe_volume", "/Volumes/healthcare_dev/landing/reference/rxnorm_prescribe", "RxNorm Prescribe Volume Path")


# Functional utilities
def pipe(initial: T, *functions: Callable[[T], T]) -> T:
    """Pipe a value through multiple functions left-to-right."""
    return reduce(lambda acc, f: f(acc), functions, initial)


# DataFrame transformation helpers
def add_reference_audit_columns(source_name: str) -> Callable[[DataFrame], DataFrame]:
    """Add audit columns for reference data."""
    def transform(df: DataFrame) -> DataFrame:
        return (
            df
            .withColumn("_load_dts", F.current_timestamp())
            .withColumn("_rec_src", F.lit(f"REFERENCE.{source_name.upper()}"))
        )
    return transform


def standardize_column_names() -> Callable[[DataFrame], DataFrame]:
    """Standardize column names to lowercase with underscores."""
    def transform(df: DataFrame) -> DataFrame:
        for col_name in df.columns:
            new_name = col_name.lower().replace(" ", "_").replace("-", "_")
            if new_name != col_name:
                df = df.withColumnRenamed(col_name, new_name)
        return df
    return transform


def get_full_table_name(config: ReferenceConfig, table_name: str) -> str:
    """Get fully qualified table name for reference data."""
    return f"{config.catalog}.{config.schema}.{table_name}"


def setup_reference_schema(spark: SparkSession, config: ReferenceConfig) -> None:
    """Ensure catalog and bronze schema exist for lookup tables."""
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {config.catalog}")
    spark.sql(f"USE CATALOG {config.catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.schema}")
    print(f"Bronze schema ready for lookup tables: {config.full_schema_path}")


def write_reference_table(
    spark: SparkSession,
    config: ReferenceConfig,
    table_name: str,
    df: DataFrame,
    mode: str = "overwrite"
) -> str:
    """Write DataFrame to a reference Delta table."""
    full_table_name = get_full_table_name(config, table_name)
    
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .option("delta.autoOptimize.optimizeWrite", "true")
        .saveAsTable(full_table_name)
    )
    
    return full_table_name


def print_reference_config_summary(config: ReferenceConfig) -> None:
    """Print reference configuration summary."""
    print("\n" + "="*70)
    print("LOOKUP DATA CONFIGURATION SUMMARY")
    print("="*70)
    print(f"Catalog:              {config.catalog}")
    print(f"Bronze Schema:        {config.full_schema_path}")
    print(f"Table Prefix:         lookup_")
    print(f"HCPCS Volume:         {config.hcpcs_volume}")
    print(f"ICD-10-CM Volume:     {config.icd10cm_volume}")
    print(f"NDC Volume:           {config.ndc_volume}")
    print(f"SNOMED-ICD10CM Vol:   {config.snomed_icd10cm_volume}")
    print(f"RxNorm Volume:        {config.rxnorm_volume}")
    print(f"RxNorm Prescribe Vol: {config.rxnorm_prescribe_volume}")
    print("="*70)
