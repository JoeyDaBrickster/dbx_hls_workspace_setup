"""
SNOMED CT to ICD-10-CM Crosswalk Ingestion

Loads the official NLM SNOMED CT to ICD-10-CM mapping file into Unity Catalog.

Source Files:
- tls_Icd10cmHumanReadableMap_US1000124_*.tsv - Human-readable SNOMED to ICD-10-CM map

Output Tables:
- lookup_snomed_icd10cm_map - Complete mapping with rules and advice
"""

import time
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window

from .reference_config import (
    ReferenceConfig, ReferenceLoadResult, pipe,
    add_reference_audit_columns, write_reference_table
)


# SNOMED map file pattern
SNOMED_MAP_FILE_PATTERN = "tls_Icd10cmHumanReadableMap_US1000124_*.tsv"

# Schema for SNOMED to ICD-10-CM map
SNOMED_MAP_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("effective_time", StringType(), True),
    StructField("active", StringType(), True),
    StructField("module_id", StringType(), True),
    StructField("refset_id", StringType(), True),
    StructField("snomed_concept_id", StringType(), True),
    StructField("snomed_concept_name", StringType(), True),
    StructField("map_group", StringType(), True),
    StructField("map_priority", StringType(), True),
    StructField("map_rule", StringType(), True),
    StructField("map_advice", StringType(), True),
    StructField("icd10cm_code", StringType(), True),
    StructField("icd10cm_name", StringType(), True),
    StructField("correlation_id", StringType(), True),
    StructField("map_category_id", StringType(), True),
    StructField("map_category_name", StringType(), True)
])


def load_snomed_map(spark: SparkSession, file_path: str) -> DataFrame:
    """Load and transform SNOMED to ICD-10-CM mapping file. All columns remain STRING."""
    df = (
        spark.read
        .option("header", "true")
        .option("delimiter", "\t")
        .option("quote", '"')
        .schema(SNOMED_MAP_SCHEMA)
        .csv(file_path)
    )
    
    return (
        df
        # String-based boolean flags
        .withColumn("is_active", F.when(F.col("active") == "1", "true").otherwise("false"))
        # Keep map_group and map_priority as strings
        # Format effective_date as YYYY-MM-DD string (not date type)
        .withColumn(
            "effective_date",
            F.when(
                F.col("effective_time").isNotNull() & (F.trim(F.col("effective_time")) != ""),
                F.concat(
                    F.substring(F.col("effective_time"), 1, 4), F.lit("-"),
                    F.substring(F.col("effective_time"), 5, 2), F.lit("-"),
                    F.substring(F.col("effective_time"), 7, 2)
                )
            )
        )
        .withColumn(
            "has_target",
            F.when(F.col("icd10cm_code").isNotNull() & (F.trim(F.col("icd10cm_code")) != ""), "true").otherwise("false")
        )
        .withColumn(
            "map_type",
            F.when(F.col("map_category_name") == "Properly Classified", "EXACT")
            .when(F.col("map_category_name") == "Context Dependent", "CONTEXT_DEPENDENT")
            .when(F.col("map_category_name") == "Cannot Be Classified", "NO_MAP")
            .otherwise("OTHER")
        )
        .withColumn(
            "requires_additional_code",
            F.when(
                F.col("map_advice").contains("CONSIDER ADDITIONAL CODE") | 
                F.col("map_advice").contains("MAP IS CONTEXT DEPENDENT"),
                "true"
            ).otherwise("false")
        )
        .withColumn(
            "is_approximate",
            F.when(F.col("map_advice").contains("POSSIBLE REQUIREMENT FOR ADDITIONAL CODE"), "true").otherwise("false")
        )
        .withColumn(
            "icd10cm_code_clean",
            F.when(F.col("icd10cm_code").isNotNull() & (F.trim(F.col("icd10cm_code")) != ""), F.trim(F.col("icd10cm_code"))).otherwise(F.lit(None))
        )
    )


def load_snomed_icd10cm(
    spark: SparkSession,
    config: ReferenceConfig,
    map_file: Optional[str] = None
) -> ReferenceLoadResult:
    """
    Load SNOMED to ICD-10-CM crosswalk.
    
    Args:
        spark: SparkSession
        config: Reference configuration
        map_file: Optional specific file path
    
    Returns:
        ReferenceLoadResult with load statistics
    """
    start_time = time.time()
    table_name = "lookup_snomed_icd10cm_map"
    
    try:
        file_path = map_file or f"{config.snomed_icd10cm_volume}/{SNOMED_MAP_FILE_PATTERN}"
        
        # Parse and transform
        df = pipe(
            load_snomed_map(spark, file_path),
            add_reference_audit_columns("SNOMED_ICD10CM")
        )
        
        # Filter to active records and select final columns
        df_final = (
            df
            .filter(F.col("is_active") == "true")
            .select(
                "id",
                "effective_date",
                "snomed_concept_id",
                "snomed_concept_name",
                "map_group",
                "map_priority",
                "map_rule",
                "map_advice",
                "icd10cm_code_clean",
                F.col("icd10cm_code_clean").alias("icd10cm_code"),
                "icd10cm_name",
                "map_category_name",
                "map_type",
                "has_target",
                "requires_additional_code",
                "is_approximate",
                "_load_dts",
                "_rec_src"
            )
        )
        
        row_count = df_final.count()
        full_table_name = write_reference_table(spark, config, table_name, df_final)
        
        elapsed = time.time() - start_time
        return ReferenceLoadResult(table_name, full_table_name, row_count, round(elapsed, 2), True)
        
    except Exception as e:
        elapsed = time.time() - start_time
        return ReferenceLoadResult(table_name, "", 0, round(elapsed, 2), False, str(e))


def create_snomed_icd10cm_views(spark: SparkSession, config: ReferenceConfig) -> None:
    """Create useful SNOMED to ICD-10-CM crosswalk views."""
    schema_path = config.full_schema_path
    
    # Simple 1:1 mappings (first priority from first group)
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_snomed_icd10cm_simple AS
    WITH ranked AS (
      SELECT 
        snomed_concept_id,
        snomed_concept_name,
        icd10cm_code,
        icd10cm_name,
        map_category_name,
        map_type,
        ROW_NUMBER() OVER (
          PARTITION BY snomed_concept_id 
          ORDER BY CAST(map_group AS INT) ASC, CAST(map_priority AS INT) ASC
        ) as rn
      FROM {schema_path}.lookup_snomed_icd10cm_map
      WHERE has_target = 'true' 
        AND map_type IN ('EXACT', 'CONTEXT_DEPENDENT')
    )
    SELECT 
      snomed_concept_id,
      snomed_concept_name,
      icd10cm_code,
      icd10cm_name,
      map_category_name,
      map_type
    FROM ranked
    WHERE rn = 1
    """)
    
    # All mappings for a SNOMED concept
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_snomed_icd10cm_all AS
    SELECT 
      snomed_concept_id,
      snomed_concept_name,
      COLLECT_SET(icd10cm_code) as icd10cm_codes,
      COUNT(DISTINCT icd10cm_code) as icd10cm_code_count,
      FIRST(map_category_name) as primary_map_category
    FROM {schema_path}.lookup_snomed_icd10cm_map
    WHERE has_target = 'true'
    GROUP BY snomed_concept_id, snomed_concept_name
    """)
    
    # ICD-10-CM to SNOMED reverse lookup
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_icd10cm_snomed_reverse AS
    SELECT 
      icd10cm_code,
      icd10cm_name,
      COLLECT_SET(snomed_concept_id) as snomed_concept_ids,
      COLLECT_SET(snomed_concept_name) as snomed_concept_names,
      COUNT(DISTINCT snomed_concept_id) as snomed_concept_count
    FROM {schema_path}.lookup_snomed_icd10cm_map
    WHERE has_target = 'true'
    GROUP BY icd10cm_code, icd10cm_name
    """)
    
    # Mapping complexity analysis
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_snomed_mapping_complexity AS
    SELECT 
      snomed_concept_id,
      snomed_concept_name,
      COUNT(*) as total_mappings,
      COUNT(DISTINCT map_group) as map_groups,
      MAX(CAST(map_priority AS INT)) as max_priority,
      SUM(CASE WHEN map_type = 'EXACT' THEN 1 ELSE 0 END) as exact_mappings,
      SUM(CASE WHEN map_type = 'CONTEXT_DEPENDENT' THEN 1 ELSE 0 END) as context_dependent_mappings,
      SUM(CASE WHEN requires_additional_code = 'true' THEN 1 ELSE 0 END) as mappings_needing_additional_code,
      CASE 
        WHEN COUNT(*) = 1 AND MAX(map_type) = 'EXACT' THEN 'SIMPLE_1_TO_1'
        WHEN COUNT(DISTINCT map_group) > 1 THEN 'COMPLEX_MULTI_GROUP'
        WHEN MAX(map_type) = 'CONTEXT_DEPENDENT' THEN 'CONTEXT_DEPENDENT'
        ELSE 'MULTIPLE_OPTIONS'
      END as mapping_complexity
    FROM {schema_path}.lookup_snomed_icd10cm_map
    WHERE has_target = 'true'
    GROUP BY snomed_concept_id, snomed_concept_name
    """)
    
    print(f"Created SNOMED-ICD10CM views in {schema_path}")
