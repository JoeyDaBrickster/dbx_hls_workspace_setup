"""
HCPCS Reference Data Ingestion

Loads HCPCS (Healthcare Common Procedure Coding System) data from CMS 
fixed-width files into Unity Catalog Delta tables.

Source Files:
- HCPC2026_JAN_ANWEB_*.txt - Main HCPCS code file (fixed-width, 320 chars/record)

Output Tables:
- lookup_hcpcs_codes - HCPCS codes with descriptions and pricing indicators
"""

import time
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from .reference_config import (
    ReferenceConfig, ReferenceLoadResult, pipe,
    add_reference_audit_columns, get_full_table_name, write_reference_table
)


# HCPCS file layout - fixed-width positions
# Default pattern - file is directly in the hcpcs volume
HCPCS_FILE_PATTERN = "HCPC*.txt"


def parse_hcpcs_fixed_width(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Parse HCPCS fixed-width file (320 chars/record) into a DataFrame.
    
    Field positions based on CMS HCPCS Contractor Record Layout:
    - 1-5: HCPCS Code (5 chars)
    - 6-10: Sequence Number (5 digits)
    - 11: Record Type (1 char: 3=procedure, 4=continuation, 7=modifier, 8=modifier cont.)
    - 12-91: Long Description (80 chars)
    - 92-119: Short Description (28 chars)
    - 120-127: Pricing Indicators (4 x 2 chars)
    - 128: Multiple Pricing Indicator (1 char)
    - 129-146: CIM References (3 x 6 chars)
    - 147-170: MCM References (3 x 8 chars)
    - 171-180: Statute Number (10 chars)
    - 181-204: Lab Certification Codes (8 x 3 chars)
    - 205-229: Cross Reference Codes (5 x 5 chars)
    - 230: Coverage Code (1 char)
    - 231-232: ASC Payment Group (2 chars)
    - 233-240: ASC Effective Date (8 digits)
    - 241-243: MOG Payment Group (3 chars)
    - 244: MOG Payment Indicator (1 char)
    - 245-252: MOG Effective Date (8 digits)
    - 253-256: Processing Note Number (4 chars)
    - 257-259: BETOS Code (3 chars)
    - 260: Filler (1 char)
    - 261-265: Type of Service Codes (5 x 1 char)
    - 266-268: Anesthesia Base Units (3 digits)
    - 269-276: Code Added Date (8 digits)
    - 277-284: Action Effective Date (8 digits)
    - 285-292: Termination Date (8 digits)
    - 293: Action Code (1 char)
    """
    raw_df = spark.read.text(file_path)
    
    # All columns as STRING for bronze-level ingestion
    parsed_df = (
        raw_df
        .select(
            # Core identifiers
            F.trim(F.substring(F.col("value"), 1, 5)).alias("hcpcs_code"),
            F.trim(F.substring(F.col("value"), 6, 5)).alias("sequence_num"),
            F.trim(F.substring(F.col("value"), 11, 1)).alias("record_type"),
            
            # Descriptions
            F.trim(F.substring(F.col("value"), 12, 80)).alias("long_description"),
            F.trim(F.substring(F.col("value"), 92, 28)).alias("short_description"),
            
            # Pricing indicators
            F.trim(F.substring(F.col("value"), 120, 2)).alias("pricing_indicator_1"),
            F.trim(F.substring(F.col("value"), 122, 2)).alias("pricing_indicator_2"),
            F.trim(F.substring(F.col("value"), 124, 2)).alias("pricing_indicator_3"),
            F.trim(F.substring(F.col("value"), 126, 2)).alias("pricing_indicator_4"),
            F.trim(F.substring(F.col("value"), 128, 1)).alias("multiple_pricing_indicator"),
            
            # Manual references
            F.trim(F.substring(F.col("value"), 129, 6)).alias("cim_reference_1"),
            F.trim(F.substring(F.col("value"), 135, 6)).alias("cim_reference_2"),
            F.trim(F.substring(F.col("value"), 141, 6)).alias("cim_reference_3"),
            F.trim(F.substring(F.col("value"), 147, 8)).alias("mcm_reference_1"),
            F.trim(F.substring(F.col("value"), 155, 8)).alias("mcm_reference_2"),
            F.trim(F.substring(F.col("value"), 163, 8)).alias("mcm_reference_3"),
            F.trim(F.substring(F.col("value"), 171, 10)).alias("statute_number"),
            
            # Lab certification codes
            F.trim(F.substring(F.col("value"), 181, 3)).alias("lab_cert_code_1"),
            F.trim(F.substring(F.col("value"), 184, 3)).alias("lab_cert_code_2"),
            F.trim(F.substring(F.col("value"), 187, 3)).alias("lab_cert_code_3"),
            F.trim(F.substring(F.col("value"), 190, 3)).alias("lab_cert_code_4"),
            F.trim(F.substring(F.col("value"), 193, 3)).alias("lab_cert_code_5"),
            F.trim(F.substring(F.col("value"), 196, 3)).alias("lab_cert_code_6"),
            F.trim(F.substring(F.col("value"), 199, 3)).alias("lab_cert_code_7"),
            F.trim(F.substring(F.col("value"), 202, 3)).alias("lab_cert_code_8"),
            
            # Cross reference codes
            F.trim(F.substring(F.col("value"), 205, 5)).alias("xref_code_1"),
            F.trim(F.substring(F.col("value"), 210, 5)).alias("xref_code_2"),
            F.trim(F.substring(F.col("value"), 215, 5)).alias("xref_code_3"),
            F.trim(F.substring(F.col("value"), 220, 5)).alias("xref_code_4"),
            F.trim(F.substring(F.col("value"), 225, 5)).alias("xref_code_5"),
            
            # Coverage and ASC
            F.trim(F.substring(F.col("value"), 230, 1)).alias("coverage_code"),
            F.trim(F.substring(F.col("value"), 231, 2)).alias("asc_payment_group"),
            F.trim(F.substring(F.col("value"), 233, 8)).alias("asc_effective_date"),
            
            # MOG
            F.trim(F.substring(F.col("value"), 241, 3)).alias("mog_payment_group"),
            F.trim(F.substring(F.col("value"), 244, 1)).alias("mog_payment_indicator"),
            F.trim(F.substring(F.col("value"), 245, 8)).alias("mog_effective_date"),
            
            # Processing and classification
            F.trim(F.substring(F.col("value"), 253, 4)).alias("processing_note_number"),
            F.trim(F.substring(F.col("value"), 257, 3)).alias("betos_code"),
            F.trim(F.substring(F.col("value"), 261, 1)).alias("type_of_service_1"),
            F.trim(F.substring(F.col("value"), 262, 1)).alias("type_of_service_2"),
            F.trim(F.substring(F.col("value"), 263, 1)).alias("type_of_service_3"),
            F.trim(F.substring(F.col("value"), 264, 1)).alias("type_of_service_4"),
            F.trim(F.substring(F.col("value"), 265, 1)).alias("type_of_service_5"),
            
            # Anesthesia and dates (all as STRING)
            F.trim(F.substring(F.col("value"), 266, 3)).alias("anesthesia_base_units"),
            F.trim(F.substring(F.col("value"), 269, 8)).alias("code_added_date"),
            F.trim(F.substring(F.col("value"), 277, 8)).alias("action_effective_date"),
            F.trim(F.substring(F.col("value"), 285, 8)).alias("termination_date"),
            F.trim(F.substring(F.col("value"), 293, 1)).alias("action_code")
        )
    )
    
    return parsed_df


def transform_hcpcs(df: DataFrame) -> DataFrame:
    """Apply business transformations to HCPCS data."""
    return (
        df
        # Filter to first-line records (contains detail info)
        .filter(F.col("record_type").isin("3", "7"))
        .withColumn(
            "code_type",
            F.when(F.col("record_type") == "3", "PROCEDURE")
            .when(F.col("record_type") == "7", "MODIFIER")
            .otherwise("UNKNOWN")
        )
        .withColumn(
            "code_category",
            F.when(F.col("hcpcs_code").rlike("^[0-9]"), "CPT")
            .when(F.col("hcpcs_code").rlike("^[A-Z]"), "HCPCS_LEVEL_II")
            .otherwise("OTHER")
        )
        .withColumn(
            # Compare as strings (YYYYMMDD format)
            "is_active",
            F.col("termination_date").isNull() | 
            (F.trim(F.col("termination_date")) == "") |
            (F.col("termination_date") > F.date_format(F.current_date(), "yyyyMMdd"))
        )
    )


def load_hcpcs(
    spark: SparkSession,
    config: ReferenceConfig,
    hcpcs_file: Optional[str] = None
) -> ReferenceLoadResult:
    """
    Load HCPCS reference data.
    
    Args:
        spark: SparkSession
        config: Reference configuration
        hcpcs_file: Optional specific file path (uses glob pattern if not provided)
    
    Returns:
        ReferenceLoadResult with load statistics
    """
    start_time = time.time()
    table_name = "lookup_hcpcs_codes"
    
    try:
        file_path = hcpcs_file or f"{config.hcpcs_volume}/{HCPCS_FILE_PATTERN}"
        
        # Parse and transform
        df = pipe(
            parse_hcpcs_fixed_width(spark, file_path),
            transform_hcpcs,
            add_reference_audit_columns("HCPCS")
        )
        
        row_count = df.count()
        full_table_name = write_reference_table(spark, config, table_name, df)
        
        elapsed = time.time() - start_time
        return ReferenceLoadResult(table_name, full_table_name, row_count, round(elapsed, 2), True)
        
    except Exception as e:
        elapsed = time.time() - start_time
        return ReferenceLoadResult(table_name, "", 0, round(elapsed, 2), False, str(e))


def create_hcpcs_views(spark: SparkSession, config: ReferenceConfig) -> None:
    """Create useful HCPCS views."""
    schema_path = config.full_schema_path
    
    # Active HCPCS codes
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_hcpcs_active AS
    SELECT *
    FROM {schema_path}.lookup_hcpcs_codes
    WHERE is_active = true
    """)
    
    # HCPCS by BETOS category
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_hcpcs_by_betos AS
    SELECT 
        betos_code,
        COUNT(*) as code_count,
        COLLECT_SET(hcpcs_code) as codes
    FROM {schema_path}.lookup_hcpcs_codes
    WHERE betos_code IS NOT NULL AND betos_code != ''
    GROUP BY betos_code
    ORDER BY betos_code
    """)
    
    # HCPCS pricing summary
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_hcpcs_pricing_summary AS
    SELECT 
        pricing_indicator_1 as pricing_indicator,
        coverage_code,
        COUNT(*) as code_count
    FROM {schema_path}.lookup_hcpcs_codes
    WHERE pricing_indicator_1 IS NOT NULL AND pricing_indicator_1 != ''
    GROUP BY pricing_indicator_1, coverage_code
    ORDER BY code_count DESC
    """)
    
    print(f"Created HCPCS views in {schema_path}")
