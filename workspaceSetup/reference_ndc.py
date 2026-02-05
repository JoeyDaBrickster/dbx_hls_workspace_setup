"""
NDC (National Drug Code) Reference Data Ingestion

Loads FDA NDC Directory data from tab-delimited files into Unity Catalog.

Source Files:
- product.txt - Product-level drug information
- package.txt - Package-level NDC codes

Output Tables:
- lookup_ndc_product - Product-level information
- lookup_ndc_package - Package-level NDC codes
"""

import time
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from .reference_config import (
    ReferenceConfig, ReferenceLoadResult, pipe,
    add_reference_audit_columns, write_reference_table, get_full_table_name
)


# NDC file paths (relative to volume)
# Files are directly in the ndc volume
NDC_PRODUCT_FILE = "product.txt"
NDC_PACKAGE_FILE = "package.txt"

# Product schema
PRODUCT_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_ndc", StringType(), True),
    StructField("product_type_name", StringType(), True),
    StructField("proprietary_name", StringType(), True),
    StructField("proprietary_name_suffix", StringType(), True),
    StructField("nonproprietary_name", StringType(), True),
    StructField("dosage_form_name", StringType(), True),
    StructField("route_name", StringType(), True),
    StructField("start_marketing_date", StringType(), True),
    StructField("end_marketing_date", StringType(), True),
    StructField("marketing_category_name", StringType(), True),
    StructField("application_number", StringType(), True),
    StructField("labeler_name", StringType(), True),
    StructField("substance_name", StringType(), True),
    StructField("active_numerator_strength", StringType(), True),
    StructField("active_ingred_unit", StringType(), True),
    StructField("pharm_classes", StringType(), True),
    StructField("dea_schedule", StringType(), True),
    StructField("ndc_exclude_flag", StringType(), True),
    StructField("listing_record_certified_through", StringType(), True)
])

# Package schema
PACKAGE_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_ndc", StringType(), True),
    StructField("ndc_package_code", StringType(), True),
    StructField("package_description", StringType(), True),
    StructField("start_marketing_date", StringType(), True),
    StructField("end_marketing_date", StringType(), True),
    StructField("ndc_exclude_flag", StringType(), True),
    StructField("sample_package", StringType(), True)
])


def normalize_ndc_to_11digit(ndc_col):
    """
    Convert NDC formats to 11-digit HIPAA format.
    
    NDC format conversion rules:
    - 4-4-2 format -> pad labeler with leading zero
    - 5-3-2 format -> pad product with leading zero  
    - 5-4-1 format -> pad package with leading zero
    
    Output: 11-digit no-dash format
    """
    return (
        F.when(
            # 4-4-2 format
            F.regexp_replace(ndc_col, "^(\\d{4})-(\\d{4})-(\\d{2})$", "0$1$2$3").rlike("^\\d{11}$"),
            F.regexp_replace(ndc_col, "^(\\d{4})-(\\d{4})-(\\d{2})$", "0$1$2$3")
        ).when(
            # 5-3-2 format
            F.regexp_replace(ndc_col, "^(\\d{5})-(\\d{3})-(\\d{2})$", "$10$2$3").rlike("^\\d{11}$"),
            F.regexp_replace(ndc_col, "^(\\d{5})-(\\d{3})-(\\d{2})$", "$10$2$3")
        ).when(
            # 5-4-1 format
            F.regexp_replace(ndc_col, "^(\\d{5})-(\\d{4})-(\\d{1})$", "$1$20$3").rlike("^\\d{11}$"),
            F.regexp_replace(ndc_col, "^(\\d{5})-(\\d{4})-(\\d{1})$", "$1$20$3")
        ).when(
            # Standard 5-4-2 format
            F.regexp_replace(ndc_col, "-", "").rlike("^\\d{11}$"),
            F.regexp_replace(ndc_col, "-", "")
        ).otherwise(
            F.regexp_replace(ndc_col, "-", "")
        )
    )


def load_ndc_product(spark: SparkSession, file_path: str) -> DataFrame:
    """Load and transform NDC product file. All columns remain STRING."""
    df = (
        spark.read
        .option("header", "true")
        .option("delimiter", "\t")
        .option("quote", '"')
        .schema(PRODUCT_SCHEMA)
        .csv(file_path)
    )
    
    return (
        df
        # Keep dates as strings (YYYYMMDD format from source)
        .withColumn("listing_certified_date", F.col("listing_record_certified_through"))
        # String-based boolean flags
        .withColumn("is_excluded", 
                    F.when(F.upper(F.col("ndc_exclude_flag")) == "Y", "true").otherwise("false"))
        .withColumn("is_active", 
                    F.when(F.col("end_marketing_date").isNull() | (F.trim(F.col("end_marketing_date")) == ""), "true").otherwise("false"))
        .withColumn("is_prescription", 
                    F.when(F.col("product_type_name").contains("PRESCRIPTION"), "true").otherwise("false"))
        .withColumn("is_otc", 
                    F.when(F.col("product_type_name").contains("OTC"), "true").otherwise("false"))
        .withColumn("is_controlled", 
                    F.when(F.col("dea_schedule").isNotNull() & (F.trim(F.col("dea_schedule")) != ""), "true").otherwise("false"))
        .withColumn("product_ndc_normalized", F.regexp_replace(F.col("product_ndc"), "-", ""))
        .drop("listing_record_certified_through")
    )


def load_ndc_package(spark: SparkSession, file_path: str) -> DataFrame:
    """Load and transform NDC package file. All columns remain STRING."""
    df = (
        spark.read
        .option("header", "true")
        .option("delimiter", "\t")
        .option("quote", '"')
        .schema(PACKAGE_SCHEMA)
        .csv(file_path)
    )
    
    return (
        df
        # Keep dates as strings (YYYYMMDD format from source)
        # String-based boolean flags
        .withColumn("is_excluded", 
                    F.when(F.upper(F.col("ndc_exclude_flag")) == "Y", "true").otherwise("false"))
        .withColumn("is_sample", 
                    F.when(F.upper(F.col("sample_package")) == "Y", "true").otherwise("false"))
        .withColumn("is_active", 
                    F.when(F.col("end_marketing_date").isNull() | (F.trim(F.col("end_marketing_date")) == ""), "true").otherwise("false"))
        .withColumn("ndc_11_digit", normalize_ndc_to_11digit(F.col("ndc_package_code")))
        .withColumn("labeler_code", F.substring(normalize_ndc_to_11digit(F.col("ndc_package_code")), 1, 5))
        .withColumn("product_code", F.substring(normalize_ndc_to_11digit(F.col("ndc_package_code")), 6, 4))
        .withColumn("package_code", F.substring(normalize_ndc_to_11digit(F.col("ndc_package_code")), 10, 2))
    )


def load_ndc(
    spark: SparkSession,
    config: ReferenceConfig,
    product_file: Optional[str] = None,
    package_file: Optional[str] = None
) -> tuple[ReferenceLoadResult, ReferenceLoadResult]:
    """
    Load NDC reference data (both product and package tables).
    
    Args:
        spark: SparkSession
        config: Reference configuration
        product_file: Optional specific product file path
        package_file: Optional specific package file path
    
    Returns:
        Tuple of ReferenceLoadResult for product and package tables
    """
    results = []
    
    # Load product table
    start_time = time.time()
    table_name = "lookup_ndc_product"
    try:
        file_path = product_file or f"{config.ndc_volume}/{NDC_PRODUCT_FILE}"
        df = pipe(
            load_ndc_product(spark, file_path),
            add_reference_audit_columns("NDC_PRODUCT")
        )
        row_count = df.count()
        full_table_name = write_reference_table(spark, config, table_name, df)
        elapsed = time.time() - start_time
        results.append(ReferenceLoadResult(table_name, full_table_name, row_count, round(elapsed, 2), True))
    except Exception as e:
        elapsed = time.time() - start_time
        results.append(ReferenceLoadResult(table_name, "", 0, round(elapsed, 2), False, str(e)))
    
    # Load package table
    start_time = time.time()
    table_name = "lookup_ndc_package"
    try:
        file_path = package_file or f"{config.ndc_volume}/{NDC_PACKAGE_FILE}"
        df = pipe(
            load_ndc_package(spark, file_path),
            add_reference_audit_columns("NDC_PACKAGE")
        )
        row_count = df.count()
        full_table_name = write_reference_table(spark, config, table_name, df)
        elapsed = time.time() - start_time
        results.append(ReferenceLoadResult(table_name, full_table_name, row_count, round(elapsed, 2), True))
    except Exception as e:
        elapsed = time.time() - start_time
        results.append(ReferenceLoadResult(table_name, "", 0, round(elapsed, 2), False, str(e)))
    
    return tuple(results)


def create_ndc_views(spark: SparkSession, config: ReferenceConfig) -> None:
    """Create useful NDC views."""
    schema_path = config.full_schema_path
    
    # Complete NDC view (product + package joined)
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_ndc_complete AS
    SELECT 
        pkg.ndc_package_code,
        pkg.ndc_11_digit,
        pkg.labeler_code,
        pkg.product_code,
        pkg.package_code,
        pkg.package_description,
        prd.proprietary_name,
        prd.proprietary_name_suffix,
        prd.nonproprietary_name,
        prd.dosage_form_name,
        prd.route_name,
        prd.labeler_name,
        prd.substance_name,
        prd.active_numerator_strength,
        prd.active_ingred_unit,
        prd.pharm_classes,
        prd.dea_schedule,
        prd.marketing_category_name,
        prd.application_number,
        prd.product_type_name,
        prd.is_prescription,
        prd.is_otc,
        prd.is_controlled,
        pkg.is_sample,
        CASE WHEN COALESCE(pkg.is_active, 'false') = 'true' AND COALESCE(prd.is_active, 'false') = 'true' THEN 'true' ELSE 'false' END as is_active,
        pkg.start_marketing_date as package_start_date,
        pkg.end_marketing_date as package_end_date,
        prd.start_marketing_date as product_start_date,
        prd.end_marketing_date as product_end_date
    FROM {schema_path}.lookup_ndc_package pkg
    LEFT JOIN {schema_path}.lookup_ndc_product prd ON pkg.product_id = prd.product_id
    WHERE pkg.is_excluded = 'false' AND (prd.is_excluded = 'false' OR prd.is_excluded IS NULL)
    """)
    
    # Active NDC codes
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_ndc_active AS
    SELECT * FROM {schema_path}.v_ndc_complete
    WHERE is_active = 'true' AND is_sample = 'false'
    """)
    
    # NDC lookup
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_ndc_lookup AS
    SELECT 
        ndc_11_digit,
        ndc_package_code as ndc_10_digit_dashed,
        proprietary_name,
        nonproprietary_name,
        dosage_form_name,
        route_name,
        labeler_name,
        substance_name,
        active_numerator_strength,
        active_ingred_unit,
        is_prescription,
        is_controlled,
        dea_schedule
    FROM {schema_path}.v_ndc_complete
    WHERE is_active = 'true'
    """)
    
    # NDC by labeler
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_ndc_by_labeler AS
    SELECT 
        labeler_name,
        labeler_code,
        COUNT(DISTINCT ndc_11_digit) as ndc_count,
        COUNT(DISTINCT nonproprietary_name) as unique_drugs
    FROM {schema_path}.v_ndc_complete
    WHERE is_active = 'true'
    GROUP BY labeler_name, labeler_code
    ORDER BY ndc_count DESC
    """)
    
    # Controlled substances
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_ndc_controlled AS
    SELECT * FROM {schema_path}.v_ndc_complete
    WHERE is_controlled = 'true' AND is_active = 'true'
    ORDER BY dea_schedule, nonproprietary_name
    """)
    
    print(f"Created NDC views in {schema_path}")
