"""
ICD-10-CM Reference Data Ingestion

Loads ICD-10-CM (International Classification of Diseases, 10th Revision, 
Clinical Modification) data from CMS fixed-width files into Unity Catalog.

Source Files:
- icd10cm-order-*.txt - Complete ICD-10-CM code list with hierarchy (fixed-width)
- icd10cm-codes-*.txt - Billable codes only (optional)

Output Tables:
- lookup_icd10cm_codes - Complete ICD-10-CM codes with descriptions
"""

import time
from typing import Optional, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .reference_config import (
    ReferenceConfig, ReferenceLoadResult, pipe,
    add_reference_audit_columns, write_reference_table
)


# ICD-10-CM file patterns
# Files are in txt/ subdirectory
ICD10CM_ORDER_FILE_PATTERN = "txt/icd10cm-order-*.txt"

# ICD-10-CM Chapter definitions
ICD10CM_CHAPTERS: List[Tuple[str, str, str, str]] = [
    ("A", "B", "01", "Certain infectious and parasitic diseases"),
    ("C", "D", "02", "Neoplasms"),
    ("D", "D", "03", "Diseases of the blood and blood-forming organs"),
    ("E", "E", "04", "Endocrine, nutritional and metabolic diseases"),
    ("F", "F", "05", "Mental, Behavioral and Neurodevelopmental disorders"),
    ("G", "G", "06", "Diseases of the nervous system"),
    ("H", "H", "07", "Diseases of the eye and adnexa / ear and mastoid process"),
    ("I", "I", "09", "Diseases of the circulatory system"),
    ("J", "J", "10", "Diseases of the respiratory system"),
    ("K", "K", "11", "Diseases of the digestive system"),
    ("L", "L", "12", "Diseases of the skin and subcutaneous tissue"),
    ("M", "M", "13", "Diseases of the musculoskeletal system and connective tissue"),
    ("N", "N", "14", "Diseases of the genitourinary system"),
    ("O", "O", "15", "Pregnancy, childbirth and the puerperium"),
    ("P", "P", "16", "Certain conditions originating in the perinatal period"),
    ("Q", "Q", "17", "Congenital malformations, deformations and chromosomal abnormalities"),
    ("R", "R", "18", "Symptoms, signs and abnormal clinical and laboratory findings"),
    ("S", "T", "19", "Injury, poisoning and certain other consequences of external causes"),
    ("V", "Y", "20", "External causes of morbidity"),
    ("Z", "Z", "21", "Factors influencing health status and contact with health services"),
    ("U", "U", "22", "Codes for special purposes"),
]


def parse_icd10cm_order_file(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Parse ICD-10-CM order file (fixed-width) into a DataFrame.
    All columns are STRING type for bronze-level ingestion.
    
    File layout:
    - Positions 1-5: Order number (right-justified integer)
    - Positions 7-13: ICD-10-CM code (no decimal point, 3-7 chars)
    - Position 15: Billable flag (0=header, 1=billable)
    - Positions 17-76: Short description (60 chars)
    - Positions 77+: Long description (variable length)
    """
    raw_df = spark.read.text(file_path)
    
    parsed_df = (
        raw_df
        .select(
            F.trim(F.substring(F.col("value"), 1, 5)).alias("order_number"),
            F.trim(F.substring(F.col("value"), 7, 7)).alias("icd10cm_code_raw"),
            F.trim(F.substring(F.col("value"), 15, 1)).alias("is_billable_flag"),
            F.trim(F.substring(F.col("value"), 17, 60)).alias("short_description"),
            F.trim(F.substring(F.col("value"), 77, 200)).alias("long_description")
        )
        .withColumn(
            # Format code with decimal point (e.g., A000 -> A00.0)
            "icd10cm_code",
            F.when(
                F.length(F.col("icd10cm_code_raw")) > 3,
                F.regexp_replace(F.col("icd10cm_code_raw"), "^(.{3})(.+)$", "$1.$2")
            ).otherwise(F.col("icd10cm_code_raw"))
        )
        .withColumn("is_billable", F.when(F.col("is_billable_flag") == "1", "true").otherwise("false"))
        .withColumn("chapter_letter", F.upper(F.substring(F.col("icd10cm_code_raw"), 1, 1)))
        .withColumn(
            "code_level",
            F.when(F.length(F.col("icd10cm_code_raw")) == 3, "CATEGORY")
            .when(F.length(F.col("icd10cm_code_raw")) == 4, "SUBCATEGORY")
            .when(F.length(F.col("icd10cm_code_raw")) == 5, "SUBCATEGORY_2")
            .when(F.length(F.col("icd10cm_code_raw")) == 6, "SUBCATEGORY_3")
            .when(F.length(F.col("icd10cm_code_raw")) == 7, "FULL_CODE")
            .otherwise("UNKNOWN")
        )
        .withColumn("category_code", F.upper(F.substring(F.col("icd10cm_code_raw"), 1, 3)))
        .drop("is_billable_flag", "icd10cm_code_raw")
    )
    
    return parsed_df


def add_chapter_info(spark: SparkSession, df: DataFrame) -> DataFrame:
    """Add chapter information to ICD-10-CM codes."""
    chapters_df = spark.createDataFrame(
        ICD10CM_CHAPTERS,
        ["start_letter", "end_letter", "chapter_num", "chapter_description"]
    )
    
    return (
        df
        .join(
            chapters_df,
            (F.col("chapter_letter") >= F.col("start_letter")) & 
            (F.col("chapter_letter") <= F.col("end_letter")),
            "left"
        )
        .drop("start_letter", "end_letter")
    )


def load_icd10cm(
    spark: SparkSession,
    config: ReferenceConfig,
    order_file: Optional[str] = None
) -> ReferenceLoadResult:
    """
    Load ICD-10-CM reference data.
    
    Args:
        spark: SparkSession
        config: Reference configuration
        order_file: Optional specific file path
    
    Returns:
        ReferenceLoadResult with load statistics
    """
    start_time = time.time()
    table_name = "lookup_icd10cm_codes"
    
    try:
        file_path = order_file or f"{config.icd10cm_volume}/{ICD10CM_ORDER_FILE_PATTERN}"
        
        # Parse and transform
        df = pipe(
            parse_icd10cm_order_file(spark, file_path),
            lambda d: add_chapter_info(spark, d),
            add_reference_audit_columns("ICD10CM")
        )
        
        row_count = df.count()
        full_table_name = write_reference_table(spark, config, table_name, df)
        
        elapsed = time.time() - start_time
        return ReferenceLoadResult(table_name, full_table_name, row_count, round(elapsed, 2), True)
        
    except Exception as e:
        elapsed = time.time() - start_time
        return ReferenceLoadResult(table_name, "", 0, round(elapsed, 2), False, str(e))


def create_icd10cm_views(spark: SparkSession, config: ReferenceConfig) -> None:
    """Create useful ICD-10-CM views."""
    schema_path = config.full_schema_path
    
    # Billable codes only
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_icd10cm_billable AS
    SELECT 
        icd10cm_code,
        short_description,
        long_description,
        category_code,
        chapter_num,
        chapter_description
    FROM {schema_path}.lookup_icd10cm_codes
    WHERE is_billable = 'true'
    """)
    
    # Categories (3-character codes)
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_icd10cm_categories AS
    SELECT 
        icd10cm_code as category_code,
        short_description as category_description,
        chapter_num,
        chapter_description
    FROM {schema_path}.lookup_icd10cm_codes
    WHERE code_level = 'CATEGORY'
    """)
    
    # Chapter summary
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_icd10cm_chapter_summary AS
    SELECT 
        chapter_num,
        chapter_description,
        COUNT(*) as total_codes,
        SUM(CASE WHEN is_billable = 'true' THEN 1 ELSE 0 END) as billable_codes,
        SUM(CASE WHEN is_billable = 'false' THEN 1 ELSE 0 END) as header_codes
    FROM {schema_path}.lookup_icd10cm_codes
    WHERE chapter_num IS NOT NULL
    GROUP BY chapter_num, chapter_description
    ORDER BY chapter_num
    """)
    
    # Code lookup (for crosswalks)
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_icd10cm_lookup AS
    SELECT 
        icd10cm_code,
        REPLACE(icd10cm_code, '.', '') as icd10cm_code_nodot,
        short_description,
        long_description,
        is_billable,
        chapter_num
    FROM {schema_path}.lookup_icd10cm_codes
    """)
    
    print(f"Created ICD-10-CM views in {schema_path}")
