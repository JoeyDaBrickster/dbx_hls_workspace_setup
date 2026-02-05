"""
RxNorm Prescribe Reference Data Ingestion

Loads RxNorm Prescribe subset (RRF format) from NLM into Unity Catalog.
This is a curated subset containing only prescribable drugs currently marketed in the US.

Source Files (RRF Format - pipe-delimited):
- RXNCONSO.RRF - Concept names for prescribable drugs
- RXNSAT.RRF - Attributes (includes NDC codes)
- RXNREL.RRF - Relationships between concepts

Output Tables:
- lookup_rxnorm_prescribe_drugs - Prescribable drug concepts
- lookup_rxnorm_prescribe_ingredients - Active ingredients
- lookup_rxnorm_prescribe_ndc - RxNorm Prescribe to NDC crosswalk
- lookup_rxnorm_prescribe_drug_ingredients - Drug-ingredient relationships
"""

import time
from typing import Optional, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from .reference_config import (
    ReferenceConfig, ReferenceLoadResult, pipe,
    add_reference_audit_columns, write_reference_table
)


# RxNorm Prescribe file paths (relative to volume)
# Files are directly in the rxnorm_prescribe volume
RXNCONSO_FILE = "RXNCONSO.RRF"
RXNSAT_FILE = "RXNSAT.RRF"
RXNREL_FILE = "RXNREL.RRF"


def load_rxnconso_prescribe(spark: SparkSession, file_path: str) -> DataFrame:
    """Load and parse RXNCONSO.RRF file from RxNorm Prescribe."""
    raw_df = (
        spark.read
        .option("delimiter", "|")
        .option("header", "false")
        .csv(file_path)
    )
    
    return (
        raw_df
        .select(
            F.col("_c0").alias("rxcui"),
            F.col("_c1").alias("language"),
            F.col("_c2").alias("term_status"),
            F.col("_c3").alias("lui"),
            F.col("_c4").alias("string_type"),
            F.col("_c5").alias("sui"),
            F.col("_c6").alias("is_preferred"),
            F.col("_c7").alias("rxaui"),
            F.col("_c8").alias("saui"),
            F.col("_c9").alias("scui"),
            F.col("_c10").alias("sdui"),
            F.col("_c11").alias("source_abbrev"),
            F.col("_c12").alias("term_type"),
            F.col("_c13").alias("source_code"),
            F.col("_c14").alias("name"),
            F.col("_c15").alias("source_restriction_level"),
            F.col("_c16").alias("suppress_flag"),
            F.col("_c17").alias("cvf")
        )
        .withColumn("is_preferred_flag", 
                    F.when(F.col("is_preferred") == "Y", "true").otherwise("false"))
        .withColumn(
            "is_prescribable",
            F.when(F.col("term_type").isin("SCD", "SBD", "GPCK", "BPCK"), "true").otherwise("false")
        )
    )


def load_rxnsat_prescribe(spark: SparkSession, file_path: str) -> DataFrame:
    """Load and parse RXNSAT.RRF file from RxNorm Prescribe."""
    raw_df = (
        spark.read
        .option("delimiter", "|")
        .option("header", "false")
        .csv(file_path)
    )
    
    return (
        raw_df
        .select(
            F.col("_c0").alias("rxcui"),
            F.col("_c1").alias("lui"),
            F.col("_c2").alias("sui"),
            F.col("_c3").alias("rxaui"),
            F.col("_c4").alias("stype"),
            F.col("_c5").alias("code"),
            F.col("_c6").alias("atui"),
            F.col("_c7").alias("satui"),
            F.col("_c8").alias("atn"),
            F.col("_c9").alias("source_abbrev"),
            F.col("_c10").alias("atv"),
            F.col("_c11").alias("suppress_flag")
        )
    )


def load_rxnrel_prescribe(spark: SparkSession, file_path: str) -> DataFrame:
    """Load and parse RXNREL.RRF file from RxNorm Prescribe."""
    raw_df = (
        spark.read
        .option("delimiter", "|")
        .option("header", "false")
        .csv(file_path)
    )
    
    return (
        raw_df
        .select(
            F.col("_c0").alias("rxcui1"),
            F.col("_c1").alias("rxaui1"),
            F.col("_c2").alias("stype1"),
            F.col("_c3").alias("rel"),
            F.col("_c4").alias("rxcui2"),
            F.col("_c5").alias("rxaui2"),
            F.col("_c6").alias("stype2"),
            F.col("_c7").alias("rela"),
            F.col("_c8").alias("rui"),
            F.col("_c9").alias("srui"),
            F.col("_c10").alias("source_abbrev"),
            F.col("_c11").alias("source_rela"),
            F.col("_c12").alias("rg"),
            F.col("_c13").alias("dir"),
            F.col("_c14").alias("suppress_flag")
        )
    )


def create_prescribable_drugs(rxnconso_df: DataFrame) -> DataFrame:
    """Create prescribable drugs table."""
    return (
        rxnconso_df
        .filter(
            (F.col("source_abbrev") == "RXNORM") & 
            (F.col("language") == "ENG") &
            (F.col("is_prescribable") == "true")
        )
        .withColumn(
            "drug_type",
            F.when(F.col("term_type") == "SCD", "GENERIC_DRUG")
            .when(F.col("term_type") == "SBD", "BRANDED_DRUG")
            .when(F.col("term_type") == "GPCK", "GENERIC_PACK")
            .when(F.col("term_type") == "BPCK", "BRANDED_PACK")
        )
        .withColumn("is_generic", 
                    F.when(F.col("term_type").isin("SCD", "GPCK"), "true").otherwise("false"))
        .withColumn("is_branded", 
                    F.when(F.col("term_type").isin("SBD", "BPCK"), "true").otherwise("false"))
        .withColumn("is_pack", 
                    F.when(F.col("term_type").isin("GPCK", "BPCK"), "true").otherwise("false"))
        .withColumn(
            "row_num",
            F.row_number().over(
                Window.partitionBy("rxcui").orderBy(F.desc("is_preferred_flag"))
            )
        )
        .filter(F.col("row_num") == 1)
        .select(
            "rxcui",
            "rxaui",
            F.col("name").alias("drug_name"),
            F.col("term_type").alias("tty"),
            "drug_type",
            "is_generic",
            "is_branded",
            "is_pack",
            "source_code"
        )
    )


def create_ingredients(rxnconso_df: DataFrame) -> DataFrame:
    """Create ingredients table."""
    return (
        rxnconso_df
        .filter(
            (F.col("source_abbrev") == "RXNORM") & 
            (F.col("language") == "ENG") &
            (F.col("term_type").isin("IN", "PIN", "MIN"))
        )
        .withColumn(
            "ingredient_type",
            F.when(F.col("term_type") == "IN", "BASE_INGREDIENT")
            .when(F.col("term_type") == "PIN", "PRECISE_INGREDIENT")
            .when(F.col("term_type") == "MIN", "MULTIPLE_INGREDIENTS")
        )
        .withColumn(
            "row_num",
            F.row_number().over(
                Window.partitionBy("rxcui").orderBy(F.desc("is_preferred_flag"))
            )
        )
        .filter(F.col("row_num") == 1)
        .select(
            "rxcui",
            F.col("name").alias("ingredient_name"),
            F.col("term_type").alias("tty"),
            "ingredient_type"
        )
    )


def create_prescribe_ndc(rxnsat_df: DataFrame, drugs_df: DataFrame) -> DataFrame:
    """Create RxNorm Prescribe to NDC crosswalk."""
    ndc_df = (
        rxnsat_df
        .filter(
            (F.col("atn") == "NDC") & 
            (F.col("atv").isNotNull()) &
            (F.length(F.col("atv")) >= 10)
        )
        .select(
            "rxcui",
            "rxaui",
            F.col("atv").alias("ndc_code"),
            "source_abbrev"
        )
        .withColumn("ndc_11_digit", F.regexp_replace(F.col("ndc_code"), "-", ""))
        .withColumn("ndc_length", F.length(F.col("ndc_11_digit")))
        .filter((F.col("ndc_length") >= 10) & (F.col("ndc_length") <= 11))
        .withColumn(
            "ndc_11_digit",
            F.when(F.col("ndc_length") == 10, F.concat(F.lit("0"), F.col("ndc_11_digit")))
            .otherwise(F.col("ndc_11_digit"))
        )
    )
    
    return (
        ndc_df
        .join(drugs_df.select("rxcui", "drug_name", "tty", "drug_type"), "rxcui", "inner")
        .select(
            "rxcui",
            "drug_name",
            "tty",
            "drug_type",
            F.col("ndc_code").alias("ndc_original"),
            "ndc_11_digit",
            "source_abbrev"
        )
        .distinct()
    )


def create_drug_ingredients(rxnrel_df: DataFrame, drugs_df: DataFrame, ingredients_df: DataFrame) -> DataFrame:
    """Create drug-ingredient mapping."""
    return (
        rxnrel_df
        .filter(F.col("rela") == "has_ingredient")
        .select(
            F.col("rxcui1").alias("drug_rxcui"),
            F.col("rxcui2").alias("ingredient_rxcui")
        )
        .join(
            drugs_df.select(F.col("rxcui").alias("drug_rxcui"), "drug_name"),
            "drug_rxcui",
            "inner"
        )
        .join(
            ingredients_df.select(F.col("rxcui").alias("ingredient_rxcui"), "ingredient_name"),
            "ingredient_rxcui",
            "inner"
        )
    )


def load_rxnorm_prescribe(
    spark: SparkSession,
    config: ReferenceConfig,
    conso_file: Optional[str] = None,
    sat_file: Optional[str] = None,
    rel_file: Optional[str] = None
) -> List[ReferenceLoadResult]:
    """
    Load RxNorm Prescribe reference data.
    
    Args:
        spark: SparkSession
        config: Reference configuration
        conso_file: Optional specific RXNCONSO file path
        sat_file: Optional specific RXNSAT file path
        rel_file: Optional specific RXNREL file path
    
    Returns:
        List of ReferenceLoadResult for each table
    """
    results = []
    
    # Load raw files
    conso_path = conso_file or f"{config.rxnorm_prescribe_volume}/{RXNCONSO_FILE}"
    sat_path = sat_file or f"{config.rxnorm_prescribe_volume}/{RXNSAT_FILE}"
    rel_path = rel_file or f"{config.rxnorm_prescribe_volume}/{RXNREL_FILE}"
    
    rxnconso_df = load_rxnconso_prescribe(spark, conso_path)
    rxnsat_df = load_rxnsat_prescribe(spark, sat_path)
    rxnrel_df = load_rxnrel_prescribe(spark, rel_path)
    
    # Create derived DataFrames
    drugs_df = create_prescribable_drugs(rxnconso_df)
    ingredients_df = create_ingredients(rxnconso_df)
    
    # 1. Save prescribable drugs
    start_time = time.time()
    table_name = "lookup_rxnorm_prescribe_drugs"
    try:
        df = pipe(drugs_df, add_reference_audit_columns("RXNORM_PRESCRIBE_DRUGS"))
        row_count = df.count()
        full_table_name = write_reference_table(spark, config, table_name, df)
        elapsed = time.time() - start_time
        results.append(ReferenceLoadResult(table_name, full_table_name, row_count, round(elapsed, 2), True))
    except Exception as e:
        elapsed = time.time() - start_time
        results.append(ReferenceLoadResult(table_name, "", 0, round(elapsed, 2), False, str(e)))
    
    # 2. Save ingredients
    start_time = time.time()
    table_name = "lookup_rxnorm_prescribe_ingredients"
    try:
        df = pipe(ingredients_df, add_reference_audit_columns("RXNORM_PRESCRIBE_INGREDIENTS"))
        row_count = df.count()
        full_table_name = write_reference_table(spark, config, table_name, df)
        elapsed = time.time() - start_time
        results.append(ReferenceLoadResult(table_name, full_table_name, row_count, round(elapsed, 2), True))
    except Exception as e:
        elapsed = time.time() - start_time
        results.append(ReferenceLoadResult(table_name, "", 0, round(elapsed, 2), False, str(e)))
    
    # 3. Save NDC crosswalk
    start_time = time.time()
    table_name = "lookup_rxnorm_prescribe_ndc"
    try:
        df = pipe(
            create_prescribe_ndc(rxnsat_df, drugs_df),
            add_reference_audit_columns("RXNORM_PRESCRIBE_NDC")
        )
        row_count = df.count()
        full_table_name = write_reference_table(spark, config, table_name, df)
        elapsed = time.time() - start_time
        results.append(ReferenceLoadResult(table_name, full_table_name, row_count, round(elapsed, 2), True))
    except Exception as e:
        elapsed = time.time() - start_time
        results.append(ReferenceLoadResult(table_name, "", 0, round(elapsed, 2), False, str(e)))
    
    # 4. Save drug-ingredient mapping
    start_time = time.time()
    table_name = "lookup_rxnorm_prescribe_drug_ingredients"
    try:
        df = pipe(
            create_drug_ingredients(rxnrel_df, drugs_df, ingredients_df),
            add_reference_audit_columns("RXNORM_PRESCRIBE_DRUG_INGREDIENTS")
        )
        row_count = df.count()
        full_table_name = write_reference_table(spark, config, table_name, df)
        elapsed = time.time() - start_time
        results.append(ReferenceLoadResult(table_name, full_table_name, row_count, round(elapsed, 2), True))
    except Exception as e:
        elapsed = time.time() - start_time
        results.append(ReferenceLoadResult(table_name, "", 0, round(elapsed, 2), False, str(e)))
    
    return results


def create_rxnorm_prescribe_views(spark: SparkSession, config: ReferenceConfig) -> None:
    """Create useful RxNorm Prescribe views."""
    schema_path = config.full_schema_path
    
    # Generic prescribable drugs
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_rxnorm_prescribe_generic AS
    SELECT rxcui, drug_name, tty, drug_type
    FROM {schema_path}.lookup_rxnorm_prescribe_drugs
    WHERE is_generic = 'true'
    """)
    
    # Branded prescribable drugs
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_rxnorm_prescribe_branded AS
    SELECT rxcui, drug_name, tty, drug_type
    FROM {schema_path}.lookup_rxnorm_prescribe_drugs
    WHERE is_branded = 'true'
    """)
    
    # NDC lookup for prescribable drugs
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_rxnorm_prescribe_ndc_lookup AS
    SELECT 
        ndc_11_digit,
        ndc_original,
        rxcui,
        drug_name,
        drug_type
    FROM {schema_path}.lookup_rxnorm_prescribe_ndc
    """)
    
    # Drug with all NDCs
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_rxnorm_prescribe_drug_ndc_all AS
    SELECT 
        rxcui,
        drug_name,
        drug_type,
        COLLECT_SET(ndc_11_digit) as ndc_codes,
        COUNT(DISTINCT ndc_11_digit) as ndc_count
    FROM {schema_path}.lookup_rxnorm_prescribe_ndc
    GROUP BY rxcui, drug_name, drug_type
    """)
    
    # Drugs with ingredients
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_rxnorm_prescribe_drugs_expanded AS
    SELECT 
        d.rxcui,
        d.drug_name,
        d.drug_type,
        d.is_generic,
        d.is_branded,
        d.is_pack,
        COLLECT_SET(di.ingredient_name) as ingredients,
        COUNT(DISTINCT di.ingredient_rxcui) as ingredient_count
    FROM {schema_path}.lookup_rxnorm_prescribe_drugs d
    LEFT JOIN {schema_path}.lookup_rxnorm_prescribe_drug_ingredients di ON d.rxcui = di.drug_rxcui
    GROUP BY d.rxcui, d.drug_name, d.drug_type, d.is_generic, d.is_branded, d.is_pack
    """)
    
    print(f"Created RxNorm Prescribe views in {schema_path}")
