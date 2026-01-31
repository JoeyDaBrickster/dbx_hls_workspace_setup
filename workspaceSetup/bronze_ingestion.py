"""
Bronze Layer Ingestion

Raw file ingestion from Synthea data organized by state.
All columns are STRING type with no schema inference.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Callable
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from .config import (
    PipelineConfig, pipe, add_audit_columns, add_state_column,
    standardize_column_names, get_all_csv_paths, get_full_table_name
)


# Bronze schemas - all STRING columns
BRONZE_SCHEMAS: Dict[str, StructType] = {
    "patients": StructType([
        StructField("Id", StringType(), True),
        StructField("BIRTHDATE", StringType(), True),
        StructField("DEATHDATE", StringType(), True),
        StructField("SSN", StringType(), True),
        StructField("DRIVERS", StringType(), True),
        StructField("PASSPORT", StringType(), True),
        StructField("PREFIX", StringType(), True),
        StructField("FIRST", StringType(), True),
        StructField("MIDDLE", StringType(), True),
        StructField("LAST", StringType(), True),
        StructField("SUFFIX", StringType(), True),
        StructField("MAIDEN", StringType(), True),
        StructField("MARITAL", StringType(), True),
        StructField("RACE", StringType(), True),
        StructField("ETHNICITY", StringType(), True),
        StructField("GENDER", StringType(), True),
        StructField("BIRTHPLACE", StringType(), True),
        StructField("ADDRESS", StringType(), True),
        StructField("CITY", StringType(), True),
        StructField("STATE", StringType(), True),
        StructField("COUNTY", StringType(), True),
        StructField("FIPS", StringType(), True),
        StructField("ZIP", StringType(), True),
        StructField("LAT", StringType(), True),
        StructField("LON", StringType(), True),
        StructField("HEALTHCARE_EXPENSES", StringType(), True),
        StructField("HEALTHCARE_COVERAGE", StringType(), True),
        StructField("INCOME", StringType(), True),
    ]),
    
    "encounters": StructType([
        StructField("Id", StringType(), True),
        StructField("START", StringType(), True),
        StructField("STOP", StringType(), True),
        StructField("PATIENT", StringType(), True),
        StructField("ORGANIZATION", StringType(), True),
        StructField("PROVIDER", StringType(), True),
        StructField("PAYER", StringType(), True),
        StructField("ENCOUNTERCLASS", StringType(), True),
        StructField("CODE", StringType(), True),
        StructField("DESCRIPTION", StringType(), True),
        StructField("BASE_ENCOUNTER_COST", StringType(), True),
        StructField("TOTAL_CLAIM_COST", StringType(), True),
        StructField("PAYER_COVERAGE", StringType(), True),
        StructField("REASONCODE", StringType(), True),
        StructField("REASONDESCRIPTION", StringType(), True),
    ]),
    
    "conditions": StructType([
        StructField("START", StringType(), True),
        StructField("STOP", StringType(), True),
        StructField("PATIENT", StringType(), True),
        StructField("ENCOUNTER", StringType(), True),
        StructField("SYSTEM", StringType(), True),
        StructField("CODE", StringType(), True),
        StructField("DESCRIPTION", StringType(), True),
    ]),
    
    "observations": StructType([
        StructField("DATE", StringType(), True),
        StructField("PATIENT", StringType(), True),
        StructField("ENCOUNTER", StringType(), True),
        StructField("CATEGORY", StringType(), True),
        StructField("CODE", StringType(), True),
        StructField("DESCRIPTION", StringType(), True),
        StructField("VALUE", StringType(), True),
        StructField("UNITS", StringType(), True),
        StructField("TYPE", StringType(), True),
    ]),
    
    "procedures": StructType([
        StructField("START", StringType(), True),
        StructField("STOP", StringType(), True),
        StructField("PATIENT", StringType(), True),
        StructField("ENCOUNTER", StringType(), True),
        StructField("SYSTEM", StringType(), True),
        StructField("CODE", StringType(), True),
        StructField("DESCRIPTION", StringType(), True),
        StructField("BASE_COST", StringType(), True),
        StructField("REASONCODE", StringType(), True),
        StructField("REASONDESCRIPTION", StringType(), True),
    ]),
    
    "medications": StructType([
        StructField("START", StringType(), True),
        StructField("STOP", StringType(), True),
        StructField("PATIENT", StringType(), True),
        StructField("PAYER", StringType(), True),
        StructField("ENCOUNTER", StringType(), True),
        StructField("CODE", StringType(), True),
        StructField("DESCRIPTION", StringType(), True),
        StructField("BASE_COST", StringType(), True),
        StructField("PAYER_COVERAGE", StringType(), True),
        StructField("DISPENSES", StringType(), True),
        StructField("TOTALCOST", StringType(), True),
        StructField("REASONCODE", StringType(), True),
        StructField("REASONDESCRIPTION", StringType(), True),
    ]),
    
    "immunizations": StructType([
        StructField("DATE", StringType(), True),
        StructField("PATIENT", StringType(), True),
        StructField("ENCOUNTER", StringType(), True),
        StructField("CODE", StringType(), True),
        StructField("DESCRIPTION", StringType(), True),
        StructField("BASE_COST", StringType(), True),
    ]),
    
    "allergies": StructType([
        StructField("START", StringType(), True),
        StructField("STOP", StringType(), True),
        StructField("PATIENT", StringType(), True),
        StructField("ENCOUNTER", StringType(), True),
        StructField("CODE", StringType(), True),
        StructField("SYSTEM", StringType(), True),
        StructField("DESCRIPTION", StringType(), True),
        StructField("TYPE", StringType(), True),
        StructField("CATEGORY", StringType(), True),
        StructField("REACTION1", StringType(), True),
        StructField("DESCRIPTION1", StringType(), True),
        StructField("SEVERITY1", StringType(), True),
        StructField("REACTION2", StringType(), True),
        StructField("DESCRIPTION2", StringType(), True),
        StructField("SEVERITY2", StringType(), True),
    ]),
    
    "careplans": StructType([
        StructField("Id", StringType(), True),
        StructField("START", StringType(), True),
        StructField("STOP", StringType(), True),
        StructField("PATIENT", StringType(), True),
        StructField("ENCOUNTER", StringType(), True),
        StructField("CODE", StringType(), True),
        StructField("DESCRIPTION", StringType(), True),
        StructField("REASONCODE", StringType(), True),
        StructField("REASONDESCRIPTION", StringType(), True),
    ]),
    
    "devices": StructType([
        StructField("START", StringType(), True),
        StructField("STOP", StringType(), True),
        StructField("PATIENT", StringType(), True),
        StructField("ENCOUNTER", StringType(), True),
        StructField("CODE", StringType(), True),
        StructField("DESCRIPTION", StringType(), True),
        StructField("UDI", StringType(), True),
    ]),
    
    "imaging_studies": StructType([
        StructField("Id", StringType(), True),
        StructField("DATE", StringType(), True),
        StructField("PATIENT", StringType(), True),
        StructField("ENCOUNTER", StringType(), True),
        StructField("SERIES_UID", StringType(), True),
        StructField("BODYSITE_CODE", StringType(), True),
        StructField("BODYSITE_DESCRIPTION", StringType(), True),
        StructField("MODALITY_CODE", StringType(), True),
        StructField("MODALITY_DESCRIPTION", StringType(), True),
        StructField("INSTANCE_UID", StringType(), True),
        StructField("SOP_CODE", StringType(), True),
        StructField("SOP_DESCRIPTION", StringType(), True),
        StructField("PROCEDURE_CODE", StringType(), True),
    ]),
    
    "supplies": StructType([
        StructField("DATE", StringType(), True),
        StructField("PATIENT", StringType(), True),
        StructField("ENCOUNTER", StringType(), True),
        StructField("CODE", StringType(), True),
        StructField("DESCRIPTION", StringType(), True),
        StructField("QUANTITY", StringType(), True),
    ]),
    
    "organizations": StructType([
        StructField("Id", StringType(), True),
        StructField("NAME", StringType(), True),
        StructField("ADDRESS", StringType(), True),
        StructField("CITY", StringType(), True),
        StructField("STATE", StringType(), True),
        StructField("ZIP", StringType(), True),
        StructField("LAT", StringType(), True),
        StructField("LON", StringType(), True),
        StructField("PHONE", StringType(), True),
        StructField("REVENUE", StringType(), True),
        StructField("UTILIZATION", StringType(), True),
    ]),
    
    "providers": StructType([
        StructField("Id", StringType(), True),
        StructField("ORGANIZATION", StringType(), True),
        StructField("NAME", StringType(), True),
        StructField("GENDER", StringType(), True),
        StructField("SPECIALITY", StringType(), True),
        StructField("ADDRESS", StringType(), True),
        StructField("CITY", StringType(), True),
        StructField("STATE", StringType(), True),
        StructField("ZIP", StringType(), True),
        StructField("LAT", StringType(), True),
        StructField("LON", StringType(), True),
        StructField("ENCOUNTERS", StringType(), True),
        StructField("PROCEDURES", StringType(), True),
    ]),
    
    "payers": StructType([
        StructField("Id", StringType(), True),
        StructField("NAME", StringType(), True),
        StructField("OWNERSHIP", StringType(), True),
        StructField("ADDRESS", StringType(), True),
        StructField("CITY", StringType(), True),
        StructField("STATE_HEADQUARTERED", StringType(), True),
        StructField("ZIP", StringType(), True),
        StructField("PHONE", StringType(), True),
        StructField("AMOUNT_COVERED", StringType(), True),
        StructField("AMOUNT_UNCOVERED", StringType(), True),
        StructField("REVENUE", StringType(), True),
        StructField("COVERED_ENCOUNTERS", StringType(), True),
        StructField("UNCOVERED_ENCOUNTERS", StringType(), True),
        StructField("COVERED_MEDICATIONS", StringType(), True),
        StructField("UNCOVERED_MEDICATIONS", StringType(), True),
        StructField("COVERED_PROCEDURES", StringType(), True),
        StructField("UNCOVERED_PROCEDURES", StringType(), True),
        StructField("COVERED_IMMUNIZATIONS", StringType(), True),
        StructField("UNCOVERED_IMMUNIZATIONS", StringType(), True),
        StructField("UNIQUE_CUSTOMERS", StringType(), True),
        StructField("QOLS_AVG", StringType(), True),
        StructField("MEMBER_MONTHS", StringType(), True),
    ]),
    
    "claims": StructType([
        StructField("Id", StringType(), True),
        StructField("PATIENTID", StringType(), True),
        StructField("PROVIDERID", StringType(), True),
        StructField("PRIMARYPATIENTINSURANCEID", StringType(), True),
        StructField("SECONDARYPATIENTINSURANCEID", StringType(), True),
        StructField("DEPARTMENTID", StringType(), True),
        StructField("PATIENTDEPARTMENTID", StringType(), True),
        StructField("DIAGNOSIS1", StringType(), True),
        StructField("DIAGNOSIS2", StringType(), True),
        StructField("DIAGNOSIS3", StringType(), True),
        StructField("DIAGNOSIS4", StringType(), True),
        StructField("DIAGNOSIS5", StringType(), True),
        StructField("DIAGNOSIS6", StringType(), True),
        StructField("DIAGNOSIS7", StringType(), True),
        StructField("DIAGNOSIS8", StringType(), True),
        StructField("REFERRINGPROVIDERID", StringType(), True),
        StructField("APPOINTMENTID", StringType(), True),
        StructField("CURRENTILLNESSDATE", StringType(), True),
        StructField("SERVICEDATE", StringType(), True),
        StructField("SUPERVISINGPROVIDERID", StringType(), True),
        StructField("STATUS1", StringType(), True),
        StructField("STATUS2", StringType(), True),
        StructField("STATUSP", StringType(), True),
        StructField("OUTSTANDING1", StringType(), True),
        StructField("OUTSTANDING2", StringType(), True),
        StructField("OUTSTANDINGP", StringType(), True),
        StructField("LASTBILLEDDATE1", StringType(), True),
        StructField("LASTBILLEDDATE2", StringType(), True),
        StructField("LASTBILLEDDATEP", StringType(), True),
        StructField("HEALTHCARECLAIMTYPEID1", StringType(), True),
        StructField("HEALTHCARECLAIMTYPEID2", StringType(), True),
    ]),
    
    "claims_transactions": StructType([
        StructField("ID", StringType(), True),
        StructField("CLAIMID", StringType(), True),
        StructField("CHARGEID", StringType(), True),
        StructField("PATIENTID", StringType(), True),
        StructField("TYPE", StringType(), True),
        StructField("AMOUNT", StringType(), True),
        StructField("METHOD", StringType(), True),
        StructField("FROMDATE", StringType(), True),
        StructField("TODATE", StringType(), True),
        StructField("PLACEOFSERVICE", StringType(), True),
        StructField("PROCEDURECODE", StringType(), True),
        StructField("MODIFIER1", StringType(), True),
        StructField("MODIFIER2", StringType(), True),
        StructField("DIAGNOSISREF1", StringType(), True),
        StructField("DIAGNOSISREF2", StringType(), True),
        StructField("DIAGNOSISREF3", StringType(), True),
        StructField("DIAGNOSISREF4", StringType(), True),
        StructField("UNITS", StringType(), True),
        StructField("DEPARTMENTID", StringType(), True),
        StructField("NOTES", StringType(), True),
        StructField("UNITAMOUNT", StringType(), True),
        StructField("TRANSFEROUTID", StringType(), True),
        StructField("TRANSFERTYPE", StringType(), True),
        StructField("PAYMENTS", StringType(), True),
        StructField("ADJUSTMENTS", StringType(), True),
        StructField("TRANSFERS", StringType(), True),
        StructField("OUTSTANDING", StringType(), True),
        StructField("APPOINTMENTID", StringType(), True),
        StructField("LINENOTE", StringType(), True),
        StructField("PATIENTINSURANCEID", StringType(), True),
        StructField("FEESCHEDULEID", StringType(), True),
        StructField("PROVIDERID", StringType(), True),
        StructField("SUPERVISINGPROVIDERID", StringType(), True),
    ]),
    
    "payer_transitions": StructType([
        StructField("PATIENT", StringType(), True),
        StructField("MEMBERID", StringType(), True),
        StructField("START_DATE", StringType(), True),
        StructField("END_DATE", StringType(), True),
        StructField("PAYER", StringType(), True),
        StructField("SECONDARY_PAYER", StringType(), True),
        StructField("PLAN_OWNERSHIP", StringType(), True),
        StructField("OWNER_NAME", StringType(), True),
    ]),
    
    "symptoms": StructType([
        StructField("PATIENT", StringType(), True),
        StructField("GENDER", StringType(), True),
        StructField("RACE", StringType(), True),
        StructField("ETHNICITY", StringType(), True),
        StructField("AGE_BEGIN", StringType(), True),
        StructField("AGE_END", StringType(), True),
        StructField("PATHOLOGY", StringType(), True),
        StructField("NUM_SYMPTOMS", StringType(), True),
        StructField("SYMPTOMS", StringType(), True),
    ]),
}


@dataclass
class IngestionResult:
    """Result of a single table ingestion."""
    table_name: str
    full_table_name: str
    row_count: int
    elapsed_seconds: float
    success: bool
    error: Optional[str] = None


def read_bronze_csv(
    spark: SparkSession,
    config: PipelineConfig,
    table_name: str,
    schema: StructType
) -> DataFrame:
    """Read CSV files from all states with explicit STRING schema."""
    csv_file = f"{table_name}.csv"
    glob_pattern = get_all_csv_paths(config, csv_file)
    
    df = (
        spark.read
        .format("csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .load(glob_pattern)
    )
    
    return pipe(
        df,
        add_audit_columns(config.source_system, table_name),
        add_state_column(),
        standardize_column_names()
    )


def read_symptoms_csv(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Read symptoms CSV from output/symptoms/csv/symptoms.csv."""
    glob_pattern = f"{config.volume_source}/*/output/symptoms/csv/symptoms.csv"
    schema = BRONZE_SCHEMAS["symptoms"]
    
    df = (
        spark.read
        .format("csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .schema(schema)
        .load(glob_pattern)
    )
    
    return pipe(
        df,
        add_audit_columns(config.source_system, "symptoms"),
        lambda df: df.withColumn("_state", 
            F.regexp_extract(F.col("_metadata.file_path"), r"/([^/]+)/output/symptoms/", 1)),
        standardize_column_names()
    )


def read_notes_text(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Read clinical notes text files from output/notes/*.txt."""
    glob_pattern = f"{config.volume_source}/*/output/notes/*.txt"
    
    df = (
        spark.read
        .format("text")
        .option("wholetext", "true")
        .load(glob_pattern)
    )
    
    return (
        df
        .withColumn("_load_dts", F.current_timestamp())
        .withColumn("_file_name", F.col("_metadata.file_path"))
        .withColumn("_rec_src", F.lit(f"{config.source_system}.NOTES"))
        .withColumn("patient_id", 
            F.regexp_extract(F.col("_metadata.file_path"), r"([a-f0-9-]{36})\.txt$", 1))
        .withColumn("_state", 
            F.regexp_extract(F.col("_metadata.file_path"), r"/([^/]+)/output/notes/", 1))
        .withColumn("patient_name",
            F.regexp_extract(F.col("_metadata.file_path"), r"/([^/]+)_[a-f0-9-]{36}\.txt$", 0))
        .withColumnRenamed("value", "note_content")
        .select("patient_id", "patient_name", "note_content", "_state", "_load_dts", "_file_name", "_rec_src")
    ).filter(F.col("patient_id").isNotNull() & (F.col("patient_id") != ""))


def read_text_encounters(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Read text encounter files from output/text_encounters/*.txt."""
    glob_pattern = f"{config.volume_source}/*/output/text_encounters/*.txt"
    
    df = (
        spark.read
        .format("text")
        .option("wholetext", "true")
        .load(glob_pattern)
    )
    
    return (
        df
        .withColumn("_load_dts", F.current_timestamp())
        .withColumn("_file_name", F.col("_metadata.file_path"))
        .withColumn("_rec_src", F.lit(f"{config.source_system}.TEXT_ENCOUNTERS"))
        .withColumn("patient_id", 
            F.regexp_extract(F.col("_metadata.file_path"), r"([a-f0-9-]{36})\d*\.txt$", 1))
        .withColumn("encounter_number",
            F.regexp_extract(F.col("_metadata.file_path"), r"[a-f0-9-]{36}(\d+)\.txt$", 1))
        .withColumn("_state", 
            F.regexp_extract(F.col("_metadata.file_path"), r"/([^/]+)/output/text_encounters/", 1))
        .withColumnRenamed("value", "encounter_content")
        .select("patient_id", "encounter_number", "encounter_content", "_state", "_load_dts", "_file_name", "_rec_src")
    ).filter(F.col("patient_id").isNotNull() & (F.col("patient_id") != ""))


def write_bronze_table(
    spark: SparkSession,
    config: PipelineConfig,
    table_name: str,
    df: DataFrame,
    mode: str = "overwrite"
) -> str:
    """Write DataFrame to a bronze Delta table."""
    full_table_name = get_full_table_name(config, "bronze", f"bronze_{table_name}")
    
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .option("delta.autoOptimize.optimizeWrite", "true")
        .saveAsTable(full_table_name)
    )
    
    return full_table_name


def ingest_bronze_table(
    spark: SparkSession,
    config: PipelineConfig,
    table_name: str
) -> IngestionResult:
    """Ingest a single bronze CSV table."""
    start_time = time.time()
    
    try:
        schema = BRONZE_SCHEMAS.get(table_name)
        if schema is None:
            raise ValueError(f"No schema defined for table: {table_name}")
        
        if table_name == "symptoms":
            df = read_symptoms_csv(spark, config)
        else:
            df = read_bronze_csv(spark, config, table_name, schema)
        
        row_count = df.count()
        full_table_name = write_bronze_table(spark, config, table_name, df)
        
        elapsed = time.time() - start_time
        return IngestionResult(table_name, full_table_name, row_count, round(elapsed, 2), True)
    except Exception as e:
        elapsed = time.time() - start_time
        return IngestionResult(table_name, "", 0, round(elapsed, 2), False, str(e))


def ingest_bronze_text_table(
    spark: SparkSession,
    config: PipelineConfig,
    table_name: str
) -> IngestionResult:
    """Ingest a bronze text file table (notes or text_encounters)."""
    start_time = time.time()
    
    try:
        if table_name == "notes":
            df = read_notes_text(spark, config)
        elif table_name == "text_encounters":
            df = read_text_encounters(spark, config)
        else:
            raise ValueError(f"Unknown text table: {table_name}")
        
        row_count = df.count()
        full_table_name = write_bronze_table(spark, config, table_name, df)
        
        elapsed = time.time() - start_time
        return IngestionResult(table_name, full_table_name, row_count, round(elapsed, 2), True)
    except Exception as e:
        elapsed = time.time() - start_time
        return IngestionResult(table_name, "", 0, round(elapsed, 2), False, str(e))


def run_bronze_ingestion(
    spark: SparkSession,
    config: PipelineConfig,
    tables: Optional[List[str]] = None,
    include_text: bool = True
) -> List[IngestionResult]:
    """Run bronze ingestion for all or selected tables."""
    csv_tables = tables or list(BRONZE_SCHEMAS.keys())
    text_tables = ["notes", "text_encounters"] if include_text else []
    
    results = []
    
    print("Ingesting CSV tables...")
    for table in csv_tables:
        print(f"  Ingesting {table}...", end=" ")
        result = ingest_bronze_table(spark, config, table)
        results.append(result)
        status = "OK" if result.success else f"FAILED: {result.error}"
        print(f"{status} - {result.row_count:,} rows in {result.elapsed_seconds}s")
    
    if text_tables:
        print("\nIngesting text file tables...")
        for table in text_tables:
            print(f"  Ingesting {table}...", end=" ")
            result = ingest_bronze_text_table(spark, config, table)
            results.append(result)
            status = "OK" if result.success else f"FAILED: {result.error}"
            print(f"{status} - {result.row_count:,} rows in {result.elapsed_seconds}s")
    
    return results
