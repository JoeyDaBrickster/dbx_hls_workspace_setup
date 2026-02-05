"""
Silver Layer Transformations

Transform Bronze layer data into FHIR R4-compliant resources with enforced schemas.

Lookup Table Enrichments:
-------------------------
The following transformations include enrichment from bronze lookup tables:

1. Conditions (transform_condition):
   - lookup_snomed_icd10cm_map: Maps SNOMED codes to ICD-10-CM diagnosis codes
   
2. Procedures (transform_procedure):
   - lookup_hcpcs_codes: Enriches with HCPCS/CPT procedure details and BETOS codes
   
3. Medications (transform_medication):
   - lookup_rxnorm_concepts: RxNorm preferred names and term types
   - lookup_rxnorm_ndc_crosswalk: Maps RxNorm codes to NDC codes
   - lookup_ndc_product: NDC product details (proprietary name, dosage form, manufacturer)
   
4. Allergies (transform_allergy):
   - lookup_rxnorm_concepts: RxNorm drug names for drug allergies
   
5. Claims (transform_claim):
   - lookup_icd10cm_codes: ICD-10-CM diagnosis descriptions for primary/secondary diagnoses
   
6. Claim Responses (transform_claimresponse):
   - lookup_hcpcs_codes: HCPCS procedure code descriptions

Note: All lookups are left joins - records will not be dropped if lookup tables are missing
or if codes are not found in lookup tables.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Callable
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType, DecimalType

from .config import PipelineConfig, pipe, get_full_table_name


# Regex patterns for type validation
NUMERIC_PATTERN = r"^-?\d+\.?\d*$"  # Matches integers and decimals (e.g., 123, -45.67, 0.5)
INTEGER_PATTERN = r"^-?\d+$"  # Matches integers only (e.g., 123, -45)
DATE_PATTERN = r"^\d{4}-\d{2}-\d{2}$"  # Matches YYYY-MM-DD
TIMESTAMP_PATTERN = r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}"  # Matches ISO8601 timestamps


# Type-safe conversion functions using try_cast with regex validation fallback
def safe_to_date(col_name: str, new_name: Optional[str] = None) -> Callable[[DataFrame], DataFrame]:
    """Safely convert string to date. Returns NULL for malformed input."""
    target = new_name or col_name
    def transform(df: DataFrame) -> DataFrame:
        col_val = F.trim(F.col(col_name))
        is_valid = (
            col_val.isNotNull() & 
            (col_val != "") & 
            col_val.rlike(DATE_PATTERN)
        )
        return df.withColumn(target, 
            F.when(is_valid, F.to_date(col_val)).otherwise(None))
    return transform


def safe_to_timestamp(col_name: str, new_name: Optional[str] = None) -> Callable[[DataFrame], DataFrame]:
    """Safely convert string to timestamp. Returns NULL for malformed input."""
    target = new_name or col_name
    def transform(df: DataFrame) -> DataFrame:
        col_val = F.trim(F.col(col_name))
        is_valid = (
            col_val.isNotNull() & 
            (col_val != "") & 
            col_val.rlike(TIMESTAMP_PATTERN)
        )
        return df.withColumn(target,
            F.when(is_valid, F.to_timestamp(col_val)).otherwise(None))
    return transform


def safe_to_double(col_name: str, new_name: Optional[str] = None) -> Callable[[DataFrame], DataFrame]:
    """Safely convert string to double. Returns NULL for malformed input."""
    target = new_name or col_name
    def transform(df: DataFrame) -> DataFrame:
        col_val = F.trim(F.col(col_name))
        is_valid = (
            col_val.isNotNull() & 
            (col_val != "") & 
            col_val.rlike(NUMERIC_PATTERN)
        )
        return df.withColumn(target,
            F.when(is_valid, col_val.cast(DoubleType())).otherwise(None))
    return transform


def safe_to_decimal(col_name: str, new_name: Optional[str] = None, 
                    precision: int = 18, scale: int = 2) -> Callable[[DataFrame], DataFrame]:
    """Safely convert string to decimal. Returns NULL for malformed input."""
    target = new_name or col_name
    def transform(df: DataFrame) -> DataFrame:
        col_val = F.trim(F.col(col_name))
        is_valid = (
            col_val.isNotNull() & 
            (col_val != "") & 
            col_val.rlike(NUMERIC_PATTERN)
        )
        return df.withColumn(target,
            F.when(is_valid, col_val.cast(DecimalType(precision, scale))).otherwise(None))
    return transform


def safe_to_long(col_name: str, new_name: Optional[str] = None) -> Callable[[DataFrame], DataFrame]:
    """Safely convert string to long. Returns NULL for malformed input."""
    target = new_name or col_name
    def transform(df: DataFrame) -> DataFrame:
        col_val = F.trim(F.col(col_name))
        is_valid = (
            col_val.isNotNull() & 
            (col_val != "") & 
            col_val.rlike(INTEGER_PATTERN)
        )
        return df.withColumn(target,
            F.when(is_valid, col_val.cast(LongType())).otherwise(None))
    return transform


def generate_id(*key_cols: str) -> Callable[[DataFrame], DataFrame]:
    """Generate deterministic SHA-256 ID from key columns."""
    def transform(df: DataFrame) -> DataFrame:
        key_concat = F.concat_ws("|", *[F.coalesce(F.col(c), F.lit("")) for c in key_cols])
        return df.withColumn("id", F.sha2(key_concat, 256))
    return transform


# Transformation functions
def transform_patient(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Transform bronze_patients to flattened patient view."""
    bronze_table = get_full_table_name(config, "bronze", "bronze_patients")
    df = spark.table(bronze_table)
    
    return pipe(
        df,
        safe_to_date("birthdate", "birth_date"),
        safe_to_date("deathdate", "death_date"),
        safe_to_double("lat", "latitude"),
        safe_to_double("lon", "longitude"),
        safe_to_double("healthcare_expenses", "healthcare_expenses_val"),
        safe_to_double("healthcare_coverage", "healthcare_coverage_val"),
        safe_to_double("income", "income_val"),
        lambda df: df.select(
            F.col("id").alias("patient_id"),
            F.col("first").alias("first_name"),
            F.col("middle").alias("middle_name"),
            F.col("last").alias("last_name"),
            F.col("prefix").alias("name_prefix"),
            F.col("suffix").alias("name_suffix"),
            F.lower(F.col("gender")).alias("gender"),
            F.col("birth_date"),
            F.col("death_date"),
            F.col("death_date").isNotNull().alias("is_deceased"),
            F.when(F.col("death_date").isNotNull(),
                F.floor(F.datediff(F.col("death_date"), F.col("birth_date")) / 365.25)
            ).otherwise(
                F.floor(F.datediff(F.current_date(), F.col("birth_date")) / 365.25)
            ).cast("int").alias("age_years"),
            F.col("address").alias("address_line"),
            F.col("city"),
            F.col("state"),
            F.col("zip").alias("postal_code"),
            F.lit("US").alias("country"),
            F.col("county"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("marital").alias("marital_status_code"),
            F.when(F.col("marital") == "M", "Married")
             .when(F.col("marital") == "S", "Never Married")
             .when(F.col("marital") == "D", "Divorced")
             .when(F.col("marital") == "W", "Widowed")
             .otherwise(None).alias("marital_status"),
            F.initcap(F.col("race")).alias("race"),
            F.when(F.col("ethnicity") == "hispanic", "Hispanic or Latino")
             .otherwise("Not Hispanic or Latino").alias("ethnicity"),
            F.col("id").alias("medical_record_number"),
            F.col("ssn").alias("ssn_last_four"),
            F.col("healthcare_expenses_val").alias("healthcare_expenses"),
            F.col("healthcare_coverage_val").alias("healthcare_coverage"),
            F.col("income_val").alias("income"),
            F.col("_rec_src").alias("source_system"),
            F.col("_state").alias("source_state"),
            F.current_timestamp().alias("last_updated"),
        )
    ).filter(F.col("patient_id").isNotNull())


def transform_encounter(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Transform bronze_encounters to flattened encounter view."""
    bronze_table = get_full_table_name(config, "bronze", "bronze_encounters")
    df = spark.table(bronze_table)
    
    return pipe(
        df,
        safe_to_timestamp("start", "start_datetime"),
        safe_to_timestamp("stop", "end_datetime"),
        safe_to_decimal("base_encounter_cost", "base_cost"),
        safe_to_decimal("total_claim_cost", "total_cost"),
        safe_to_decimal("payer_coverage", "payer_cov"),
        lambda df: df.withColumn("duration_minutes",
            F.when(F.col("start_datetime").isNotNull() & F.col("end_datetime").isNotNull(),
                ((F.unix_timestamp(F.col("end_datetime")) - F.unix_timestamp(F.col("start_datetime"))) / 60).cast(LongType())
            ).otherwise(None)),
        lambda df: df.withColumn("patient_responsibility",
            F.coalesce(F.col("total_cost"), F.lit(0)) - F.coalesce(F.col("payer_cov"), F.lit(0))),
        lambda df: df.select(
            F.col("id").alias("encounter_id"),
            F.col("patient").alias("patient_id"),
            F.col("provider").alias("provider_id"),
            F.col("organization").alias("organization_id"),
            F.col("payer").alias("payer_id"),
            F.when(F.lower(F.col("encounterclass")) == "ambulatory", "AMB")
             .when(F.lower(F.col("encounterclass")) == "outpatient", "AMB")
             .when(F.lower(F.col("encounterclass")) == "inpatient", "IMP")
             .when(F.lower(F.col("encounterclass")) == "emergency", "EMER")
             .when(F.lower(F.col("encounterclass")) == "urgentcare", "AMB")
             .when(F.lower(F.col("encounterclass")) == "wellness", "AMB")
             .otherwise("AMB").alias("encounter_class_code"),
            F.col("encounterclass").alias("encounter_class"),
            F.lit("finished").alias("encounter_status"),
            F.col("code").alias("encounter_type_code"),
            F.col("description").alias("encounter_type"),
            F.col("start_datetime"),
            F.col("end_datetime"),
            F.col("duration_minutes"),
            F.col("reasoncode").alias("reason_code"),
            F.col("reasondescription").alias("reason_description"),
            F.col("base_cost"),
            F.col("total_cost"),
            F.col("payer_cov").alias("payer_coverage"),
            F.col("patient_responsibility"),
            (F.lower(F.col("encounterclass")) == "emergency").alias("is_emergency"),
            (F.lower(F.col("encounterclass")) == "inpatient").alias("is_inpatient"),
            F.lower(F.col("encounterclass")).isin(["ambulatory", "outpatient", "wellness", "urgentcare"]).alias("is_ambulatory"),
            F.col("_rec_src").alias("source_system"),
            F.col("_state").alias("source_state"),
            F.current_timestamp().alias("last_updated"),
        )
    ).filter(F.col("encounter_id").isNotNull() & F.col("patient_id").isNotNull())


def transform_condition(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Transform bronze_conditions to flattened condition view with SNOMED→ICD-10 mapping."""
    bronze_table = get_full_table_name(config, "bronze", "bronze_conditions")
    df = spark.table(bronze_table)
    
    # Load SNOMED to ICD-10-CM lookup
    snomed_map_table = get_full_table_name(config, "bronze", "lookup_snomed_icd10cm_map")
    try:
        snomed_map = spark.table(snomed_map_table).select(
            F.col("snomed_concept_id"),
            F.col("icd10cm_code"),
            F.col("icd10cm_name"),
            F.col("map_priority")
        )
    except:
        print(f"Warning: {snomed_map_table} not found, skipping SNOMED→ICD-10 enrichment")
        snomed_map = None
    
    transformed = pipe(
        df,
        generate_id("patient", "code", "start"),
        safe_to_date("start", "onset_date"),
        safe_to_date("stop", "abatement_date"),
        lambda df: df.withColumn("clinical_status",
            F.when(F.col("stop").isNull() | (F.col("stop") == ""), "active").otherwise("resolved")),
        lambda df: df.withColumn("duration_days",
            F.when(F.col("abatement_date").isNotNull(),
                F.datediff(F.col("abatement_date"), F.col("onset_date"))).otherwise(None))
    )
    
    # Join with SNOMED→ICD-10 map if available
    if snomed_map is not None:
        transformed = transformed.join(
            snomed_map,
            transformed["code"] == snomed_map["snomed_concept_id"],
            "left"
        )
    
    return transformed.select(
        F.col("id").alias("condition_id"),
        F.col("patient").alias("patient_id"),
        F.col("encounter").alias("encounter_id"),
        F.col("code").alias("condition_code"),
        F.lit("http://snomed.info/sct").alias("code_system"),
        F.col("description").alias("condition_description"),
        F.coalesce(F.col("icd10cm_code"), F.lit(None)).alias("icd10cm_code") if snomed_map else F.lit(None).alias("icd10cm_code"),
        F.coalesce(F.col("icd10cm_name"), F.lit(None)).alias("icd10cm_name") if snomed_map else F.lit(None).alias("icd10cm_name"),
        F.lit("encounter-diagnosis").alias("category_code"),
        F.lit("Encounter Diagnosis").alias("category_display"),
        F.col("clinical_status"),
        F.lit("confirmed").alias("verification_status"),
        F.col("onset_date"),
        F.col("abatement_date"),
        F.col("onset_date").alias("recorded_date"),
        F.col("duration_days"),
        (F.col("clinical_status") == "active").alias("is_active"),
        (F.col("clinical_status") == "resolved").alias("is_resolved"),
        F.col("_rec_src").alias("source_system"),
        F.current_timestamp().alias("last_updated"),
    ).filter(F.col("patient_id").isNotNull() & F.col("condition_code").isNotNull())


def transform_observation(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Transform bronze_observations to flattened observation view."""
    bronze_table = get_full_table_name(config, "bronze", "bronze_observations")
    df = spark.table(bronze_table)
    
    return pipe(
        df,
        generate_id("patient", "code", "date", "encounter"),
        safe_to_timestamp("date", "effective_datetime"),
        lambda df: df.withColumn("effective_date", F.col("effective_datetime").cast("date")),
        lambda df: df.withColumn("numeric_value",
            F.when(F.col("value").rlike(r"^-?[0-9]*\.?[0-9]+$"),
                   F.col("value").cast(DecimalType(18, 6))).otherwise(None)),
        lambda df: df.withColumn("string_value",
            F.when(~F.col("value").rlike(r"^-?[0-9]*\.?[0-9]+$"), F.col("value")).otherwise(None)),
        lambda df: df.select(
            F.col("id").alias("observation_id"),
            F.col("patient").alias("patient_id"),
            F.col("encounter").alias("encounter_id"),
            F.col("code").alias("observation_code"),
            F.lit("http://loinc.org").alias("code_system"),
            F.col("description").alias("observation_description"),
            F.coalesce(F.lower(F.col("category")), F.lit("laboratory")).alias("category_code"),
            F.coalesce(F.initcap(F.col("category")), F.lit("Laboratory")).alias("category_display"),
            F.lit("final").alias("observation_status"),
            F.col("effective_datetime"),
            F.col("effective_date"),
            F.col("numeric_value"),
            F.col("units").alias("value_unit"),
            F.col("units").alias("unit_code"),
            F.col("string_value"),
            F.coalesce(
                F.concat(F.col("numeric_value").cast("string"), F.lit(" "), F.col("units")),
                F.col("string_value")
            ).alias("display_value"),
            (F.lower(F.col("category")) == "vital-signs").alias("is_vital_sign"),
            (F.lower(F.col("category")) == "laboratory").alias("is_lab_result"),
            (F.lower(F.col("category")) == "survey").alias("is_survey"),
            F.col("_rec_src").alias("source_system"),
            F.current_timestamp().alias("last_updated"),
        )
    ).filter(F.col("patient_id").isNotNull() & F.col("observation_code").isNotNull())


def transform_procedure(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Transform bronze_procedures to flattened procedure view with HCPCS enrichment."""
    bronze_table = get_full_table_name(config, "bronze", "bronze_procedures")
    df = spark.table(bronze_table)
    
    # Load HCPCS lookup
    hcpcs_table = get_full_table_name(config, "bronze", "lookup_hcpcs_codes")
    try:
        hcpcs_lookup = spark.table(hcpcs_table).select(
            F.col("hcpcs_code"),
            F.col("short_description").alias("hcpcs_description"),
            F.col("code_type").alias("hcpcs_code_type"),
            F.col("code_category").alias("hcpcs_category"),
            F.col("betos_code")
        ).filter(F.col("is_active") == True)
    except:
        print(f"Warning: {hcpcs_table} not found, skipping HCPCS enrichment")
        hcpcs_lookup = None
    
    transformed = pipe(
        df,
        generate_id("patient", "code", "start", "encounter"),
        safe_to_timestamp("start", "start_datetime"),
        safe_to_timestamp("stop", "end_datetime"),
        safe_to_decimal("base_cost", "procedure_cost")
    )
    
    # Join with HCPCS lookup if available
    if hcpcs_lookup is not None:
        transformed = transformed.join(
            hcpcs_lookup,
            transformed["code"] == hcpcs_lookup["hcpcs_code"],
            "left"
        )
    
    return transformed.select(
        F.col("id").alias("procedure_id"),
        F.col("patient").alias("patient_id"),
        F.col("encounter").alias("encounter_id"),
        F.col("code").alias("procedure_code"),
        F.lit("http://snomed.info/sct").alias("code_system"),
        F.col("description").alias("procedure_description"),
        F.coalesce(F.col("hcpcs_code"), F.lit(None)).alias("hcpcs_code") if hcpcs_lookup else F.lit(None).alias("hcpcs_code"),
        F.coalesce(F.col("hcpcs_description"), F.lit(None)).alias("hcpcs_description") if hcpcs_lookup else F.lit(None).alias("hcpcs_description"),
        F.coalesce(F.col("hcpcs_code_type"), F.lit(None)).alias("hcpcs_code_type") if hcpcs_lookup else F.lit(None).alias("hcpcs_code_type"),
        F.coalesce(F.col("betos_code"), F.lit(None)).alias("betos_code") if hcpcs_lookup else F.lit(None).alias("betos_code"),
        F.lit("completed").alias("procedure_status"),
        F.col("start_datetime"),
        F.col("end_datetime"),
        F.col("start_datetime").cast("date").alias("performed_date"),
        F.col("procedure_cost"),
        F.col("reasoncode").alias("reason_code"),
        F.col("reasondescription").alias("reason_description"),
        F.col("_rec_src").alias("source_system"),
        F.current_timestamp().alias("last_updated"),
    ).filter(F.col("patient_id").isNotNull() & F.col("procedure_code").isNotNull())


def transform_medication(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Transform bronze_medications to flattened medication view with RxNorm/NDC enrichment."""
    bronze_table = get_full_table_name(config, "bronze", "bronze_medications")
    df = spark.table(bronze_table)
    
    # Load RxNorm concepts lookup
    rxnorm_concepts_table = get_full_table_name(config, "bronze", "lookup_rxnorm_concepts")
    try:
        rxnorm_concepts = spark.table(rxnorm_concepts_table).select(
            F.col("rxcui"),
            F.col("rxnorm_name"),
            F.col("tty").alias("rxnorm_term_type")
        )
    except:
        print(f"Warning: {rxnorm_concepts_table} not found, skipping RxNorm enrichment")
        rxnorm_concepts = None
    
    # Load RxNorm to NDC crosswalk
    rxnorm_ndc_table = get_full_table_name(config, "bronze", "lookup_rxnorm_ndc_crosswalk")
    try:
        rxnorm_ndc = spark.table(rxnorm_ndc_table).select(
            F.col("rxcui"),
            F.col("ndc_11_digit").alias("ndc_code")
        ).dropDuplicates(["rxcui"])  # Get one NDC per RXCUI (could be multiple)
    except:
        print(f"Warning: {rxnorm_ndc_table} not found, skipping RxNorm→NDC crosswalk")
        rxnorm_ndc = None
    
    # Load NDC product lookup
    ndc_product_table = get_full_table_name(config, "bronze", "lookup_ndc_product")
    try:
        ndc_product = spark.table(ndc_product_table).select(
            F.col("product_ndc"),
            F.col("proprietary_name"),
            F.col("nonproprietary_name"),
            F.col("dosage_form_name"),
            F.col("route_name"),
            F.col("labeler_name")
        )
    except:
        print(f"Warning: {ndc_product_table} not found, skipping NDC enrichment")
        ndc_product = None
    
    transformed = pipe(
        df,
        generate_id("patient", "code", "start"),
        safe_to_timestamp("start", "start_datetime"),
        safe_to_timestamp("stop", "end_datetime"),
        safe_to_decimal("base_cost", "base_cost_val"),
        safe_to_decimal("totalcost", "total_cost"),
        safe_to_decimal("payer_coverage", "payer_cov"),
        safe_to_long("dispenses", "dispenses_val"),
        lambda df: df.withColumn("patient_cost",
            F.coalesce(F.col("total_cost"), F.lit(0)) - F.coalesce(F.col("payer_cov"), F.lit(0)))
    )
    
    # Join with RxNorm concepts if available
    if rxnorm_concepts is not None:
        transformed = transformed.join(
            rxnorm_concepts,
            transformed["code"] == rxnorm_concepts["rxcui"],
            "left"
        )
    
    # Join with RxNorm→NDC crosswalk if available
    if rxnorm_ndc is not None:
        transformed = transformed.join(
            rxnorm_ndc,
            transformed["code"] == rxnorm_ndc["rxcui"],
            "left"
        )
    
    # Join with NDC product if available
    if ndc_product is not None and rxnorm_ndc is not None:
        transformed = transformed.join(
            ndc_product,
            F.col("ndc_code") == ndc_product["product_ndc"],
            "left"
        )
    
    return transformed.select(
        F.col("id").alias("medication_id"),
        F.col("patient").alias("patient_id"),
        F.col("encounter").alias("encounter_id"),
        F.col("payer").alias("payer_id"),
        F.col("code").alias("medication_code"),
        F.lit("http://www.nlm.nih.gov/research/umls/rxnorm").alias("code_system"),
        F.col("description").alias("medication_description"),
        F.coalesce(F.col("rxnorm_name"), F.lit(None)).alias("rxnorm_name") if rxnorm_concepts else F.lit(None).alias("rxnorm_name"),
        F.coalesce(F.col("rxnorm_term_type"), F.lit(None)).alias("rxnorm_term_type") if rxnorm_concepts else F.lit(None).alias("rxnorm_term_type"),
        F.coalesce(F.col("ndc_code"), F.lit(None)).alias("ndc_code") if rxnorm_ndc else F.lit(None).alias("ndc_code"),
        F.coalesce(F.col("proprietary_name"), F.lit(None)).alias("ndc_proprietary_name") if ndc_product else F.lit(None).alias("ndc_proprietary_name"),
        F.coalesce(F.col("nonproprietary_name"), F.lit(None)).alias("ndc_nonproprietary_name") if ndc_product else F.lit(None).alias("ndc_nonproprietary_name"),
        F.coalesce(F.col("dosage_form_name"), F.lit(None)).alias("dosage_form") if ndc_product else F.lit(None).alias("dosage_form"),
        F.coalesce(F.col("route_name"), F.lit(None)).alias("route") if ndc_product else F.lit(None).alias("route"),
        F.coalesce(F.col("labeler_name"), F.lit(None)).alias("manufacturer") if ndc_product else F.lit(None).alias("manufacturer"),
        F.when(F.col("stop").isNull() | (F.col("stop") == ""), "active").otherwise("completed").alias("rx_status"),
        F.lit("order").alias("rx_intent"),
        F.col("start_datetime").cast("date").alias("prescribed_date"),
        F.col("start_datetime"),
        F.col("end_datetime"),
        (F.coalesce(F.col("dispenses_val"), F.lit(1)) - 1).alias("refills_allowed"),
        F.col("reasondescription").alias("reason_description"),
        F.col("base_cost_val").alias("base_cost"),
        F.col("total_cost"),
        F.col("payer_cov").alias("payer_coverage"),
        F.col("patient_cost"),
        (F.col("end_datetime").isNull()).alias("is_active"),
        F.col("_rec_src").alias("source_system"),
        F.current_timestamp().alias("last_updated"),
    ).filter(F.col("patient_id").isNotNull() & F.col("medication_code").isNotNull())


def transform_organization(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Transform bronze_organizations to flattened organization view."""
    bronze_table = get_full_table_name(config, "bronze", "bronze_organizations")
    df = spark.table(bronze_table)
    
    return pipe(
        df,
        safe_to_double("lat", "latitude"),
        safe_to_double("lon", "longitude"),
        safe_to_decimal("revenue", "revenue_val"),
        safe_to_long("utilization", "utilization_val"),
        lambda df: df.select(
            F.col("id").alias("organization_id"),
            F.col("name"),
            F.lit(True).alias("active"),
            F.lit("prov").alias("type_code"),
            F.lit("Healthcare Provider").alias("type_display"),
            F.col("address").alias("address_line"),
            F.col("city"),
            F.col("state"),
            F.col("zip").alias("postal_code"),
            F.lit("US").alias("country"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("phone"),
            F.col("revenue_val").alias("revenue"),
            F.col("utilization_val").alias("utilization"),
            F.col("_rec_src").alias("source_system"),
            F.current_timestamp().alias("last_updated"),
        )
    ).filter(F.col("organization_id").isNotNull())


def transform_practitioner(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Transform bronze_providers to flattened practitioner view."""
    bronze_table = get_full_table_name(config, "bronze", "bronze_providers")
    df = spark.table(bronze_table)
    
    return pipe(
        df,
        safe_to_double("lat", "latitude"),
        safe_to_double("lon", "longitude"),
        safe_to_long("encounters", "encounter_count"),
        safe_to_long("procedures", "procedure_count"),
        lambda df: df.select(
            F.col("id").alias("practitioner_id"),
            F.col("organization").alias("organization_id"),
            F.col("name"),
            F.lit(True).alias("active"),
            F.lower(F.col("gender")).alias("gender"),
            F.col("speciality").alias("specialty"),
            F.col("speciality").alias("specialty_code"),
            F.col("address").alias("address_line"),
            F.col("city"),
            F.col("state"),
            F.col("zip").alias("postal_code"),
            F.lit("US").alias("country"),
            F.col("latitude"),
            F.col("longitude"),
            F.col("encounter_count"),
            F.col("procedure_count"),
            F.col("_rec_src").alias("source_system"),
            F.current_timestamp().alias("last_updated"),
        )
    ).filter(F.col("practitioner_id").isNotNull())


def transform_allergy(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Transform bronze_allergies to flattened allergy intolerance view with RxNorm enrichment."""
    bronze_table = get_full_table_name(config, "bronze", "bronze_allergies")
    df = spark.table(bronze_table)
    
    # Load RxNorm concepts for drug allergies
    rxnorm_concepts_table = get_full_table_name(config, "bronze", "lookup_rxnorm_concepts")
    try:
        rxnorm_concepts = spark.table(rxnorm_concepts_table).select(
            F.col("rxcui"),
            F.col("rxnorm_name"),
            F.col("tty").alias("rxnorm_term_type")
        )
    except:
        print(f"Warning: {rxnorm_concepts_table} not found, skipping RxNorm enrichment for allergies")
        rxnorm_concepts = None
    
    transformed = pipe(
        df,
        generate_id("patient", "code", "start"),
        safe_to_date("start", "onset_date"),
        safe_to_date("stop", "resolution_date"),
        lambda df: df.withColumn("clinical_status",
            F.when(F.col("stop").isNull() | (F.col("stop") == ""), "active").otherwise("resolved")),
        lambda df: df.withColumn("criticality",
            F.when(
                (F.lower(F.col("severity1")) == "severe") | (F.lower(F.col("severity2")) == "severe"),
                "high"
            ).otherwise("low"))
    )
    
    # Join with RxNorm for drug allergies if available
    if rxnorm_concepts is not None:
        transformed = transformed.join(
            rxnorm_concepts,
            transformed["code"] == rxnorm_concepts["rxcui"],
            "left"
        )
    
    return transformed.select(
        F.col("id").alias("allergy_id"),
        F.col("patient").alias("patient_id"),
        F.col("encounter").alias("encounter_id"),
        F.col("code").alias("allergy_code"),
        F.coalesce(F.col("system"), F.lit("http://snomed.info/sct")).alias("code_system"),
        F.col("description").alias("allergy_description"),
        F.coalesce(F.col("rxnorm_name"), F.lit(None)).alias("rxnorm_name") if rxnorm_concepts else F.lit(None).alias("rxnorm_name"),
        F.coalesce(F.col("rxnorm_term_type"), F.lit(None)).alias("rxnorm_term_type") if rxnorm_concepts else F.lit(None).alias("rxnorm_term_type"),
        F.col("clinical_status"),
        F.lit("confirmed").alias("verification_status"),
        F.coalesce(F.lower(F.col("type")), F.lit("allergy")).alias("allergy_type"),
        F.coalesce(F.lower(F.col("category")), F.lit("environment")).alias("category"),
        F.col("criticality"),
        F.col("onset_date"),
        F.col("onset_date").alias("recorded_date"),
        F.col("resolution_date"),
        F.col("reaction1").alias("primary_reaction_code"),
        F.col("description1").alias("primary_reaction"),
        F.when(F.lower(F.col("severity1")) == "severe", "severe")
         .when(F.lower(F.col("severity1")) == "moderate", "moderate")
         .otherwise("mild").alias("reaction_severity"),
        (F.col("clinical_status") == "active").alias("is_active"),
        (F.col("criticality") == "high").alias("is_high_criticality"),
        F.col("_rec_src").alias("source_system"),
        F.current_timestamp().alias("last_updated"),
    ).filter(F.col("patient_id").isNotNull() & F.col("allergy_code").isNotNull())


def transform_careplan(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Transform bronze_careplans to flattened careplan view."""
    bronze_table = get_full_table_name(config, "bronze", "bronze_careplans")
    df = spark.table(bronze_table)
    
    return pipe(
        df,
        safe_to_date("start", "start_date"),
        safe_to_date("stop", "end_date"),
        lambda df: df.withColumn("careplan_status",
            F.when(F.col("stop").isNull() | (F.col("stop") == ""), "active").otherwise("completed")),
        lambda df: df.withColumn("duration_days",
            F.when(F.col("end_date").isNotNull(),
                F.datediff(F.col("end_date"), F.col("start_date"))
            ).otherwise(
                F.datediff(F.current_date(), F.col("start_date"))
            )),
        lambda df: df.select(
            F.col("id").alias("careplan_id"),
            F.col("patient").alias("patient_id"),
            F.col("encounter").alias("encounter_id"),
            F.col("code").alias("careplan_code"),
            F.col("description").alias("careplan_description"),
            F.col("careplan_status"),
            F.lit("order").alias("careplan_intent"),
            F.col("start_date"),
            F.col("end_date"),
            F.col("duration_days"),
            F.col("reasoncode").alias("reason_code"),
            F.col("reasondescription").alias("reason_description"),
            F.when(F.col("end_date").isNotNull(),
                F.when(F.col("end_date").isNull(), "in-progress").otherwise("completed")
            ).otherwise("in-progress").alias("activity_status"),
            (F.col("careplan_status") == "active").alias("is_active"),
            F.col("_rec_src").alias("source_system"),
            F.current_timestamp().alias("last_updated"),
        )
    ).filter(F.col("careplan_id").isNotNull() & F.col("patient_id").isNotNull())


def transform_claim(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Transform bronze_claims to flattened claim view with ICD-10-CM enrichment."""
    bronze_table = get_full_table_name(config, "bronze", "bronze_claims")
    df = spark.table(bronze_table)
    
    # Load ICD-10-CM lookup for diagnosis codes
    icd10cm_table = get_full_table_name(config, "bronze", "lookup_icd10cm_codes")
    try:
        icd10cm_lookup = spark.table(icd10cm_table).select(
            F.col("icd10cm_code"),
            F.col("short_description").alias("icd10cm_short_desc"),
            F.col("long_description").alias("icd10cm_long_desc")
        )
    except:
        print(f"Warning: {icd10cm_table} not found, skipping ICD-10-CM enrichment")
        icd10cm_lookup = None
    
    transformed = pipe(
        df,
        safe_to_date("servicedate", "service_date"),
        safe_to_date("currentillnessdate", "illness_date"),
        safe_to_decimal("outstanding1", "outstanding1_val"),
        safe_to_decimal("outstanding2", "outstanding2_val"),
        lambda df: df.withColumn("total_outstanding",
            F.coalesce(F.col("outstanding1_val"), F.lit(0)) + F.coalesce(F.col("outstanding2_val"), F.lit(0))),
        lambda df: df.withColumn("diagnosis_count",
            F.size(F.array_remove(F.array(
                F.col("diagnosis1"), F.col("diagnosis2"), F.col("diagnosis3"), F.col("diagnosis4"),
                F.col("diagnosis5"), F.col("diagnosis6"), F.col("diagnosis7"), F.col("diagnosis8")
            ), None)))
    )
    
    # Join with ICD-10-CM lookup for primary diagnosis if available
    if icd10cm_lookup is not None:
        icd10cm_primary = icd10cm_lookup.selectExpr(
            "icd10cm_code as dx1_code",
            "icd10cm_short_desc as dx1_short_desc",
            "icd10cm_long_desc as dx1_long_desc"
        )
        transformed = transformed.join(
            icd10cm_primary,
            transformed["diagnosis1"] == icd10cm_primary["dx1_code"],
            "left"
        )
        
        # Join for secondary diagnosis
        icd10cm_secondary = icd10cm_lookup.selectExpr(
            "icd10cm_code as dx2_code",
            "icd10cm_short_desc as dx2_short_desc",
            "icd10cm_long_desc as dx2_long_desc"
        )
        transformed = transformed.join(
            icd10cm_secondary,
            transformed["diagnosis2"] == icd10cm_secondary["dx2_code"],
            "left"
        )
    
    return transformed.select(
        F.col("id").alias("claim_id"),
        F.col("patientid").alias("patient_id"),
        F.col("providerid").alias("provider_id"),
        F.col("primarypatientinsuranceid").alias("primary_coverage_id"),
        F.col("secondarypatientinsuranceid").alias("secondary_coverage_id"),
        F.lit("professional").alias("claim_type_code"),
        F.lit("Professional").alias("claim_type"),
        F.lit("active").alias("claim_status"),
        F.lit("claim").alias("claim_use"),
        F.coalesce(F.col("service_date"), F.col("illness_date")).alias("service_date"),
        F.coalesce(F.col("service_date"), F.col("illness_date")).alias("created_date"),
        F.col("diagnosis1").alias("primary_diagnosis_code"),
        F.coalesce(F.col("dx1_short_desc"), F.lit(None)).alias("primary_diagnosis_desc") if icd10cm_lookup else F.lit(None).alias("primary_diagnosis_desc"),
        F.col("diagnosis2").alias("secondary_diagnosis_code"),
        F.coalesce(F.col("dx2_short_desc"), F.lit(None)).alias("secondary_diagnosis_desc") if icd10cm_lookup else F.lit(None).alias("secondary_diagnosis_desc"),
        F.col("diagnosis_count"),
        F.col("total_outstanding").alias("total_amount"),
        F.col("appointmentid").alias("encounter_id"),
        F.col("_rec_src").alias("source_system"),
        F.current_timestamp().alias("last_updated"),
    ).filter(F.col("claim_id").isNotNull() & F.col("patient_id").isNotNull())


def transform_claimresponse(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Transform bronze_claims_transactions to flattened claim response view with HCPCS enrichment."""
    bronze_table = get_full_table_name(config, "bronze", "bronze_claims_transactions")
    df = spark.table(bronze_table)
    
    # Load HCPCS lookup for procedure codes
    hcpcs_table = get_full_table_name(config, "bronze", "lookup_hcpcs_codes")
    try:
        hcpcs_lookup = spark.table(hcpcs_table).select(
            F.col("hcpcs_code"),
            F.col("short_description").alias("hcpcs_description"),
            F.col("code_type").alias("hcpcs_code_type")
        ).filter(F.col("is_active") == True)
    except:
        print(f"Warning: {hcpcs_table} not found, skipping HCPCS enrichment for claim responses")
        hcpcs_lookup = None
    
    transformed = pipe(
        df,
        safe_to_date("fromdate", "from_date"),
        safe_to_date("todate", "to_date"),
        safe_to_decimal("amount", "submitted_amount"),
        safe_to_decimal("payments", "payment_amount"),
        safe_to_decimal("outstanding", "outstanding_amount"),
        safe_to_decimal("adjustments", "adjustment_amount"),
        lambda df: df.withColumn("outcome",
            F.when(F.coalesce(F.col("outstanding_amount"), F.lit(0)) == 0, "complete")
             .when(F.coalesce(F.col("payment_amount"), F.lit(0)) > 0, "partial")
             .otherwise("queued"))
    )
    
    # Join with HCPCS lookup if available
    if hcpcs_lookup is not None:
        transformed = transformed.join(
            hcpcs_lookup,
            transformed["procedurecode"] == hcpcs_lookup["hcpcs_code"],
            "left"
        )
    
    return transformed.select(
        F.col("id").alias("claimresponse_id"),
        F.col("claimid").alias("claim_id"),
        F.col("patientid").alias("patient_id"),
        F.lit("active").alias("response_status"),
        F.col("outcome"),
        F.col("from_date").alias("created_date"),
        F.col("submitted_amount"),
        F.col("payment_amount").alias("benefit_amount"),
        F.col("payment_amount"),
        F.col("to_date").alias("payment_date"),
        F.col("outstanding_amount"),
        F.col("procedurecode").alias("procedure_code"),
        F.coalesce(F.col("hcpcs_description"), F.lit(None)).alias("procedure_description") if hcpcs_lookup else F.lit(None).alias("procedure_description"),
        F.coalesce(F.col("hcpcs_code_type"), F.lit(None)).alias("procedure_code_type") if hcpcs_lookup else F.lit(None).alias("procedure_code_type"),
        (F.col("outcome") == "complete").alias("is_complete"),
        (F.col("outcome") == "partial").alias("is_partial"),
        F.col("_rec_src").alias("source_system"),
        F.current_timestamp().alias("last_updated"),
    ).filter(F.col("claimresponse_id").isNotNull() & F.col("claim_id").isNotNull())


def transform_coverage(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Transform bronze_payer_transitions to flattened coverage view."""
    bronze_table = get_full_table_name(config, "bronze", "bronze_payer_transitions")
    df = spark.table(bronze_table)
    
    return pipe(
        df,
        generate_id("patient", "payer", "start_date"),
        safe_to_date("start_date", "start_dt"),
        safe_to_date("end_date", "end_dt"),
        lambda df: df.withColumn("coverage_status",
            F.when(
                F.col("end_dt").isNull() | (F.col("end_dt") >= F.current_date()),
                "active"
            ).otherwise("cancelled")),
        lambda df: df.withColumn("coverage_duration_days",
            F.when(F.col("end_dt").isNull(),
                F.datediff(F.current_date(), F.col("start_dt"))
            ).otherwise(
                F.datediff(F.col("end_dt"), F.col("start_dt"))
            )),
        lambda df: df.withColumn("relationship_code",
            F.when(F.lower(F.coalesce(F.col("plan_ownership"), F.lit("self"))) == "self", "self")
             .when(F.lower(F.col("plan_ownership")) == "spouse", "spouse")
             .when(F.lower(F.col("plan_ownership")) == "guardian", "parent")
             .otherwise("self")),
        lambda df: df.select(
            F.col("id").alias("coverage_id"),
            F.col("patient").alias("patient_id"),
            F.col("payer").alias("payer_id"),
            F.col("payer").alias("payer_name"),
            F.col("memberid").alias("member_id"),
            F.col("coverage_status"),
            F.lit("EHCPOL").alias("coverage_type_code"),
            F.lit("Extended healthcare").alias("coverage_type"),
            F.col("relationship_code"),
            F.when(F.col("relationship_code") == "self", "Self")
             .when(F.col("relationship_code") == "spouse", "Spouse")
             .when(F.col("relationship_code") == "parent", "Parent")
             .otherwise("Self").alias("relationship"),
            F.col("start_dt").alias("start_date"),
            F.col("end_dt").alias("end_date"),
            F.col("owner_name"),
            (F.col("coverage_status") == "active").alias("is_active"),
            F.col("coverage_duration_days"),
            F.col("_rec_src").alias("source_system"),
            F.current_timestamp().alias("last_updated"),
        )
    ).filter(F.col("patient_id").isNotNull() & F.col("payer_id").isNotNull())


def transform_device(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Transform bronze_devices to flattened device view."""
    bronze_table = get_full_table_name(config, "bronze", "bronze_devices")
    df = spark.table(bronze_table)
    
    return pipe(
        df,
        lambda df: df.withColumn("device_id_generated", 
            F.coalesce(F.col("udi"), F.sha2(F.concat_ws("|", F.col("patient"), F.col("code"), F.col("start")), 256))),
        safe_to_date("start", "association_start"),
        safe_to_date("stop", "association_end"),
        lambda df: df.withColumn("device_status",
            F.when(F.col("stop").isNull() | (F.col("stop") == ""), "active").otherwise("inactive")),
        lambda df: df.select(
            F.col("device_id_generated").alias("device_id"),
            F.col("patient").alias("patient_id"),
            F.col("encounter").alias("encounter_id"),
            F.col("udi"),
            F.col("code").alias("device_code"),
            F.col("description").alias("device_description"),
            F.col("description").alias("device_name"),
            F.col("device_status"),
            F.col("association_start"),
            F.col("association_end"),
            (F.col("device_status") == "active").alias("is_active"),
            F.col("udi").isNotNull().alias("has_udi"),
            F.col("_rec_src").alias("source_system"),
            F.current_timestamp().alias("last_updated"),
        )
    ).filter(F.col("patient_id").isNotNull())


def transform_imagingstudy(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Transform bronze_imaging_studies to flattened imaging study view."""
    bronze_table = get_full_table_name(config, "bronze", "bronze_imaging_studies")
    df = spark.table(bronze_table)
    
    return pipe(
        df,
        safe_to_date("date", "study_date"),
        lambda df: df.select(
            F.col("id").alias("study_id"),
            F.col("patient").alias("patient_id"),
            F.col("encounter").alias("encounter_id"),
            F.lit("available").alias("study_status"),
            F.col("study_date"),
            F.col("series_uid"),
            F.col("modality_code"),
            F.col("modality_description"),
            F.col("bodysite_code"),
            F.col("bodysite_description"),
            F.col("instance_uid"),
            F.col("sop_code"),
            F.col("sop_description"),
            F.col("procedure_code"),
            F.col("_rec_src").alias("source_system"),
            F.current_timestamp().alias("last_updated"),
        )
    ).filter(F.col("study_id").isNotNull() & F.col("patient_id").isNotNull())


def transform_immunization(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Transform bronze_immunizations to flattened immunization view."""
    bronze_table = get_full_table_name(config, "bronze", "bronze_immunizations")
    df = spark.table(bronze_table)
    
    return pipe(
        df,
        generate_id("patient", "code", "date"),
        safe_to_date("date", "administration_date"),
        safe_to_decimal("base_cost", "cost"),
        lambda df: df.select(
            F.col("id").alias("immunization_id"),
            F.col("patient").alias("patient_id"),
            F.col("encounter").alias("encounter_id"),
            F.col("code").alias("vaccine_code"),
            F.lit("http://hl7.org/fhir/sid/cvx").alias("code_system"),
            F.col("description").alias("vaccine_description"),
            F.lit("completed").alias("immunization_status"),
            F.col("administration_date"),
            F.col("administration_date").alias("recorded_date"),
            F.lit(True).alias("is_primary_source"),
            F.col("cost"),
            F.col("_rec_src").alias("source_system"),
            F.current_timestamp().alias("last_updated"),
        )
    ).filter(F.col("patient_id").isNotNull() & F.col("vaccine_code").isNotNull())


def transform_payer(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Transform bronze_payers to flattened payer view."""
    bronze_table = get_full_table_name(config, "bronze", "bronze_payers")
    df = spark.table(bronze_table)
    
    return pipe(
        df,
        safe_to_decimal("amount_covered", "amount_covered_val"),
        safe_to_decimal("amount_uncovered", "amount_uncovered_val"),
        safe_to_decimal("revenue", "revenue_val"),
        safe_to_long("covered_encounters", "covered_encounters_val"),
        safe_to_long("uncovered_encounters", "uncovered_encounters_val"),
        safe_to_long("covered_medications", "covered_medications_val"),
        safe_to_long("uncovered_medications", "uncovered_medications_val"),
        safe_to_long("covered_procedures", "covered_procedures_val"),
        safe_to_long("uncovered_procedures", "uncovered_procedures_val"),
        safe_to_long("covered_immunizations", "covered_immunizations_val"),
        safe_to_long("uncovered_immunizations", "uncovered_immunizations_val"),
        safe_to_long("unique_customers", "unique_customers_val"),
        safe_to_decimal("qols_avg", "qols_avg_val", 10, 2),
        safe_to_long("member_months", "member_months_val"),
        lambda df: df.withColumn("coverage_ratio",
            F.col("amount_covered_val") / (F.col("amount_covered_val") + F.col("amount_uncovered_val"))),
        lambda df: df.select(
            F.col("id").alias("payer_id"),
            F.col("name").alias("payer_name"),
            F.col("ownership").alias("ownership_type"),
            F.lit(True).alias("is_active"),
            F.col("phone"),
            F.col("city"),
            F.col("state_headquartered").alias("state"),
            F.col("zip").alias("postal_code"),
            F.col("amount_covered_val").alias("amount_covered"),
            F.col("amount_uncovered_val").alias("amount_uncovered"),
            F.col("revenue_val").alias("revenue"),
            F.col("coverage_ratio"),
            F.col("covered_encounters_val").alias("covered_encounters"),
            F.col("uncovered_encounters_val").alias("uncovered_encounters"),
            F.col("covered_medications_val").alias("covered_medications"),
            F.col("uncovered_medications_val").alias("uncovered_medications"),
            F.col("covered_procedures_val").alias("covered_procedures"),
            F.col("uncovered_procedures_val").alias("uncovered_procedures"),
            F.col("covered_immunizations_val").alias("covered_immunizations"),
            F.col("uncovered_immunizations_val").alias("uncovered_immunizations"),
            F.col("unique_customers_val").alias("unique_customers"),
            F.col("qols_avg_val").alias("quality_of_life_score_avg"),
            F.col("member_months_val").alias("member_months"),
            F.col("_rec_src").alias("source_system"),
            F.current_timestamp().alias("last_updated"),
        )
    ).filter(F.col("payer_id").isNotNull())


def transform_supply(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Transform bronze_supplies to flattened supply delivery view."""
    bronze_table = get_full_table_name(config, "bronze", "bronze_supplies")
    df = spark.table(bronze_table)
    
    return pipe(
        df,
        generate_id("patient", "code", "date", "encounter"),
        safe_to_date("date", "delivery_date"),
        safe_to_long("quantity", "quantity_val"),
        lambda df: df.select(
            F.col("id").alias("supply_id"),
            F.col("patient").alias("patient_id"),
            F.col("encounter").alias("encounter_id"),
            F.col("code").alias("supply_code"),
            F.col("description").alias("supply_description"),
            F.coalesce(F.col("quantity_val"), F.lit(1)).alias("quantity"),
            F.lit("unit").alias("unit"),
            F.lit("completed").alias("delivery_status"),
            F.lit("device").alias("supply_type_code"),
            F.lit("Device").alias("supply_type"),
            F.col("delivery_date"),
            F.col("encounter").isNotNull().alias("has_encounter"),
            F.col("_rec_src").alias("source_system"),
            F.current_timestamp().alias("last_updated"),
        )
    ).filter(F.col("patient_id").isNotNull() & F.col("supply_code").isNotNull())


# Silver table registry
@dataclass(frozen=True)
class SilverTableDef:
    """Definition of a silver table."""
    name: str
    view_name: str
    transform_fn: Callable[[SparkSession, PipelineConfig], DataFrame]
    resource_type: str


SILVER_TABLE_REGISTRY: Dict[str, SilverTableDef] = {
    # Core clinical resources
    "patient": SilverTableDef("patient", "v_patient_flattened", transform_patient, "Patient"),
    "encounter": SilverTableDef("encounter", "v_encounter_flattened", transform_encounter, "Encounter"),
    "condition": SilverTableDef("condition", "v_condition_flattened", transform_condition, "Condition"),
    "observation": SilverTableDef("observation", "v_observation_flattened", transform_observation, "Observation"),
    "procedure": SilverTableDef("procedure", "v_procedure_flattened", transform_procedure, "Procedure"),
    "medication": SilverTableDef("medicationrequest", "v_medication_flattened", transform_medication, "MedicationRequest"),
    # Administrative resources
    "organization": SilverTableDef("organization", "v_organization_flattened", transform_organization, "Organization"),
    "practitioner": SilverTableDef("practitioner", "v_practitioner_flattened", transform_practitioner, "Practitioner"),
    "payer": SilverTableDef("payer", "v_payer_flattened", transform_payer, "Organization"),
    # Additional clinical resources
    "allergy": SilverTableDef("allergyintolerance", "v_allergy_flattened", transform_allergy, "AllergyIntolerance"),
    "careplan": SilverTableDef("careplan", "v_careplan_flattened", transform_careplan, "CarePlan"),
    "device": SilverTableDef("device", "v_device_flattened", transform_device, "Device"),
    "imagingstudy": SilverTableDef("imagingstudy", "v_imagingstudy_flattened", transform_imagingstudy, "ImagingStudy"),
    "immunization": SilverTableDef("immunization", "v_immunization_flattened", transform_immunization, "Immunization"),
    "supply": SilverTableDef("supplydelivery", "v_supply_flattened", transform_supply, "SupplyDelivery"),
    # Financial/coverage resources
    "claim": SilverTableDef("claim", "v_claim_flattened", transform_claim, "Claim"),
    "claimresponse": SilverTableDef("claimresponse", "v_claimresponse_flattened", transform_claimresponse, "ClaimResponse"),
    "coverage": SilverTableDef("coverage", "v_coverage_flattened", transform_coverage, "Coverage"),
}


@dataclass
class TransformResult:
    """Result of a silver transformation."""
    table_name: str
    view_name: str
    row_count: int
    elapsed_seconds: float
    success: bool
    resource_type: str
    error: Optional[str] = None


def write_silver_table(
    spark: SparkSession,
    config: PipelineConfig,
    table_def: SilverTableDef
) -> TransformResult:
    """Write a silver table."""
    start_time = time.time()
    try:
        df = table_def.transform_fn(spark, config)
        full_table = get_full_table_name(config, "silver", table_def.view_name)
        df.write.format("delta").mode("overwrite").saveAsTable(full_table)
        row_count = spark.table(full_table).count()
        elapsed = time.time() - start_time
        print(f"  Created {full_table} with {row_count:,} rows in {elapsed:.2f}s")
        return TransformResult(table_def.name, table_def.view_name, row_count, elapsed, True, table_def.resource_type)
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"  ERROR: {table_def.name} - {str(e)}")
        return TransformResult(table_def.name, table_def.view_name, 0, elapsed, False, table_def.resource_type, str(e))


def run_all_silver_transforms(
    spark: SparkSession,
    config: PipelineConfig
) -> List[TransformResult]:
    """Run all silver transformations."""
    results = []
    print(f"Running silver transformations for {len(SILVER_TABLE_REGISTRY)} tables...")
    for name, table_def in SILVER_TABLE_REGISTRY.items():
        result = write_silver_table(spark, config, table_def)
        results.append(result)
    successful = sum(1 for r in results if r.success)
    total_rows = sum(r.row_count for r in results)
    print(f"\nCompleted: {successful}/{len(results)} tables, {total_rows:,} total rows")
    return results
