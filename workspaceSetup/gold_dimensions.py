"""
Gold Layer: Dimensions, Facts, and Aggregates

Create dimensional models from Silver FHIR-aligned data for analytics.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Callable, Tuple
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

from .config import PipelineConfig, get_full_table_name


def surrogate_key(*cols: str) -> F.Column:
    """Generate SHA-256 surrogate key from columns."""
    return F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c), F.lit("")) for c in cols]), 256)


def date_key(col_name: str) -> F.Column:
    """Generate integer date key (YYYYMMDD)."""
    return F.date_format(F.col(col_name), "yyyyMMdd").cast(IntegerType())


# Dimension builders
def build_dim_date(spark: SparkSession, config: PipelineConfig, start_year: int = 2000, end_year: int = 2030) -> DataFrame:
    """Build date dimension table."""
    dates = spark.sql(f"""
        SELECT explode(sequence(
            to_date('{start_year}-01-01'), 
            to_date('{end_year}-12-31'), 
            interval 1 day
        )) as date
    """)
    
    return dates.select(
        F.date_format(F.col("date"), "yyyyMMdd").cast(IntegerType()).alias("date_key"),
        F.col("date"),
        F.year(F.col("date")).alias("year"),
        F.quarter(F.col("date")).alias("quarter"),
        F.month(F.col("date")).alias("month"),
        F.date_format(F.col("date"), "MMMM").alias("month_name"),
        F.weekofyear(F.col("date")).alias("week_of_year"),
        F.dayofmonth(F.col("date")).alias("day_of_month"),
        F.dayofweek(F.col("date")).alias("day_of_week"),
        F.date_format(F.col("date"), "EEEE").alias("day_name"),
        F.when(F.dayofweek(F.col("date")).isin([1, 7]), True).otherwise(False).alias("is_weekend"),
        F.date_trunc("month", F.col("date")).alias("month_start"),
        F.last_day(F.col("date")).alias("month_end"),
        F.date_trunc("quarter", F.col("date")).alias("quarter_start"),
        F.date_trunc("year", F.col("date")).alias("year_start"),
    )


def build_dim_patient(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build patient dimension from silver."""
    silver_table = get_full_table_name(config, "silver", "v_patient_flattened")
    df = spark.table(silver_table)
    
    return df.select(
        surrogate_key("patient_id").alias("patient_key"),
        F.col("patient_id"),
        F.col("first_name"),
        F.col("middle_name"),
        F.col("last_name"),
        F.concat_ws(" ", F.col("name_prefix"), F.col("first_name"), F.col("middle_name"), F.col("last_name"), F.col("name_suffix")).alias("full_name"),
        F.col("gender"),
        F.col("birth_date"),
        F.col("death_date"),
        F.col("is_deceased"),
        F.col("age_years"),
        F.when(F.col("age_years") < 18, "Pediatric (0-17)")
         .when(F.col("age_years").between(18, 34), "Young Adult (18-34)")
         .when(F.col("age_years").between(35, 49), "Adult (35-49)")
         .when(F.col("age_years").between(50, 64), "Middle Age (50-64)")
         .when(F.col("age_years") >= 65, "Senior (65+)")
         .otherwise("Unknown").alias("age_group"),
        F.col("address_line").alias("address"),
        F.col("city"),
        F.col("state"),
        F.col("postal_code").alias("zip_code"),
        F.col("country"),
        F.col("marital_status"),
        F.col("race"),
        F.col("ethnicity"),
        F.col("medical_record_number").alias("mrn"),
        F.col("source_system"),
        F.col("last_updated").alias("source_last_updated"),
        F.current_timestamp().alias("dim_created_dts"),
        F.current_timestamp().alias("dim_refresh_dts"),
    ).filter(F.col("patient_id").isNotNull())


def build_dim_provider(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build provider dimension from silver."""
    silver_table = get_full_table_name(config, "silver", "v_practitioner_flattened")
    df = spark.table(silver_table)
    
    return df.select(
        surrogate_key("practitioner_id").alias("provider_key"),
        F.col("practitioner_id").alias("provider_id"),
        F.col("name").alias("provider_name"),
        F.col("gender"),
        F.col("specialty"),
        F.col("specialty_code"),
        surrogate_key("organization_id").alias("organization_key"),
        F.col("organization_id"),
        F.col("address_line").alias("address"),
        F.col("city"),
        F.col("state"),
        F.col("postal_code").alias("zip_code"),
        F.col("encounter_count"),
        F.col("procedure_count"),
        F.col("active"),
        F.col("source_system"),
        F.current_timestamp().alias("dim_created_dts"),
        F.current_timestamp().alias("dim_refresh_dts"),
    ).filter(F.col("provider_id").isNotNull())


def build_dim_organization(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build organization dimension from silver."""
    silver_table = get_full_table_name(config, "silver", "v_organization_flattened")
    df = spark.table(silver_table)
    
    return df.select(
        surrogate_key("organization_id").alias("organization_key"),
        F.col("organization_id"),
        F.col("name"),
        F.col("type_code"),
        F.col("type_display"),
        F.col("address_line").alias("address"),
        F.col("city"),
        F.col("state"),
        F.col("postal_code").alias("zip_code"),
        F.col("phone"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("revenue"),
        F.col("utilization"),
        F.when(F.col("utilization") >= 10000, "Large")
         .when(F.col("utilization") >= 1000, "Medium")
         .when(F.col("utilization") >= 100, "Small")
         .otherwise("Very Small").alias("size_classification"),
        F.col("active"),
        F.col("source_system"),
        F.current_timestamp().alias("dim_created_dts"),
        F.current_timestamp().alias("dim_refresh_dts"),
    ).filter(F.col("organization_id").isNotNull())


def build_dim_payer(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build payer dimension from silver."""
    silver_table = get_full_table_name(config, "silver", "v_payer_flattened")
    df = spark.table(silver_table)
    
    return df.select(
        surrogate_key("payer_id").alias("payer_key"),
        F.col("payer_id"),
        F.col("payer_name"),
        F.col("ownership_type"),
        F.col("is_active"),
        F.col("phone"),
        F.col("city"),
        F.col("state"),
        F.col("postal_code").alias("zip_code"),
        F.col("amount_covered"),
        F.col("amount_uncovered"),
        F.col("revenue"),
        F.col("coverage_ratio"),
        F.col("covered_encounters"),
        F.col("uncovered_encounters"),
        (F.col("covered_encounters") + F.col("uncovered_encounters")).alias("total_encounters"),
        F.col("covered_medications"),
        F.col("uncovered_medications"),
        F.col("covered_procedures"),
        F.col("uncovered_procedures"),
        F.col("covered_immunizations"),
        F.col("uncovered_immunizations"),
        F.col("unique_customers").alias("member_count"),
        F.col("quality_of_life_score_avg"),
        F.col("member_months"),
        F.when(
            (F.col("covered_encounters") + F.col("uncovered_encounters")) > 0,
            F.round(F.col("covered_encounters") * 100.0 / (F.col("covered_encounters") + F.col("uncovered_encounters")), 2)
        ).otherwise(F.lit(0)).alias("encounter_coverage_rate_pct"),
        F.when(F.col("member_months") > 0, F.round(F.col("amount_covered") / F.col("member_months"), 2))
         .otherwise(F.lit(0)).alias("pmpm_covered"),
        F.when(F.col("coverage_ratio") >= 0.8, "Premium")
         .when(F.col("coverage_ratio") >= 0.6, "Standard")
         .when(F.col("coverage_ratio") >= 0.4, "Basic")
         .otherwise("Minimal").alias("payer_tier"),
        F.col("source_system"),
        F.current_timestamp().alias("dim_created_dts"),
        F.current_timestamp().alias("dim_refresh_dts"),
    ).filter(F.col("payer_id").isNotNull())


def build_dim_encounter_type(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build encounter type reference dimension from silver."""
    silver_table = get_full_table_name(config, "silver", "v_encounter_flattened")
    df = spark.table(silver_table)
    
    return df.select(
        F.col("encounter_class_code"),
        F.col("encounter_class"),
        F.col("encounter_type_code"),
        F.col("encounter_type")
    ).distinct().select(
        surrogate_key("encounter_class_code", "encounter_type_code").alias("encounter_type_key"),
        F.col("encounter_class_code"),
        F.col("encounter_class"),
        F.col("encounter_type_code"),
        F.col("encounter_type"),
        F.when(F.col("encounter_class_code") == "EMER", "Emergency")
         .when(F.col("encounter_class_code") == "IMP", "Inpatient")
         .when(F.col("encounter_class_code") == "AMB", "Outpatient")
         .otherwise("Other").alias("encounter_category"),
        F.when(F.col("encounter_class_code") == "EMER", 1)
         .when(F.col("encounter_class_code") == "IMP", 2)
         .when(F.col("encounter_class_code") == "AMB", 3)
         .otherwise(4).alias("acuity_level"),
        F.current_timestamp().alias("dim_created_dts"),
    ).filter(F.col("encounter_class_code").isNotNull())


# Fact builders
def build_fact_encounter(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build encounter fact table."""
    silver_table = get_full_table_name(config, "silver", "v_encounter_flattened")
    df = spark.table(silver_table)
    
    return df.select(
        surrogate_key("encounter_id").alias("encounter_key"),
        surrogate_key("patient_id").alias("patient_key"),
        surrogate_key("provider_id").alias("provider_key"),
        surrogate_key("organization_id").alias("organization_key"),
        date_key("start_datetime").alias("start_date_key"),
        date_key("end_datetime").alias("end_date_key"),
        F.col("encounter_id"),
        F.col("encounter_class_code"),
        F.col("encounter_class"),
        F.col("encounter_type_code"),
        F.col("encounter_type").alias("encounter_description"),
        F.col("reason_code"),
        F.col("reason_description"),
        F.col("start_datetime").alias("start_date"),
        F.col("end_datetime").alias("end_date"),
        F.coalesce(F.col("base_cost"), F.lit(0)).alias("base_encounter_cost"),
        F.coalesce(F.col("total_cost"), F.lit(0)).alias("total_claim_cost"),
        F.coalesce(F.col("payer_coverage"), F.lit(0)).alias("payer_coverage_amount"),
        F.coalesce(F.col("patient_responsibility"), F.lit(0)).alias("patient_responsibility"),
        F.col("duration_minutes"),
        F.col("is_emergency"),
        F.col("is_inpatient"),
        F.col("is_ambulatory"),
        F.col("source_system"),
        F.current_timestamp().alias("fact_created_dts"),
        F.current_timestamp().alias("fact_refresh_dts"),
    ).filter(F.col("encounter_id").isNotNull() & F.col("patient_id").isNotNull())


def build_fact_condition(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build condition fact table."""
    silver_table = get_full_table_name(config, "silver", "v_condition_flattened")
    df = spark.table(silver_table)
    
    return df.select(
        surrogate_key("condition_id").alias("condition_key"),
        surrogate_key("patient_id").alias("patient_key"),
        surrogate_key("encounter_id").alias("encounter_key"),
        date_key("onset_date").alias("onset_date_key"),
        date_key("abatement_date").alias("abatement_date_key"),
        F.col("condition_id"),
        F.col("condition_code"),
        F.col("code_system"),
        F.col("condition_description"),
        F.col("category_code"),
        F.col("clinical_status"),
        F.col("verification_status"),
        F.col("onset_date"),
        F.col("abatement_date"),
        F.col("duration_days"),
        F.col("is_active"),
        F.col("is_resolved"),
        F.when(F.col("duration_days") > 90, True)
         .when(F.col("is_active") & (F.datediff(F.current_date(), F.col("onset_date")) > 90), True)
         .otherwise(False).alias("is_chronic"),
        F.col("source_system"),
        F.current_timestamp().alias("fact_created_dts"),
        F.current_timestamp().alias("fact_refresh_dts"),
    ).filter(F.col("condition_id").isNotNull() & F.col("patient_id").isNotNull())


def build_fact_medication(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build medication fact table."""
    silver_table = get_full_table_name(config, "silver", "v_medication_flattened")
    df = spark.table(silver_table)
    
    return df.select(
        surrogate_key("medication_id").alias("medication_key"),
        surrogate_key("patient_id").alias("patient_key"),
        surrogate_key("encounter_id").alias("encounter_key"),
        date_key("prescribed_date").alias("start_date_key"),
        date_key("end_datetime").alias("end_date_key"),
        F.col("medication_id"),
        F.col("medication_code"),
        F.col("code_system"),
        F.col("medication_description"),
        F.col("rx_status"),
        F.col("rx_intent"),
        F.col("prescribed_date").alias("start_date"),
        F.col("end_datetime").cast("date").alias("end_date"),
        F.col("refills_allowed"),
        F.col("reason_description"),
        F.coalesce(F.col("base_cost"), F.lit(0)).alias("base_cost"),
        F.coalesce(F.col("total_cost"), F.lit(0)).alias("total_cost"),
        F.coalesce(F.col("payer_coverage"), F.lit(0)).alias("payer_coverage"),
        F.coalesce(F.col("patient_cost"), F.lit(0)).alias("patient_cost"),
        F.col("is_active"),
        F.col("source_system"),
        F.current_timestamp().alias("fact_created_dts"),
        F.current_timestamp().alias("fact_refresh_dts"),
    ).filter(F.col("medication_id").isNotNull() & F.col("patient_id").isNotNull())


def build_fact_procedure(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build procedure fact table."""
    silver_table = get_full_table_name(config, "silver", "v_procedure_flattened")
    df = spark.table(silver_table)
    
    return df.select(
        surrogate_key("procedure_id").alias("procedure_key"),
        surrogate_key("patient_id").alias("patient_key"),
        surrogate_key("encounter_id").alias("encounter_key"),
        date_key("performed_date").alias("procedure_date_key"),
        F.col("procedure_id"),
        F.col("procedure_code"),
        F.col("code_system"),
        F.col("procedure_description"),
        F.col("procedure_status"),
        F.col("performed_date"),
        F.coalesce(F.col("procedure_cost"), F.lit(0)).alias("procedure_cost"),
        F.col("reason_code"),
        F.col("reason_description"),
        F.when(F.col("procedure_cost") >= 5000, "High Cost")
         .when(F.col("procedure_cost") >= 1000, "Medium Cost")
         .otherwise("Low Cost").alias("cost_tier"),
        F.col("source_system"),
        F.current_timestamp().alias("fact_created_dts"),
        F.current_timestamp().alias("fact_refresh_dts"),
    ).filter(F.col("procedure_id").isNotNull() & F.col("patient_id").isNotNull())


def build_fact_claim(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build claim fact table from claim and claimresponse views."""
    claim_table = get_full_table_name(config, "silver", "v_claim_flattened")
    response_table = get_full_table_name(config, "silver", "v_claimresponse_flattened")
    
    claims = spark.table(claim_table)
    responses = spark.table(response_table)
    
    return claims.join(
        responses.select(
            F.col("claim_id"),
            F.col("outcome").alias("claim_outcome"),
            F.col("benefit_amount").alias("approved_amount"),
            F.col("payment_amount").alias("paid_amount"),
            F.col("outstanding_amount"),
            F.col("payment_date"),
            F.col("procedure_code").alias("adjudicated_procedure_code"),
            F.col("is_complete"),
            F.col("is_partial")
        ),
        on="claim_id",
        how="left"
    ).select(
        surrogate_key("claim_id").alias("claim_key"),
        surrogate_key("patient_id").alias("patient_key"),
        surrogate_key("provider_id").alias("provider_key"),
        surrogate_key("primary_coverage_id").alias("coverage_key"),
        date_key("service_date").alias("service_date_key"),
        date_key("created_date").alias("created_date_key"),
        F.col("claim_id"),
        F.col("claim_type_code"),
        F.col("claim_type"),
        F.col("claim_status"),
        F.col("claim_use"),
        F.col("primary_diagnosis_code"),
        F.col("secondary_diagnosis_code"),
        F.col("diagnosis_count"),
        F.col("service_date"),
        F.col("created_date").alias("claim_created_date"),
        F.coalesce(F.col("total_amount"), F.lit(0)).alias("submitted_amount"),
        F.col("claim_outcome"),
        F.coalesce(F.col("approved_amount"), F.lit(0)).alias("approved_amount"),
        F.coalesce(F.col("paid_amount"), F.lit(0)).alias("paid_amount"),
        F.coalesce(F.col("outstanding_amount"), F.lit(0)).alias("outstanding_amount"),
        F.col("payment_date"),
        F.col("adjudicated_procedure_code"),
        (F.coalesce(F.col("total_amount"), F.lit(0)) - F.coalesce(F.col("paid_amount"), F.lit(0))).alias("patient_liability"),
        F.when(F.col("total_amount") > 0,
            F.round(F.coalesce(F.col("paid_amount"), F.lit(0)) * 100.0 / F.col("total_amount"), 2)
        ).otherwise(F.lit(0)).alias("payment_rate_pct"),
        F.coalesce(F.col("is_complete"), F.lit(False)).alias("is_fully_paid"),
        F.coalesce(F.col("is_partial"), F.lit(False)).alias("is_partially_paid"),
        (F.coalesce(F.col("outstanding_amount"), F.lit(0)) > 0).alias("has_outstanding_balance"),
        F.col("source_system"),
        F.current_timestamp().alias("fact_created_dts"),
        F.current_timestamp().alias("fact_refresh_dts"),
    ).filter(F.col("claim_id").isNotNull() & F.col("patient_id").isNotNull())


# Aggregate builders
def build_agg_patient_summary(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build pre-aggregated patient summary."""
    dim_patient = get_full_table_name(config, "gold", "dim_patient")
    fact_encounter = get_full_table_name(config, "gold", "fact_encounter")
    fact_condition = get_full_table_name(config, "gold", "fact_condition")
    fact_medication = get_full_table_name(config, "gold", "fact_medication")
    fact_procedure = get_full_table_name(config, "gold", "fact_procedure")
    
    enc_agg = spark.table(fact_encounter).groupBy("patient_key").agg(
        F.count("*").alias("total_encounters"),
        F.sum(F.when(F.col("is_emergency"), 1).otherwise(0)).alias("emergency_visits"),
        F.sum(F.when(F.col("is_inpatient"), 1).otherwise(0)).alias("inpatient_stays"),
        F.sum(F.when(F.col("is_ambulatory"), 1).otherwise(0)).alias("ambulatory_visits"),
        F.sum(F.col("total_claim_cost")).alias("total_healthcare_cost"),
        F.sum(F.col("payer_coverage_amount")).alias("total_payer_coverage"),
        F.sum(F.col("patient_responsibility")).alias("total_patient_cost"),
        F.avg(F.col("total_claim_cost")).alias("avg_encounter_cost"),
    )
    
    cond_agg = spark.table(fact_condition).groupBy("patient_key").agg(
        F.count("*").alias("total_conditions"),
        F.sum(F.when(F.col("is_active"), 1).otherwise(0)).alias("active_conditions"),
        F.sum(F.when(F.col("is_chronic"), 1).otherwise(0)).alias("chronic_conditions"),
    )
    
    med_agg = spark.table(fact_medication).groupBy("patient_key").agg(
        F.count("*").alias("total_prescriptions"),
        F.sum(F.when(F.col("is_active"), 1).otherwise(0)).alias("active_prescriptions"),
        F.sum(F.col("total_cost")).alias("total_medication_cost"),
    )
    
    proc_agg = spark.table(fact_procedure).groupBy("patient_key").agg(
        F.count("*").alias("total_procedures"),
        F.sum(F.col("procedure_cost")).alias("total_procedure_cost"),
    )
    
    return (
        spark.table(dim_patient)
        .join(enc_agg, "patient_key", "left")
        .join(cond_agg, "patient_key", "left")
        .join(med_agg, "patient_key", "left")
        .join(proc_agg, "patient_key", "left")
        .select(
            F.col("patient_key"),
            F.col("patient_id"),
            F.col("full_name").alias("patient_full_name"),
            F.col("age_years"),
            F.col("age_group"),
            F.col("gender"),
            F.col("race"),
            F.col("ethnicity"),
            F.col("state"),
            F.col("is_deceased"),
            F.coalesce(F.col("total_encounters"), F.lit(0)).alias("total_encounters"),
            F.coalesce(F.col("emergency_visits"), F.lit(0)).alias("emergency_visits"),
            F.coalesce(F.col("inpatient_stays"), F.lit(0)).alias("inpatient_stays"),
            F.coalesce(F.col("ambulatory_visits"), F.lit(0)).alias("ambulatory_visits"),
            F.coalesce(F.col("total_healthcare_cost"), F.lit(0)).alias("total_healthcare_cost"),
            F.coalesce(F.col("total_payer_coverage"), F.lit(0)).alias("total_payer_coverage"),
            F.coalesce(F.col("total_patient_cost"), F.lit(0)).alias("total_patient_cost"),
            F.coalesce(F.col("avg_encounter_cost"), F.lit(0)).alias("avg_encounter_cost"),
            F.coalesce(F.col("total_conditions"), F.lit(0)).alias("total_conditions"),
            F.coalesce(F.col("active_conditions"), F.lit(0)).alias("active_conditions"),
            F.coalesce(F.col("chronic_conditions"), F.lit(0)).alias("chronic_conditions"),
            F.coalesce(F.col("total_prescriptions"), F.lit(0)).alias("total_prescriptions"),
            F.coalesce(F.col("active_prescriptions"), F.lit(0)).alias("active_prescriptions"),
            F.coalesce(F.col("total_medication_cost"), F.lit(0)).alias("total_medication_cost"),
            F.coalesce(F.col("total_procedures"), F.lit(0)).alias("total_procedures"),
            F.coalesce(F.col("total_procedure_cost"), F.lit(0)).alias("total_procedure_cost"),
            F.when(F.coalesce(F.col("chronic_conditions"), F.lit(0)) >= 3, "High")
             .when(F.coalesce(F.col("chronic_conditions"), F.lit(0)) >= 1, "Medium")
             .otherwise("Low").alias("chronic_risk_level"),
            F.when(F.coalesce(F.col("emergency_visits"), F.lit(0)) >= 3, "High")
             .when(F.coalesce(F.col("emergency_visits"), F.lit(0)) >= 1, "Medium")
             .otherwise("Low").alias("emergency_utilization_level"),
            F.current_timestamp().alias("agg_refresh_dts"),
        )
    )


def build_agg_encounter_type_summary(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build encounter type summary aggregate."""
    fact_encounter = get_full_table_name(config, "gold", "fact_encounter")
    
    return spark.table(fact_encounter).groupBy(
        "encounter_class",
        "encounter_class_code"
    ).agg(
        F.count("*").alias("encounter_count"),
        F.countDistinct("patient_key").alias("unique_patients"),
        F.sum(F.col("total_claim_cost")).alias("total_cost"),
        F.avg(F.col("total_claim_cost")).alias("avg_cost"),
        F.avg(F.col("duration_minutes")).alias("avg_duration_minutes"),
        F.current_timestamp().alias("agg_refresh_dts"),
    )


def build_agg_condition_prevalence(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build condition prevalence aggregate."""
    fact_condition = get_full_table_name(config, "gold", "fact_condition")
    dim_patient = get_full_table_name(config, "gold", "dim_patient")
    
    total_patients = spark.table(dim_patient).count()
    
    return spark.table(fact_condition).groupBy(
        "condition_code",
        "condition_description"
    ).agg(
        F.count("*").alias("total_occurrences"),
        F.countDistinct("patient_key").alias("unique_patients"),
        (F.countDistinct("patient_key") / F.lit(total_patients) * 100).alias("prevalence_pct"),
        F.sum(F.when(F.col("is_active"), 1).otherwise(0)).alias("active_cases"),
        F.sum(F.when(F.col("is_chronic"), 1).otherwise(0)).alias("chronic_cases"),
        F.avg(F.col("duration_days")).alias("avg_duration_days"),
        F.current_timestamp().alias("agg_refresh_dts"),
    )


def build_agg_provider_performance(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build provider performance aggregate."""
    dim_provider = get_full_table_name(config, "gold", "dim_provider")
    fact_encounter = get_full_table_name(config, "gold", "fact_encounter")
    
    providers = spark.table(dim_provider)
    encounters = spark.table(fact_encounter)
    
    return providers.join(
        encounters.groupBy("provider_key").agg(
            F.count("*").alias("total_encounters"),
            F.countDistinct("patient_key").alias("unique_patients"),
            F.sum(F.when(F.col("is_emergency"), 1).otherwise(0)).alias("emergency_encounters"),
            F.sum(F.when(F.col("is_inpatient"), 1).otherwise(0)).alias("inpatient_encounters"),
            F.sum(F.when(F.col("is_ambulatory"), 1).otherwise(0)).alias("ambulatory_encounters"),
            F.sum(F.col("total_claim_cost")).alias("total_charges"),
            F.sum(F.col("payer_coverage_amount")).alias("total_payer_payments"),
            F.sum(F.col("patient_responsibility")).alias("total_patient_charges"),
            F.avg(F.col("total_claim_cost")).alias("avg_charge_per_encounter"),
            F.avg(F.col("duration_minutes")).alias("avg_encounter_duration_minutes"),
            F.min("start_date").alias("first_encounter_date"),
            F.max("start_date").alias("last_encounter_date"),
        ),
        on="provider_key",
        how="left"
    ).select(
        F.col("provider_key"),
        F.col("provider_id"),
        F.col("provider_name"),
        F.col("specialty"),
        F.col("organization_id"),
        F.col("state"),
        F.coalesce(F.col("total_encounters"), F.lit(0)).alias("total_encounters"),
        F.coalesce(F.col("unique_patients"), F.lit(0)).alias("unique_patients"),
        F.coalesce(F.col("emergency_encounters"), F.lit(0)).alias("emergency_encounters"),
        F.coalesce(F.col("inpatient_encounters"), F.lit(0)).alias("inpatient_encounters"),
        F.coalesce(F.col("ambulatory_encounters"), F.lit(0)).alias("ambulatory_encounters"),
        F.coalesce(F.col("total_charges"), F.lit(0)).alias("total_charges"),
        F.coalesce(F.col("total_payer_payments"), F.lit(0)).alias("total_payer_payments"),
        F.coalesce(F.col("total_patient_charges"), F.lit(0)).alias("total_patient_charges"),
        F.coalesce(F.col("avg_charge_per_encounter"), F.lit(0)).alias("avg_charge_per_encounter"),
        F.coalesce(F.col("avg_encounter_duration_minutes"), F.lit(0)).alias("avg_encounter_duration_minutes"),
        F.round(
            F.coalesce(F.col("total_encounters"), F.lit(0)) * 1.0 / 
            F.greatest(F.coalesce(F.col("unique_patients"), F.lit(1)), F.lit(1)), 2
        ).alias("encounters_per_patient"),
        F.round(
            F.coalesce(F.col("total_charges"), F.lit(0)) / 
            F.greatest(F.coalesce(F.col("unique_patients"), F.lit(1)), F.lit(1)), 2
        ).alias("revenue_per_patient"),
        F.col("first_encounter_date"),
        F.col("last_encounter_date"),
        F.when(F.coalesce(F.col("total_encounters"), F.lit(0)) >= 1000, "High Volume")
         .when(F.coalesce(F.col("total_encounters"), F.lit(0)) >= 100, "Medium Volume")
         .otherwise("Low Volume").alias("volume_tier"),
        F.current_timestamp().alias("agg_refresh_dts"),
    )


def build_agg_medication_utilization(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build medication utilization aggregate."""
    fact_medication = get_full_table_name(config, "gold", "fact_medication")
    
    return spark.table(fact_medication).groupBy(
        "medication_code",
        "medication_description"
    ).agg(
        F.count("*").alias("total_prescriptions"),
        F.countDistinct("patient_key").alias("unique_patients"),
        F.sum(F.when(F.col("is_active"), 1).otherwise(0)).alias("active_prescriptions"),
        F.sum(F.col("total_cost")).alias("total_medication_cost"),
        F.sum(F.col("base_cost")).alias("total_base_cost"),
        F.sum(F.col("payer_coverage")).alias("total_payer_coverage"),
        F.sum(F.col("patient_cost")).alias("total_patient_cost"),
        F.avg(F.col("total_cost")).alias("avg_prescription_cost"),
        F.avg(F.col("patient_cost")).alias("avg_patient_cost"),
        F.when(F.sum(F.col("total_cost")) > 0,
            F.round(F.sum(F.col("payer_coverage")) * 100.0 / F.sum(F.col("total_cost")), 2)
        ).otherwise(F.lit(0)).alias("payer_coverage_rate_pct"),
        F.avg(F.col("refills_allowed")).alias("avg_refills_allowed"),
        F.min("start_date").alias("first_prescription_date"),
        F.max("start_date").alias("last_prescription_date"),
        F.when(F.count("*") >= 1000, "High Utilization")
         .when(F.count("*") >= 100, "Medium Utilization")
         .otherwise("Low Utilization").alias("utilization_tier"),
        F.current_timestamp().alias("agg_refresh_dts"),
    )


def build_agg_payer_statistics(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build payer statistics aggregate."""
    dim_payer = get_full_table_name(config, "gold", "dim_payer")
    fact_claim = get_full_table_name(config, "gold", "fact_claim")
    
    payers = spark.table(dim_payer)
    claims = spark.table(fact_claim)
    
    return payers.join(
        claims.groupBy("coverage_key").agg(
            F.count("*").alias("total_claims"),
            F.countDistinct("patient_key").alias("unique_patients_claims"),
            F.sum(F.col("submitted_amount")).alias("total_submitted"),
            F.sum(F.col("approved_amount")).alias("total_approved"),
            F.sum(F.col("paid_amount")).alias("total_paid"),
            F.sum(F.col("outstanding_amount")).alias("total_outstanding"),
            F.avg(F.col("submitted_amount")).alias("avg_claim_amount"),
            F.avg(F.col("paid_amount")).alias("avg_payment_amount"),
            F.sum(F.when(F.col("is_fully_paid"), 1).otherwise(0)).alias("fully_paid_claims"),
            F.sum(F.when(F.col("is_partially_paid"), 1).otherwise(0)).alias("partially_paid_claims"),
            F.sum(F.when(F.col("has_outstanding_balance"), 1).otherwise(0)).alias("claims_with_balance"),
            F.avg(F.datediff(F.col("payment_date"), F.col("service_date"))).alias("avg_days_to_payment"),
        ).withColumnRenamed("coverage_key", "payer_key"),
        on="payer_key",
        how="left"
    ).select(
        F.col("payer_key"),
        F.col("payer_id"),
        F.col("payer_name"),
        F.col("ownership_type"),
        F.col("payer_tier"),
        F.col("member_count"),
        F.col("coverage_ratio").alias("profile_coverage_ratio"),
        F.col("quality_of_life_score_avg"),
        F.coalesce(F.col("total_claims"), F.lit(0)).alias("total_claims"),
        F.coalesce(F.col("unique_patients_claims"), F.lit(0)).alias("unique_patients"),
        F.coalesce(F.col("total_submitted"), F.lit(0)).alias("total_submitted"),
        F.coalesce(F.col("total_approved"), F.lit(0)).alias("total_approved"),
        F.coalesce(F.col("total_paid"), F.lit(0)).alias("total_paid"),
        F.coalesce(F.col("total_outstanding"), F.lit(0)).alias("total_outstanding"),
        F.when(F.coalesce(F.col("total_submitted"), F.lit(0)) > 0,
            F.round(F.coalesce(F.col("total_paid"), F.lit(0)) * 100.0 / F.col("total_submitted"), 2)
        ).otherwise(F.lit(0)).alias("payment_rate_pct"),
        F.when(F.coalesce(F.col("total_submitted"), F.lit(0)) > 0,
            F.round(F.coalesce(F.col("total_approved"), F.lit(0)) * 100.0 / F.col("total_submitted"), 2)
        ).otherwise(F.lit(0)).alias("approval_rate_pct"),
        F.coalesce(F.col("avg_claim_amount"), F.lit(0)).alias("avg_claim_amount"),
        F.coalesce(F.col("avg_payment_amount"), F.lit(0)).alias("avg_payment_amount"),
        F.coalesce(F.col("fully_paid_claims"), F.lit(0)).alias("fully_paid_claims"),
        F.coalesce(F.col("partially_paid_claims"), F.lit(0)).alias("partially_paid_claims"),
        F.coalesce(F.col("claims_with_balance"), F.lit(0)).alias("claims_with_balance"),
        F.col("avg_days_to_payment"),
        F.current_timestamp().alias("agg_refresh_dts"),
    )


def build_agg_financial_summary(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build financial summary aggregate by time period."""
    fact_encounter = get_full_table_name(config, "gold", "fact_encounter")
    dim_date = get_full_table_name(config, "gold", "dim_date")
    
    encounters = spark.table(fact_encounter)
    dates = spark.table(dim_date)
    
    return encounters.join(
        dates.select(
            F.col("date_key"),
            F.col("year"),
            F.col("quarter"),
            F.col("month"),
            F.concat(F.col("year"), F.lit("-Q"), F.col("quarter")).alias("year_quarter"),
            F.date_format(F.col("date"), "yyyy-MM").alias("year_month"),
        ),
        encounters.start_date_key == dates.date_key,
        "inner"
    ).groupBy(
        "year", "quarter", "month", "year_quarter", "year_month"
    ).agg(
        F.count("*").alias("total_encounters"),
        F.countDistinct("patient_key").alias("unique_patients"),
        F.countDistinct("provider_key").alias("unique_providers"),
        F.countDistinct("organization_key").alias("unique_organizations"),
        F.sum(F.when(F.col("is_emergency"), 1).otherwise(0)).alias("emergency_encounters"),
        F.sum(F.when(F.col("is_inpatient"), 1).otherwise(0)).alias("inpatient_encounters"),
        F.sum(F.when(F.col("is_ambulatory"), 1).otherwise(0)).alias("ambulatory_encounters"),
        F.sum(F.col("base_encounter_cost")).alias("total_base_cost"),
        F.sum(F.col("total_claim_cost")).alias("total_claim_cost"),
        F.sum(F.col("payer_coverage_amount")).alias("total_payer_coverage"),
        F.sum(F.col("patient_responsibility")).alias("total_patient_responsibility"),
        F.avg(F.col("total_claim_cost")).alias("avg_claim_cost"),
        F.avg(F.col("payer_coverage_amount")).alias("avg_payer_coverage"),
        F.avg(F.col("patient_responsibility")).alias("avg_patient_responsibility"),
        F.sum(F.col("duration_minutes")).alias("total_duration_minutes"),
        F.avg(F.col("duration_minutes")).alias("avg_duration_minutes"),
        F.when(F.sum(F.col("total_claim_cost")) > 0,
            F.round(F.sum(F.col("payer_coverage_amount")) * 100.0 / F.sum(F.col("total_claim_cost")), 2)
        ).otherwise(F.lit(0)).alias("payer_coverage_rate_pct"),
        F.when(F.count("*") > 0,
            F.round(F.sum(F.when(F.col("is_emergency"), 1).otherwise(0)) * 100.0 / F.count("*"), 2)
        ).otherwise(F.lit(0)).alias("emergency_rate_pct"),
        F.round(F.count("*") * 1.0 / F.greatest(F.countDistinct("patient_key"), F.lit(1)), 2).alias("avg_encounters_per_patient"),
        F.current_timestamp().alias("agg_refresh_dts"),
    ).orderBy("year", "quarter", "month")


def build_agg_readmission_rates(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build readmission rates aggregate."""
    fact_encounter = get_full_table_name(config, "gold", "fact_encounter")
    
    # Get inpatient and emergency encounters
    admissions = spark.table(fact_encounter).filter(
        (F.col("is_inpatient") == True) | (F.col("is_emergency") == True)
    ).select(
        F.col("encounter_key"),
        F.col("patient_key"),
        F.col("encounter_class_code"),
        F.col("start_date"),
        F.col("end_date"),
    )
    
    # Add lag to get previous encounter
    patient_window = Window.partitionBy("patient_key").orderBy("start_date")
    
    numbered = admissions.withColumn("prev_date", F.lag("start_date").over(patient_window))
    numbered = numbered.withColumn("days_since_prev", F.datediff(F.col("start_date"), F.col("prev_date")))
    numbered = numbered.withColumn("is_30_day_readmit", 
        F.when(F.col("days_since_prev") <= 30, 1).otherwise(0))
    numbered = numbered.withColumn("is_60_day_readmit", 
        F.when(F.col("days_since_prev") <= 60, 1).otherwise(0))
    numbered = numbered.withColumn("is_90_day_readmit", 
        F.when(F.col("days_since_prev") <= 90, 1).otherwise(0))
    
    return numbered.filter(F.col("prev_date").isNotNull()).groupBy(
        F.year(F.col("start_date")).alias("year"),
        F.quarter(F.col("start_date")).alias("quarter"),
        F.month(F.col("start_date")).alias("month")
    ).agg(
        F.count("*").alias("total_admissions"),
        F.countDistinct("patient_key").alias("unique_patients"),
        F.sum(F.col("is_30_day_readmit")).alias("readmissions_30_day"),
        F.sum(F.col("is_60_day_readmit")).alias("readmissions_60_day"),
        F.sum(F.col("is_90_day_readmit")).alias("readmissions_90_day"),
        F.when(F.count("*") > 0,
            F.round(F.sum(F.col("is_30_day_readmit")) * 100.0 / F.count("*"), 2)
        ).otherwise(F.lit(0)).alias("readmission_rate_30_day_pct"),
        F.when(F.count("*") > 0,
            F.round(F.sum(F.col("is_60_day_readmit")) * 100.0 / F.count("*"), 2)
        ).otherwise(F.lit(0)).alias("readmission_rate_60_day_pct"),
        F.when(F.count("*") > 0,
            F.round(F.sum(F.col("is_90_day_readmit")) * 100.0 / F.count("*"), 2)
        ).otherwise(F.lit(0)).alias("readmission_rate_90_day_pct"),
        F.avg(F.when(F.col("is_30_day_readmit") == 1, F.col("days_since_prev"))).alias("avg_days_to_30_day_readmit"),
        F.avg(F.when(F.col("is_90_day_readmit") == 1, F.col("days_since_prev"))).alias("avg_days_to_90_day_readmit"),
        F.current_timestamp().alias("agg_refresh_dts"),
    ).orderBy("year", "quarter", "month")


# Gold table registry
@dataclass(frozen=True)
class GoldTableDef:
    """Definition of a gold table."""
    name: str
    category: str
    build_fn: Callable[[SparkSession, PipelineConfig], DataFrame]
    dependencies: Tuple[str, ...] = ()


GOLD_TABLE_REGISTRY: Dict[str, GoldTableDef] = {
    # Dimensions
    "dim_date": GoldTableDef("dim_date", "dimension", build_dim_date),
    "dim_patient": GoldTableDef("dim_patient", "dimension", build_dim_patient),
    "dim_provider": GoldTableDef("dim_provider", "dimension", build_dim_provider),
    "dim_organization": GoldTableDef("dim_organization", "dimension", build_dim_organization),
    "dim_payer": GoldTableDef("dim_payer", "dimension", build_dim_payer),
    "dim_encounter_type": GoldTableDef("dim_encounter_type", "dimension", build_dim_encounter_type),
    # Facts
    "fact_encounter": GoldTableDef("fact_encounter", "fact", build_fact_encounter, ("dim_patient", "dim_provider", "dim_organization")),
    "fact_condition": GoldTableDef("fact_condition", "fact", build_fact_condition, ("dim_patient",)),
    "fact_medication": GoldTableDef("fact_medication", "fact", build_fact_medication, ("dim_patient",)),
    "fact_procedure": GoldTableDef("fact_procedure", "fact", build_fact_procedure, ("dim_patient",)),
    "fact_claim": GoldTableDef("fact_claim", "fact", build_fact_claim, ("dim_patient", "dim_payer")),
    # Aggregates
    "agg_patient_summary": GoldTableDef("agg_patient_summary", "aggregate", build_agg_patient_summary, ("dim_patient", "fact_encounter", "fact_condition", "fact_medication", "fact_procedure")),
    "agg_encounter_type_summary": GoldTableDef("agg_encounter_type_summary", "aggregate", build_agg_encounter_type_summary, ("fact_encounter",)),
    "agg_condition_prevalence": GoldTableDef("agg_condition_prevalence", "aggregate", build_agg_condition_prevalence, ("dim_patient", "fact_condition")),
    "agg_provider_performance": GoldTableDef("agg_provider_performance", "aggregate", build_agg_provider_performance, ("dim_provider", "fact_encounter")),
    "agg_medication_utilization": GoldTableDef("agg_medication_utilization", "aggregate", build_agg_medication_utilization, ("fact_medication",)),
    "agg_payer_statistics": GoldTableDef("agg_payer_statistics", "aggregate", build_agg_payer_statistics, ("dim_payer", "fact_claim")),
    "agg_financial_summary": GoldTableDef("agg_financial_summary", "aggregate", build_agg_financial_summary, ("dim_date", "fact_encounter")),
    "agg_readmission_rates": GoldTableDef("agg_readmission_rates", "aggregate", build_agg_readmission_rates, ("fact_encounter",)),
}


@dataclass
class GoldResult:
    """Result of a gold table build."""
    table_name: str
    category: str
    row_count: int
    elapsed_seconds: float
    success: bool
    error: Optional[str] = None


def write_gold_table(
    spark: SparkSession,
    config: PipelineConfig,
    table_def: GoldTableDef
) -> GoldResult:
    """Write a gold table."""
    start_time = time.time()
    try:
        df = table_def.build_fn(spark, config)
        full_table = get_full_table_name(config, "gold", table_def.name)
        df.write.format("delta").mode("overwrite").saveAsTable(full_table)
        row_count = spark.table(full_table).count()
        elapsed = time.time() - start_time
        print(f"  Created {full_table} with {row_count:,} rows in {elapsed:.2f}s")
        return GoldResult(table_def.name, table_def.category, row_count, elapsed, True)
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"  ERROR: {table_def.name} - {str(e)}")
        return GoldResult(table_def.name, table_def.category, 0, elapsed, False, str(e))


def run_gold_builds_in_order(
    spark: SparkSession,
    config: PipelineConfig,
    categories: Optional[List[str]] = None
) -> List[GoldResult]:
    """Run gold builds respecting dependencies."""
    if categories is None:
        categories = ["dimension", "fact", "aggregate"]
    
    results = []
    built_tables = set()
    
    for category in categories:
        tables_in_category = [(name, defn) for name, defn in GOLD_TABLE_REGISTRY.items() if defn.category == category]
        print(f"\nBuilding {category} tables ({len(tables_in_category)} tables)...")
        
        for name, table_def in tables_in_category:
            missing_deps = [d for d in table_def.dependencies if d not in built_tables]
            if missing_deps:
                print(f"  Skipping {name}: missing dependencies {missing_deps}")
                continue
            
            result = write_gold_table(spark, config, table_def)
            results.append(result)
            if result.success:
                built_tables.add(name)
    
    successful = sum(1 for r in results if r.success)
    total_rows = sum(r.row_count for r in results)
    print(f"\nCompleted: {successful}/{len(results)} tables, {total_rows:,} total rows")
    return results
