"""
Gold Layer: CMS-Compatible Dimensions and Facts

Creates CMS SynPUF-compatible tables from Silver FHIR-aligned data.
All CMS fields are included; NULLs are used for data gaps.

References:
- CMS SynPUF Data User Guide
- sql/sql_batch/gold/cms_approximation/README.md
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Callable, Tuple, Union
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType
from pyspark.sql.window import Window

from .config import PipelineConfig, get_full_table_name


def surrogate_key(*cols) -> F.Column:
    """Generate SHA-256 surrogate key from columns. Accepts string column names or Column objects."""
    if len(cols) == 1:
        col_ref = cols[0] if isinstance(cols[0], F.Column) else F.col(cols[0])
        return F.sha2(F.coalesce(col_ref, F.lit("")).cast("string"), 256)
    
    col_refs = [c if isinstance(c, F.Column) else F.col(c) for c in cols]
    return F.sha2(F.concat_ws("|", *[F.coalesce(c, F.lit("")) for c in col_refs]), 256)


def date_key(col_ref) -> F.Column:
    """Generate integer date key (YYYYMMDD). Accepts string column name or Column object."""
    col_obj = col_ref if isinstance(col_ref, F.Column) else F.col(col_ref)
    return F.date_format(col_obj, "yyyyMMdd").cast(IntegerType())


# =============================================================================
# CMS CHRONIC CONDITION DETECTION
# =============================================================================

def detect_chronic_conditions(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """
    Detect CMS chronic conditions from silver conditions.
    
    Uses condition codes to identify 11 CMS chronic condition flags:
    - SP_ALZHDMTA: Alzheimer's Disease/Dementia
    - SP_CHF: Congestive Heart Failure
    - SP_CHRNKIDN: Chronic Kidney Disease
    - SP_CNCR: Cancer
    - SP_COPD: Chronic Obstructive Pulmonary Disease
    - SP_DEPRESSN: Depression
    - SP_DIABETES: Diabetes
    - SP_ISCHMCHT: Ischemic Heart Disease
    - SP_OSTEOPRS: Osteoporosis
    - SP_RA_OA: Rheumatoid Arthritis / Osteoarthritis
    - SP_STRKETIA: Stroke/TIA
    
    Returns patient-level chronic condition flags (1=Yes, 2=No per CMS convention).
    """
    condition_table = get_full_table_name(config, "silver", "v_condition_flattened")
    conditions = spark.table(condition_table)
    
    # Define CMS chronic condition mappings (SNOMED and ICD-10-CM codes)
    # These are examples - expand with full eCQM value set codes in production
    chronic_condition_codes = {
        # Alzheimer's/Dementia - SNOMED codes
        'alzheimers': ['26929004', '230270009', '12348006', '421529006'],
        # CHF - SNOMED codes  
        'chf': ['42343007', '407596008', '194767001', '194779001'],
        # CKD - SNOMED codes
        'ckd': ['709044004', '46177005', '236425005', '236426006'],
        # Cancer - SNOMED codes
        'cancer': ['363346000', '93870000', '109838007', '254837009'],
        # COPD - SNOMED codes
        'copd': ['13645005', '185086009', '68328006'],
        # Depression - SNOMED codes
        'depression': ['35489007', '370143000', '33078009', '192080009'],
        # Diabetes - SNOMED codes
        'diabetes': ['44054006', '73211009', '46635009', '11530004'],
        # IHD - SNOMED codes
        'ihd': ['414545008', '413439005', '194828000', '233838004'],
        # Osteoporosis - SNOMED codes
        'osteoporosis': ['64859006', '203438003', '240165008'],
        # RA/OA - SNOMED codes
        'ra_oa': ['69896004', '239873007', '396275006', '201834006'],
        # Stroke/TIA - SNOMED codes
        'stroke': ['230690007', '422504002', '195189005', '230706003']
    }
    
    # Create patient-level chronic condition flags
    patient_conditions = conditions.groupBy("patient_id").agg(
        # Alzheimer's/Dementia
        F.max(F.when(F.col("condition_code").isin(chronic_condition_codes['alzheimers']), 1).otherwise(0)).alias("has_alzheimers"),
        # CHF
        F.max(F.when(F.col("condition_code").isin(chronic_condition_codes['chf']), 1).otherwise(0)).alias("has_chf"),
        # CKD
        F.max(F.when(F.col("condition_code").isin(chronic_condition_codes['ckd']), 1).otherwise(0)).alias("has_ckd"),
        # Cancer
        F.max(F.when(F.col("condition_code").isin(chronic_condition_codes['cancer']), 1).otherwise(0)).alias("has_cancer"),
        # COPD
        F.max(F.when(F.col("condition_code").isin(chronic_condition_codes['copd']), 1).otherwise(0)).alias("has_copd"),
        # Depression
        F.max(F.when(F.col("condition_code").isin(chronic_condition_codes['depression']), 1).otherwise(0)).alias("has_depression"),
        # Diabetes
        F.max(F.when(F.col("condition_code").isin(chronic_condition_codes['diabetes']), 1).otherwise(0)).alias("has_diabetes"),
        # IHD
        F.max(F.when(F.col("condition_code").isin(chronic_condition_codes['ihd']), 1).otherwise(0)).alias("has_ihd"),
        # Osteoporosis
        F.max(F.when(F.col("condition_code").isin(chronic_condition_codes['osteoporosis']), 1).otherwise(0)).alias("has_osteoporosis"),
        # RA/OA
        F.max(F.when(F.col("condition_code").isin(chronic_condition_codes['ra_oa']), 1).otherwise(0)).alias("has_ra_oa"),
        # Stroke/TIA
        F.max(F.when(F.col("condition_code").isin(chronic_condition_codes['stroke']), 1).otherwise(0)).alias("has_stroke"),
        # ESRD (detected from ckd + dialysis or transplant codes)
        F.max(F.when(F.col("condition_code").isin(['90688005', '236435004', '236436003']), 1).otherwise(0)).alias("has_esrd")
    )
    
    return patient_conditions


# =============================================================================
# CMS DIMENSIONS
# =============================================================================

def build_dim_cms_beneficiary(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """
    Build CMS Beneficiary Dimension with demographics and chronic conditions.
    
    Includes all CMS SynPUF beneficiary fields with NULLs for missing data.
    """
    patient_table = get_full_table_name(config, "silver", "v_patient_flattened")
    patients = spark.table(patient_table)
    
    # Get chronic conditions
    chronic_conditions = detect_chronic_conditions(spark, config)
    
    return patients.join(
        chronic_conditions,
        "patient_id",
        "left"
    ).select(
        # Surrogate keys
        surrogate_key("patient_id").alias("beneficiary_key"),
        F.col("patient_id").alias("desynpuf_id"),
        
        # Demographics
        F.col("birth_date").alias("bene_birth_dt"),
        F.col("death_date").alias("bene_death_dt"),
        F.when(F.lower(F.col("gender")) == "male", 1)
         .when(F.lower(F.col("gender")) == "female", 2)
         .otherwise(0).alias("bene_sex_ident_cd"),
        F.when(F.lower(F.col("race")) == "white", 1)
         .when(F.lower(F.col("race")).isin(["black", "african american"]), 2)
         .when(F.lower(F.col("race")) == "asian", 4)
         .when(F.lower(F.col("ethnicity")).isin(["hispanic", "latino"]), 5)
         .otherwise(3).alias("bene_race_cd"),
        
        # ESRD Indicator
        F.when(F.coalesce(F.col("has_esrd"), F.lit(0)) == 1, "Y").otherwise("0").alias("bene_esrd_ind"),
        
        # Geographic
        F.col("state").alias("sp_state_code"),
        F.col("county").alias("bene_county_cd"),  # From Synthea patients.csv
        F.lit(None).cast("string").alias("bene_zip_cd"),  # Not in Synthea
        
        # Coverage Months (set to 12 - gaps: actual coverage tracking)
        F.lit(12).cast(IntegerType()).alias("bene_hi_cvrage_tot_mons"),   # Part A
        F.lit(12).cast(IntegerType()).alias("bene_smi_cvrage_tot_mons"),  # Part B
        F.lit(0).cast(IntegerType()).alias("bene_hmo_cvrage_tot_mons"),   # NULL - no HMO data
        F.lit(0).cast(IntegerType()).alias("plan_cvrg_mos_num"),          # Part D - NULL
        
        # Chronic Condition Flags (1=Yes, 2=No per CMS convention)
        F.when(F.coalesce(F.col("has_alzheimers"), F.lit(0)) == 1, 1).otherwise(2).alias("sp_alzhdmta"),
        F.when(F.coalesce(F.col("has_chf"), F.lit(0)) == 1, 1).otherwise(2).alias("sp_chf"),
        F.when(F.coalesce(F.col("has_ckd"), F.lit(0)) == 1, 1).otherwise(2).alias("sp_chrnkidn"),
        F.when(F.coalesce(F.col("has_cancer"), F.lit(0)) == 1, 1).otherwise(2).alias("sp_cncr"),
        F.when(F.coalesce(F.col("has_copd"), F.lit(0)) == 1, 1).otherwise(2).alias("sp_copd"),
        F.when(F.coalesce(F.col("has_depression"), F.lit(0)) == 1, 1).otherwise(2).alias("sp_depressn"),
        F.when(F.coalesce(F.col("has_diabetes"), F.lit(0)) == 1, 1).otherwise(2).alias("sp_diabetes"),
        F.when(F.coalesce(F.col("has_ihd"), F.lit(0)) == 1, 1).otherwise(2).alias("sp_ischmcht"),
        F.when(F.coalesce(F.col("has_osteoporosis"), F.lit(0)) == 1, 1).otherwise(2).alias("sp_osteoprs"),
        F.when(F.coalesce(F.col("has_ra_oa"), F.lit(0)) == 1, 1).otherwise(2).alias("sp_ra_oa"),
        F.when(F.coalesce(F.col("has_stroke"), F.lit(0)) == 1, 1).otherwise(2).alias("sp_strketia"),
        
        # Annual Costs (will be joined from summary table)
        F.lit(None).cast(DecimalType(10, 2)).alias("medreimb_ip"),
        F.lit(None).cast(DecimalType(10, 2)).alias("benres_ip"),
        F.lit(None).cast(DecimalType(10, 2)).alias("pppymt_ip"),
        F.lit(None).cast(DecimalType(10, 2)).alias("medreimb_op"),
        F.lit(None).cast(DecimalType(10, 2)).alias("benres_op"),
        F.lit(None).cast(DecimalType(10, 2)).alias("pppymt_op"),
        F.lit(None).cast(DecimalType(10, 2)).alias("medreimb_car"),
        F.lit(None).cast(DecimalType(10, 2)).alias("benres_car"),
        F.lit(None).cast(DecimalType(10, 2)).alias("pppymt_car"),
        
        # Metadata
        F.col("source_system"),
        F.current_timestamp().alias("dim_created_dts"),
        F.current_timestamp().alias("dim_refresh_dts"),
    )


def build_dim_cms_provider(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """Build CMS Provider Dimension with NPI and specialty."""
    provider_table = get_full_table_name(config, "silver", "v_practitioner_flattened")
    providers = spark.table(provider_table)
    
    return providers.select(
        surrogate_key("practitioner_id").alias("provider_key"),
        F.col("practitioner_id").alias("provider_id"),
        F.col("practitioner_id").alias("prvdr_num"),  # CMS provider number (using ID)
        F.lit(None).cast("string").alias("npi"),  # NULL - NPI not in Synthea
        F.col("name").alias("provider_name"),
        F.col("specialty").alias("provider_specialty"),
        F.col("specialty_code"),
        F.col("gender"),
        F.col("state"),
        F.col("city"),
        F.col("postal_code").alias("zip_code"),
        F.col("organization_id"),
        surrogate_key("organization_id").alias("organization_key"),
        F.col("active"),
        F.col("source_system"),
        F.current_timestamp().alias("dim_created_dts"),
    )


# =============================================================================
# CMS FACT TABLES
# =============================================================================

def build_fact_cms_inpatient_claims(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """
    Build CMS Inpatient Claims fact table.
    
    Includes:
    - Claim header fields (CLM_ID, FROM_DT, THRU_DT, etc.)
    - Diagnosis codes (ICD_DGNS_CD1-10) with ICD-10-CM enrichment
    - Procedure codes (ICD_PRCDR_CD1-6) with ICD-10-PCS enrichment
    - Financial fields (CLM_PMT_AMT, NCH_PRMRY_PYR_CLM_PD_AMT, etc.)
    - DRG code (NULL - requires grouper logic)
    """
    encounter_table = get_full_table_name(config, "silver", "v_encounter_flattened")
    condition_table = get_full_table_name(config, "silver", "v_condition_flattened")
    procedure_table = get_full_table_name(config, "silver", "v_procedure_flattened")
    
    # Get inpatient encounters
    inpatient = spark.table(encounter_table).filter(
        F.lower(F.col("encounter_class")) == "inpatient"
    )
    
    # Get diagnosis codes for each encounter (pivot to columns)
    diagnoses = spark.table(condition_table).filter(
        F.col("encounter_id").isNotNull()
    )
    
    dx_window = Window.partitionBy("encounter_id").orderBy("onset_date")
    diagnoses_ranked = diagnoses.withColumn("dx_rank", F.row_number().over(dx_window))
    
    # Pivot diagnoses (up to 10)
    dx_pivot = diagnoses_ranked.groupBy("encounter_id").agg(
        *[F.max(F.when(F.col("dx_rank") == i, F.coalesce(F.col("icd10cm_code"), F.col("condition_code")))).alias(f"icd_dgns_cd{i}")
          for i in range(1, 11)]
    )
    
    # Get procedure codes for each encounter (pivot to columns)
    procedures = spark.table(procedure_table).filter(
        F.col("encounter_id").isNotNull()
    )
    
    proc_window = Window.partitionBy("encounter_id").orderBy("performed_date")
    procedures_ranked = procedures.withColumn("proc_rank", F.row_number().over(proc_window))
    
    # Pivot procedures (up to 6)
    proc_pivot = procedures_ranked.groupBy("encounter_id").agg(
        *[F.max(F.when(F.col("proc_rank") == i, F.col("hcpcs_code"))).alias(f"icd_prcdr_cd{i}")
          for i in range(1, 7)],
        *[F.max(F.when(F.col("proc_rank") == i, F.col("performed_date"))).alias(f"prcdr_dt{i}")
          for i in range(1, 7)]
    )
    
    return inpatient.join(dx_pivot, "encounter_id", "left").join(proc_pivot, "encounter_id", "left").select(
        # Claim Identification
        surrogate_key("encounter_id").alias("claim_key"),
        F.col("encounter_id").alias("clm_id"),
        surrogate_key("patient_id").alias("beneficiary_key"),
        F.col("patient_id").alias("desynpuf_id"),
        
        # Claim Dates
        F.col("start_datetime").cast("date").alias("clm_from_dt"),
        F.col("end_datetime").cast("date").alias("clm_thru_dt"),
        F.col("start_datetime").cast("date").alias("nch_bene_dschrg_dt"),
        
        # Provider
        surrogate_key("provider_id").alias("provider_key"),
        F.col("provider_id").alias("at_physn_npi"),  # Attending physician
        F.col("provider_id").alias("op_physn_npi"),  # Operating physician (NULL - no data)
        F.lit(None).cast("string").alias("ot_physn_npi"),  # Other physician - NULL
        
        # Organization/Facility
        surrogate_key("organization_id").alias("organization_key"),
        F.col("organization_id").alias("provider_id_cms"),
        
        # Admission/Discharge
        F.lit(None).cast("int").alias("clm_admsn_dt"),  # NULL - use clm_from_dt
        F.lit(1).cast("int").alias("admtng_icd_dgns_cd"),  # Admission diagnosis (use primary)
        F.lit(None).cast("int").alias("clm_pass_thru_per_diem_amt"),  # NULL
        
        # Financial
        F.coalesce(F.col("total_cost"), F.lit(0)).cast(DecimalType(10, 2)).alias("clm_pmt_amt"),
        F.coalesce(F.col("payer_coverage"), F.lit(0)).cast(DecimalType(10, 2)).alias("nch_prmry_pyr_clm_pd_amt"),
        F.coalesce(F.col("base_cost"), F.lit(0)).cast(DecimalType(10, 2)).alias("nch_bene_ip_ddctbl_amt"),
        F.coalesce(F.col("patient_responsibility"), F.lit(0)).cast(DecimalType(10, 2)).alias("nch_bene_pta_coinsrnc_lblty_amt"),
        F.coalesce(F.col("patient_responsibility"), F.lit(0)).cast(DecimalType(10, 2)).alias("nch_bene_blood_ddctbl_lblty_am"),
        F.lit(None).cast(DecimalType(10, 2)).alias("nch_ip_ncvrd_chrg_amt"),  # NULL
        F.lit(None).cast(DecimalType(10, 2)).alias("nch_ip_tot_ddctn_amt"),   # NULL
        
        # Utilization
        F.coalesce(
            F.datediff(F.col("end_datetime"), F.col("start_datetime")),
            F.lit(0)
        ).cast("int").alias("clm_utlztn_day_cnt"),
        
        # DRG - NULL (requires MS-DRG grouper logic)
        F.lit(None).cast("string").alias("clm_drg_cd"),
        
        # Diagnosis Codes (ICD-10-CM) - up to 10
        *[F.col(f"icd_dgns_cd{i}") for i in range(1, 11)],
        
        # Procedure Codes (ICD-10-PCS or HCPCS) - up to 6
        *[F.col(f"icd_prcdr_cd{i}") for i in range(1, 7)],
        *[F.col(f"prcdr_dt{i}") for i in range(1, 7)],
        
        # Metadata
        date_key("start_datetime").alias("service_date_key"),
        F.col("source_system"),
        F.current_timestamp().alias("fact_created_dts"),
    ).filter((F.col("clm_id").isNotNull()) & (F.col("desynpuf_id").isNotNull()))


def build_fact_cms_outpatient_claims(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """
    Build CMS Outpatient Claims fact table.
    
    Similar to inpatient but for outpatient/ambulatory encounters.
    """
    encounter_table = get_full_table_name(config, "silver", "v_encounter_flattened")
    condition_table = get_full_table_name(config, "silver", "v_condition_flattened")
    procedure_table = get_full_table_name(config, "silver", "v_procedure_flattened")
    
    # Get outpatient encounters (ambulatory, emergency, outpatient, urgentcare)
    outpatient = spark.table(encounter_table).filter(
        F.lower(F.col("encounter_class")).isin(["ambulatory", "outpatient", "emergency", "urgentcare"])
    )
    
    # Get diagnosis codes
    diagnoses = spark.table(condition_table).filter(F.col("encounter_id").isNotNull())
    dx_window = Window.partitionBy("encounter_id").orderBy("onset_date")
    diagnoses_ranked = diagnoses.withColumn("dx_rank", F.row_number().over(dx_window))
    dx_pivot = diagnoses_ranked.groupBy("encounter_id").agg(
        *[F.max(F.when(F.col("dx_rank") == i, F.coalesce(F.col("icd10cm_code"), F.col("condition_code")))).alias(f"icd_dgns_cd{i}")
          for i in range(1, 11)]
    )
    
    # Get procedure codes
    procedures = spark.table(procedure_table).filter(F.col("encounter_id").isNotNull())
    proc_window = Window.partitionBy("encounter_id").orderBy("performed_date")
    procedures_ranked = procedures.withColumn("proc_rank", F.row_number().over(proc_window))
    proc_pivot = procedures_ranked.groupBy("encounter_id").agg(
        *[F.max(F.when(F.col("proc_rank") == i, F.col("hcpcs_code"))).alias(f"icd_prcdr_cd{i}")
          for i in range(1, 7)],
        *[F.max(F.when(F.col("proc_rank") == i, F.col("performed_date"))).alias(f"prcdr_dt{i}")
          for i in range(1, 7)]
    )
    
    return outpatient.join(dx_pivot, "encounter_id", "left").join(proc_pivot, "encounter_id", "left").select(
        # Claim Identification
        surrogate_key("encounter_id").alias("claim_key"),
        F.col("encounter_id").alias("clm_id"),
        surrogate_key("patient_id").alias("beneficiary_key"),
        F.col("patient_id").alias("desynpuf_id"),
        
        # Claim Dates
        F.col("start_datetime").cast("date").alias("clm_from_dt"),
        F.col("end_datetime").cast("date").alias("clm_thru_dt"),
        
        # Provider/Facility
        surrogate_key("provider_id").alias("provider_key"),
        surrogate_key("organization_id").alias("organization_key"),
        F.col("provider_id").alias("at_physn_npi"),
        F.col("provider_id").alias("op_physn_npi"),
        F.col("organization_id").alias("provider_id_cms"),
        
        # Financial
        F.coalesce(F.col("total_cost"), F.lit(0)).cast(DecimalType(10, 2)).alias("clm_pmt_amt"),
        F.coalesce(F.col("payer_coverage"), F.lit(0)).cast(DecimalType(10, 2)).alias("nch_prmry_pyr_clm_pd_amt"),
        F.coalesce(F.col("patient_responsibility"), F.lit(0)).cast(DecimalType(10, 2)).alias("nch_bene_blood_ddctbl_lblty_am"),
        F.lit(None).cast(DecimalType(10, 2)).alias("nch_profnl_cmpnt_chrg_amt"),  # NULL
        
        # Utilization
        F.lit(1).cast("int").alias("clm_line_cnt"),  # Number of line items (NULL - not tracked)
        
        # Emergency Flag
        (F.lower(F.col("encounter_class")) == "emergency").alias("is_emergency"),
        
        # Diagnosis Codes (ICD-10-CM) - up to 10
        *[F.col(f"icd_dgns_cd{i}") for i in range(1, 11)],
        
        # Procedure Codes (HCPCS) - up to 6
        *[F.col(f"icd_prcdr_cd{i}") for i in range(1, 7)],
        *[F.col(f"prcdr_dt{i}") for i in range(1, 7)],
        
        # Metadata
        date_key("start_datetime").alias("service_date_key"),
        F.col("source_system"),
        F.current_timestamp().alias("fact_created_dts"),
    ).filter((F.col("clm_id").isNotNull()) & (F.col("desynpuf_id").isNotNull()))


def build_fact_cms_prescription_drug_events(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """
    Build CMS Prescription Drug Events (Part D PDE) fact table.
    
    Includes:
    - PDE_ID: Prescription event ID
    - PROD_SRVC_ID: NDC code (from lookup enrichment)
    - QTY_DSPNSD_NUM: Quantity dispensed
    - DAYS_SUPLY_NUM: Days supply
    - Cost fields
    """
    medication_table = get_full_table_name(config, "silver", "v_medication_flattened")
    medications = spark.table(medication_table)
    
    return medications.select(
        # PDE Identification
        surrogate_key("medication_id").alias("pde_key"),
        F.col("medication_id").alias("pde_id"),
        surrogate_key("patient_id").alias("beneficiary_key"),
        F.col("patient_id").alias("desynpuf_id"),
        
        # Service Date
        F.col("prescribed_date").alias("srvc_dt"),
        date_key("prescribed_date").alias("service_date_key"),
        
        # Product Service ID (NDC code from lookup enrichment)
        F.coalesce(F.col("ndc_code"), F.col("medication_code")).alias("prod_srvc_id"),
        
        # Drug Information
        F.col("medication_code").alias("rxnorm_code"),
        F.col("medication_description"),
        F.col("rxnorm_name"),
        F.col("ndc_proprietary_name").alias("brand_name"),
        F.col("ndc_nonproprietary_name").alias("generic_name"),
        F.col("dosage_form"),
        F.col("route"),
        F.col("manufacturer"),
        
        # Quantity Dispensed (estimated from dispenses)
        F.coalesce(F.col("refills_allowed"), F.lit(1)).cast(DecimalType(10, 2)).alias("qty_dspnsd_num"),
        
        # Days Supply (calculated from prescription duration)
        F.when(
            F.col("end_datetime").isNotNull(),
            F.greatest(F.lit(1), F.datediff(F.col("end_datetime"), F.col("start_datetime")))
        ).otherwise(F.lit(30)).cast("int").alias("days_suply_num"),
        
        # Cost Fields
        F.coalesce(F.col("patient_cost"), F.lit(0)).cast(DecimalType(10, 2)).alias("ptnt_pay_amt"),
        F.lit(None).cast(DecimalType(10, 2)).alias("othtroop_amt"),  # NULL - other TROOP amount
        F.lit(None).cast(DecimalType(10, 2)).alias("lics_amt"),      # NULL - low income subsidy
        F.lit(None).cast(DecimalType(10, 2)).alias("plro_amt"),      # NULL - patient liability reduction
        F.coalesce(F.col("total_cost"), F.lit(0)).cast(DecimalType(10, 2)).alias("cvrd_d_plan_pd_amt"),  # Covered Plan Paid
        F.coalesce(F.col("payer_coverage"), F.lit(0)).cast(DecimalType(10, 2)).alias("ncvrd_plan_pd_amt"),  # Non-covered Plan Paid
        F.coalesce(F.col("total_cost"), F.lit(0)).cast(DecimalType(10, 2)).alias("tot_rx_cst_amt"),  # Total cost
        
        # Drug Coverage Status
        F.lit("C").alias("drug_cvrg_stus_cd"),  # C = Covered (default assumption)
        
        # Dispensing Status
        F.lit(None).cast("string").alias("daw_prod_slctn_cd"),  # NULL - DAW code not tracked
        
        # Metadata
        F.col("source_system"),
        F.current_timestamp().alias("fact_created_dts"),
    ).filter((F.col("pde_id").isNotNull()) & (F.col("desynpuf_id").isNotNull()))


def build_fact_cms_carrier_claims(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """
    Build CMS Carrier Claims fact table (professional services).
    
    Note: Limited fidelity due to Synthea data structure.
    - Single provider per encounter (no multiple physicians)
    - No line-level billing detail
    - HCPCS codes only available through enrichment
    """
    encounter_table = get_full_table_name(config, "silver", "v_encounter_flattened")
    procedure_table = get_full_table_name(config, "silver", "v_procedure_flattened")
    
    # Get professional service encounters (wellness, outpatient)
    # Alias to 'enc' to avoid ambiguous references
    professional = spark.table(encounter_table).filter(
        F.lower(F.col("encounter_class")).isin(["wellness", "outpatient"])
    ).alias("enc")
    
    # Get procedures with HCPCS codes
    # Alias to 'proc' to avoid ambiguous references
    procedures = spark.table(procedure_table).filter(F.col("encounter_id").isNotNull()).alias("proc")
    proc_window = Window.partitionBy("encounter_id").orderBy("performed_date")
    procedures_ranked = procedures.withColumn("line_num", F.row_number().over(proc_window))
    
    # Join encounters with procedures (line items)
    # Use qualified column references to avoid ambiguity
    return professional.join(procedures_ranked, professional["encounter_id"] == procedures_ranked["encounter_id"], "left").select(
        # Claim Identification
        surrogate_key(professional["encounter_id"]).alias("claim_key"),
        professional["encounter_id"].alias("clm_id"),
        surrogate_key(professional["patient_id"]).alias("beneficiary_key"),
        professional["patient_id"].alias("desynpuf_id"),
        
        # Line Number
        F.coalesce(procedures_ranked["line_num"], F.lit(1)).alias("clm_line_num"),
        
        # Claim Dates (from encounter)
        professional["start_datetime"].cast("date").alias("clm_from_dt"),
        professional["end_datetime"].cast("date").alias("clm_thru_dt"),
        
        # Provider (from encounter)
        surrogate_key(professional["provider_id"]).alias("provider_key"),
        professional["provider_id"].alias("prf_physn_npi"),
        F.lit(None).cast("string").alias("carr_clm_rfrng_pin_num"),  # NULL - referring physician
        
        # Service Information (from procedure)
        procedures_ranked["hcpcs_code"].alias("hcpcs_cd"),  # From enrichment
        procedures_ranked["hcpcs_description"],
        procedures_ranked["betos_code"].alias("betos_cd"),
        procedures_ranked["procedure_description"].alias("line_srvc_desc"),
        
        # Service Location
        F.lit(None).cast("string").alias("carr_clm_cash_ddctbl_apld_amt"),  # NULL
        F.lit(None).cast("string").alias("line_place_of_srvc_cd"),  # NULL
        
        # Financial (Line Level - from procedure)
        F.coalesce(procedures_ranked["procedure_cost"], F.lit(0)).cast(DecimalType(10, 2)).alias("line_nch_pmt_amt"),
        F.coalesce(procedures_ranked["procedure_cost"], F.lit(0)).cast(DecimalType(10, 2)).alias("line_bene_pmt_amt"),
        F.coalesce(procedures_ranked["procedure_cost"], F.lit(0)).cast(DecimalType(10, 2)).alias("line_prvdr_pmt_amt"),
        F.coalesce(procedures_ranked["procedure_cost"], F.lit(0)).cast(DecimalType(10, 2)).alias("line_bene_ptb_ddctbl_amt"),
        F.coalesce(procedures_ranked["procedure_cost"], F.lit(0)).cast(DecimalType(10, 2)).alias("line_bene_prmry_pyr_pd_amt"),
        F.coalesce(procedures_ranked["procedure_cost"], F.lit(0)).cast(DecimalType(10, 2)).alias("line_coinsrnc_amt"),
        F.coalesce(procedures_ranked["procedure_cost"], F.lit(0)).cast(DecimalType(10, 2)).alias("line_sbmtd_chrg_amt"),
        F.coalesce(procedures_ranked["procedure_cost"], F.lit(0)).cast(DecimalType(10, 2)).alias("line_alowd_chrg_amt"),
        F.lit(None).cast(DecimalType(10, 2)).alias("line_prcsg_ind_cd"),  # NULL
        F.lit(None).cast("string").alias("line_pmt_80_100_cd"),  # NULL
        
        # Diagnosis Reference
        F.lit(None).cast("string").alias("line_icd_dgns_cd"),  # NULL - link to encounter diagnoses
        
        # Service Count
        F.lit(1).cast(DecimalType(10, 2)).alias("line_service_cnt"),  # Units of service
        
        # Metadata (from encounter)
        date_key(professional["start_datetime"]).alias("service_date_key"),
        professional["source_system"],
        F.current_timestamp().alias("fact_created_dts"),
    ).filter((F.col("clm_id").isNotNull()) & (F.col("desynpuf_id").isNotNull()))


# =============================================================================
# CMS SUMMARY/AGGREGATE TABLES
# =============================================================================

def build_agg_cms_beneficiary_summary_yearly(spark: SparkSession, config: PipelineConfig) -> DataFrame:
    """
    Build CMS Beneficiary Summary with annual costs aggregated.
    
    Combines beneficiary demographics with annual utilization and costs.
    """
    beneficiary_dim = get_full_table_name(config, "gold", "dim_cms_beneficiary")
    inpatient_claims = get_full_table_name(config, "gold", "fact_cms_inpatient_claims")
    outpatient_claims = get_full_table_name(config, "gold", "fact_cms_outpatient_claims")
    pde_facts = get_full_table_name(config, "gold", "fact_cms_prescription_drug_events")
    carrier_claims = get_full_table_name(config, "gold", "fact_cms_carrier_claims")
    
    # Load beneficiaries and drop NULL cost columns to avoid ambiguity when joining with aggregated costs
    beneficiaries = spark.table(beneficiary_dim).drop(
        "medreimb_ip", "benres_ip", "pppymt_ip",
        "medreimb_op", "benres_op", "pppymt_op",
        "medreimb_car", "benres_car", "pppymt_car"
    )
    
    # Aggregate inpatient costs by year
    ip_costs = spark.table(inpatient_claims).groupBy(
        F.col("desynpuf_id"), 
        F.year(F.col("clm_from_dt")).alias("year")
    ).agg(
        F.sum("clm_pmt_amt").alias("medreimb_ip"),
        F.sum("nch_bene_pta_coinsrnc_lblty_amt").alias("benres_ip"),
        F.sum("nch_prmry_pyr_clm_pd_amt").alias("pppymt_ip")
    )
    
    # Aggregate outpatient costs by year
    op_costs = spark.table(outpatient_claims).groupBy(
        F.col("desynpuf_id"),
        F.year(F.col("clm_from_dt")).alias("year")
    ).agg(
        F.sum("clm_pmt_amt").alias("medreimb_op"),
        F.sum("nch_bene_blood_ddctbl_lblty_am").alias("benres_op"),
        F.sum("nch_prmry_pyr_clm_pd_amt").alias("pppymt_op")
    )
    
    # Aggregate carrier costs by year
    car_costs = spark.table(carrier_claims).groupBy(
        F.col("desynpuf_id"),
        F.year(F.col("clm_from_dt")).alias("year")
    ).agg(
        F.sum("line_nch_pmt_amt").alias("medreimb_car"),
        F.sum("line_bene_ptb_ddctbl_amt").alias("benres_car"),
        F.sum("line_bene_prmry_pyr_pd_amt").alias("pppymt_car")
    )
    
    # Get all unique beneficiary-year combinations from cost tables
    beneficiary_years = (
        ip_costs.select("desynpuf_id", "year")
        .union(op_costs.select("desynpuf_id", "year"))
        .union(car_costs.select("desynpuf_id", "year"))
        .distinct()
    )
    
    # Join beneficiaries with years, then join all cost aggregations
    return (
        beneficiaries
        .join(beneficiary_years, "desynpuf_id", "inner")
        .join(ip_costs, ["desynpuf_id", "year"], "left")
        .join(op_costs, ["desynpuf_id", "year"], "left")
        .join(car_costs, ["desynpuf_id", "year"], "left")
        .select(
            F.col("beneficiary_key"),
            F.col("desynpuf_id"),
            F.col("year"),
            # Demographics
            F.col("bene_birth_dt"),
            F.col("bene_death_dt"),
            F.col("bene_sex_ident_cd"),
            F.col("bene_race_cd"),
            F.col("bene_esrd_ind"),
            F.col("sp_state_code"),
            F.col("bene_county_cd"),
            # Coverage
            F.col("bene_hi_cvrage_tot_mons"),
            F.col("bene_smi_cvrage_tot_mons"),
            F.col("bene_hmo_cvrage_tot_mons"),
            F.col("plan_cvrg_mos_num"),
            # Chronic Conditions
            F.col("sp_alzhdmta"),
            F.col("sp_chf"),
            F.col("sp_chrnkidn"),
            F.col("sp_cncr"),
            F.col("sp_copd"),
            F.col("sp_depressn"),
            F.col("sp_diabetes"),
            F.col("sp_ischmcht"),
            F.col("sp_osteoprs"),
            F.col("sp_ra_oa"),
            F.col("sp_strketia"),
            # Annual Costs
            F.coalesce(F.col("medreimb_ip"), F.lit(0)).alias("medreimb_ip"),
            F.coalesce(F.col("benres_ip"), F.lit(0)).alias("benres_ip"),
            F.coalesce(F.col("pppymt_ip"), F.lit(0)).alias("pppymt_ip"),
            F.coalesce(F.col("medreimb_op"), F.lit(0)).alias("medreimb_op"),
            F.coalesce(F.col("benres_op"), F.lit(0)).alias("benres_op"),
            F.coalesce(F.col("pppymt_op"), F.lit(0)).alias("pppymt_op"),
            F.coalesce(F.col("medreimb_car"), F.lit(0)).alias("medreimb_car"),
            F.coalesce(F.col("benres_car"), F.lit(0)).alias("benres_car"),
            F.coalesce(F.col("pppymt_car"), F.lit(0)).alias("pppymt_car"),
            # Metadata
            F.current_timestamp().alias("agg_refresh_dts"),
        )
    )


# =============================================================================
# CMS TABLE REGISTRY
# =============================================================================

@dataclass(frozen=True)
class CMSTableDef:
    """Definition of a CMS gold table."""
    name: str
    category: str
    build_fn: Callable[[SparkSession, PipelineConfig], DataFrame]
    dependencies: Tuple[str, ...] = ()


CMS_TABLE_REGISTRY: Dict[str, CMSTableDef] = {
    # CMS Dimensions
    "dim_cms_beneficiary": CMSTableDef("dim_cms_beneficiary", "cms_dimension", build_dim_cms_beneficiary),
    "dim_cms_provider": CMSTableDef("dim_cms_provider", "cms_dimension", build_dim_cms_provider),
    
    # CMS Fact Tables
    "fact_cms_inpatient_claims": CMSTableDef("fact_cms_inpatient_claims", "cms_fact", build_fact_cms_inpatient_claims, ("dim_cms_beneficiary", "dim_cms_provider")),
    "fact_cms_outpatient_claims": CMSTableDef("fact_cms_outpatient_claims", "cms_fact", build_fact_cms_outpatient_claims, ("dim_cms_beneficiary", "dim_cms_provider")),
    "fact_cms_prescription_drug_events": CMSTableDef("fact_cms_prescription_drug_events", "cms_fact", build_fact_cms_prescription_drug_events, ("dim_cms_beneficiary",)),
    "fact_cms_carrier_claims": CMSTableDef("fact_cms_carrier_claims", "cms_fact", build_fact_cms_carrier_claims, ("dim_cms_beneficiary", "dim_cms_provider")),
    
    # CMS Aggregates
    "agg_cms_beneficiary_summary_yearly": CMSTableDef("agg_cms_beneficiary_summary_yearly", "cms_aggregate", build_agg_cms_beneficiary_summary_yearly, 
                                                       ("dim_cms_beneficiary", "fact_cms_inpatient_claims", "fact_cms_outpatient_claims", "fact_cms_carrier_claims")),
}


@dataclass
class CMSResult:
    """Result of a CMS table build."""
    table_name: str
    category: str
    row_count: int
    elapsed_seconds: float
    success: bool
    error: Optional[str] = None


def write_cms_table(
    spark: SparkSession,
    config: PipelineConfig,
    table_def: CMSTableDef
) -> CMSResult:
    """Write a CMS gold table."""
    start_time = time.time()
    try:
        df = table_def.build_fn(spark, config)
        full_table = get_full_table_name(config, "gold", table_def.name)
        df.write.format("delta").mode("overwrite").saveAsTable(full_table)
        row_count = spark.table(full_table).count()
        elapsed = time.time() - start_time
        print(f"  Created {full_table} with {row_count:,} rows in {elapsed:.2f}s")
        return CMSResult(table_def.name, table_def.category, row_count, elapsed, True)
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"  ERROR: {table_def.name} - {str(e)}")
        return CMSResult(table_def.name, table_def.category, 0, elapsed, False, str(e))


def run_cms_builds_in_order(
    spark: SparkSession,
    config: PipelineConfig,
    categories: Optional[List[str]] = None
) -> List[CMSResult]:
    """Run CMS gold builds respecting dependencies."""
    if categories is None:
        categories = ["cms_dimension", "cms_fact", "cms_aggregate"]
    
    results = []
    built_tables = set()
    
    for category in categories:
        tables_in_category = [(name, defn) for name, defn in CMS_TABLE_REGISTRY.items() if defn.category == category]
        print(f"\nBuilding {category} tables ({len(tables_in_category)} tables)...")
        
        for name, table_def in tables_in_category:
            missing_deps = [d for d in table_def.dependencies if d not in built_tables]
            if missing_deps:
                print(f"  Skipping {name}: missing dependencies {missing_deps}")
                continue
            
            result = write_cms_table(spark, config, table_def)
            results.append(result)
            if result.success:
                built_tables.add(name)
    
    successful = sum(1 for r in results if r.success)
    total_rows = sum(r.row_count for r in results)
    print(f"\nCompleted: {successful}/{len(results)} CMS tables, {total_rows:,} total rows")
    return results
