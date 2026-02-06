"""
Create workshop schema and views that map gold tables to analyst-expected names.
fact_patient_claims, rprt_patient_claims, dim_beneficiary, dim_provider, dim_date, dim_diagnosis.

Expects the same gold layer produced by BuildTheWorkspace.ipynb (dbx_hls_workspace_setup):
- run_gold_builds_in_order: dim_date, dim_provider, etc.
- run_cms_builds_in_order: dim_cms_beneficiary, dim_cms_provider, fact_cms_outpatient_claims,
  fact_cms_inpatient_claims. Config in that notebook: catalog=healthcare_dev, gold_schema=gold.

Run from a Databricks notebook with spark in scope.
"""
from typing import Optional


def create_workshop_schemas(spark, catalog: str, workshop_schema: str = "cms", ai_schema: str = "ai") -> None:
    """Create catalog schemas cms and ai if they do not exist."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{workshop_schema}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{ai_schema}")
    print(f"Schemas ready: {catalog}.{workshop_schema}, {catalog}.{ai_schema}")


def create_workshop_views(
    spark,
    catalog: str,
    gold_schema: str = "gold",
    workshop_schema: str = "cms",
) -> None:
    """
    Create views in workshop_schema that alias or derive from gold tables.
    Expects gold to contain: dim_cms_beneficiary, dim_cms_provider, fact_cms_outpatient_claims,
    fact_cms_inpatient_claims, dim_date. dim_diagnosis is built from silver if available.
    """
    g = gold_schema
    c = catalog
    w = workshop_schema
    full_g = f"{c}.{g}"
    full_w = f"{c}.{w}"

    # dim_beneficiary: alias dim_cms_beneficiary with analyst-friendly column names
    spark.sql(f"""
        CREATE OR REPLACE VIEW {full_w}.dim_beneficiary AS
        SELECT
            beneficiary_key,
            desynpuf_id AS beneficiary_code,
            bene_birth_dt AS date_of_birth,
            bene_death_dt AS death_date,
            CASE bene_sex_ident_cd WHEN 1 THEN 'M' WHEN 2 THEN 'F' ELSE 'U' END AS gender,
            bene_race_cd AS race_cd,
            sp_state_code AS state,
            bene_county_cd AS county_cd,
            bene_esrd_ind,
            sp_alzhdmta, sp_chf, sp_chrnkidn, sp_cncr, sp_copd, sp_depressn,
            sp_diabetes, sp_ischmcht, sp_osteoprs, sp_ra_oa, sp_strketia,
            source_system, dim_created_dts, dim_refresh_dts
        FROM {full_g}.dim_cms_beneficiary
    """)
    print(f"Created {full_w}.dim_beneficiary")

    # dim_provider: alias dim_cms_provider
    spark.sql(f"""
        CREATE OR REPLACE VIEW {full_w}.dim_provider AS
        SELECT
            provider_key,
            provider_id,
            prvdr_num,
            npi,
            provider_name,
            provider_specialty,
            specialty_code,
            gender,
            state,
            city,
            zip_code,
            organization_id,
            organization_key,
            active,
            source_system,
            dim_created_dts
        FROM {full_g}.dim_cms_provider
    """)
    print(f"Created {full_w}.dim_provider")

    # dim_date: alias gold dim_date
    spark.sql(f"""
        CREATE OR REPLACE VIEW {full_w}.dim_date AS
        SELECT * FROM {full_g}.dim_date
    """)
    print(f"Created {full_w}.dim_date")

    # fact_patient_claims: union outpatient + inpatient with common column names
    spark.sql(f"""
        CREATE OR REPLACE VIEW {full_w}.fact_patient_claims AS
        SELECT
            claim_key AS claim_id,
            beneficiary_key,
            desynpuf_id AS beneficiary_code,
            'Outpatient' AS claim_type,
            provider_key AS attending_physician_provider_key,
            at_physn_npi AS attending_physician_npi,
            clm_from_dt AS claim_start_date,
            clm_thru_dt AS claim_end_date,
            CAST(NULL AS DATE) AS inpatient_admission_date,
            clm_pmt_amt AS claim_payment_amount,
            nch_prmry_pyr_clm_pd_amt AS primary_payer_claim_paid_amount,
            service_date_key,
            source_system,
            fact_created_dts
        FROM {full_g}.fact_cms_outpatient_claims
        UNION ALL
        SELECT
            claim_key AS claim_id,
            beneficiary_key,
            desynpuf_id AS beneficiary_code,
            'Inpatient' AS claim_type,
            provider_key AS attending_physician_provider_key,
            at_physn_npi AS attending_physician_npi,
            clm_from_dt AS claim_start_date,
            clm_thru_dt AS claim_end_date,
            nch_bene_dschrg_dt AS inpatient_admission_date,
            clm_pmt_amt AS claim_payment_amount,
            nch_prmry_pyr_clm_pd_amt AS primary_payer_claim_paid_amount,
            service_date_key,
            source_system,
            fact_created_dts
        FROM {full_g}.fact_cms_inpatient_claims
    """)
    print(f"Created {full_w}.fact_patient_claims")

    # rprt_patient_claims: reporting view joining fact + dim_beneficiary + dim_provider
    spark.sql(f"""
        CREATE OR REPLACE VIEW {full_w}.rprt_patient_claims AS
        SELECT
            b.beneficiary_key,
            b.desynpuf_id AS beneficiary_code,
            b.bene_birth_dt AS date_of_birth,
            b.bene_death_dt AS death_date,
            CASE b.bene_sex_ident_cd WHEN 1 THEN 'M' WHEN 2 THEN 'F' ELSE 'U' END AS gender,
            b.sp_state_code AS state,
            p.provider_key,
            p.provider_id,
            p.provider_name,
            p.state AS provider_state,
            c.claim_id,
            c.claim_type,
            c.claim_start_date,
            c.claim_end_date,
            c.inpatient_admission_date,
            c.claim_payment_amount,
            c.primary_payer_claim_paid_amount
        FROM {full_w}.fact_patient_claims c
        JOIN {full_g}.dim_cms_beneficiary b ON c.beneficiary_key = b.beneficiary_key
        LEFT JOIN {full_g}.dim_cms_provider p ON c.attending_physician_provider_key = p.provider_key
    """)
    print(f"Created {full_w}.rprt_patient_claims")

    # dim_diagnosis: from silver conditions if available; otherwise minimal stub
    silver_schema = "silver"
    try:
        spark.table(f"{c}.{silver_schema}.v_condition_flattened").limit(1).collect()
        spark.sql(f"""
            CREATE OR REPLACE VIEW {full_w}.dim_diagnosis AS
            SELECT
                sha2(coalesce(condition_code, ''), 256) AS diagnosis_key,
                condition_code AS diagnosis_code,
                condition_description AS description,
                icd10cm_code,
                source_system
            FROM (
                SELECT DISTINCT condition_code, condition_description, icd10cm_code, source_system
                FROM {c}.{silver_schema}.v_condition_flattened
                WHERE condition_code IS NOT NULL
            )
        """)
        print(f"Created {full_w}.dim_diagnosis from silver conditions")
    except Exception:
        # Stub: no rows, but schema exists for Genie/lineage
        spark.sql(f"""
            CREATE OR REPLACE VIEW {full_w}.dim_diagnosis AS
            SELECT
                CAST(NULL AS STRING) AS diagnosis_key,
                CAST(NULL AS STRING) AS diagnosis_code,
                CAST(NULL AS STRING) AS description,
                CAST(NULL AS STRING) AS icd10cm_code,
                CAST(NULL AS STRING) AS source_system
            FROM {full_g}.dim_cms_beneficiary LIMIT 0
        """)
        print(f"Created {full_w}.dim_diagnosis (stub)")
    return None
