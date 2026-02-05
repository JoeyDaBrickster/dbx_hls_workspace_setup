"""
Lookup Data Ingestion Orchestrator

Runs all lookup/reference data loaders and creates associated views.
Lookup tables are stored in the bronze schema with "lookup_" prefix.
This is the main entry point for loading healthcare reference data.

Usage:
    from workspaceSetup.reference_ingestion import run_reference_ingestion
    from workspaceSetup.reference_config import create_reference_config
    
    config = create_reference_config(catalog="healthcare_dev", schema="bronze")
    results = run_reference_ingestion(spark, config)
"""

import time
from typing import List, Optional, Set

from pyspark.sql import SparkSession

from .reference_config import (
    ReferenceConfig, ReferenceLoadResult,
    setup_reference_schema, print_reference_config_summary
)
from .reference_hcpcs import load_hcpcs, create_hcpcs_views
from .reference_icd10cm import load_icd10cm, create_icd10cm_views
from .reference_ndc import load_ndc, create_ndc_views
from .reference_snomed_icd10cm import load_snomed_icd10cm, create_snomed_icd10cm_views
from .reference_rxnorm import load_rxnorm, create_rxnorm_views
from .reference_rxnorm_prescribe import load_rxnorm_prescribe, create_rxnorm_prescribe_views


# Available reference data loaders
AVAILABLE_LOADERS = {
    "hcpcs",
    "icd10cm",
    "ndc",
    "snomed_icd10cm",
    "rxnorm",
    "rxnorm_prescribe"
}


def run_reference_ingestion(
    spark: SparkSession,
    config: ReferenceConfig,
    loaders: Optional[Set[str]] = None,
    create_views: bool = True,
    verbose: bool = True
) -> List[ReferenceLoadResult]:
    """
    Run reference data ingestion for all or selected loaders.
    
    Args:
        spark: SparkSession
        config: Reference configuration
        loaders: Set of loader names to run (None = all)
        create_views: Whether to create views after loading
        verbose: Print progress messages
    
    Returns:
        List of ReferenceLoadResult for all tables
    """
    total_start = time.time()
    results = []
    
    # Determine which loaders to run
    loaders_to_run = loaders if loaders else AVAILABLE_LOADERS
    invalid_loaders = loaders_to_run - AVAILABLE_LOADERS
    if invalid_loaders:
        raise ValueError(f"Invalid loader names: {invalid_loaders}. Valid: {AVAILABLE_LOADERS}")
    
    if verbose:
        print_reference_config_summary(config)
        print(f"\nLoaders to run: {', '.join(sorted(loaders_to_run))}")
        print("\n" + "="*70)
    
    # Setup schema
    setup_reference_schema(spark, config)
    
    # Run each loader
    if "hcpcs" in loaders_to_run:
        if verbose:
            print("\n[1/6] Loading HCPCS reference data...")
        result = load_hcpcs(spark, config)
        results.append(result)
        _print_result(result, verbose)
        if create_views and result.success:
            create_hcpcs_views(spark, config)
    
    if "icd10cm" in loaders_to_run:
        if verbose:
            print("\n[2/6] Loading ICD-10-CM reference data...")
        result = load_icd10cm(spark, config)
        results.append(result)
        _print_result(result, verbose)
        if create_views and result.success:
            create_icd10cm_views(spark, config)
    
    if "ndc" in loaders_to_run:
        if verbose:
            print("\n[3/6] Loading NDC reference data...")
        ndc_results = load_ndc(spark, config)
        results.extend(ndc_results)
        for r in ndc_results:
            _print_result(r, verbose)
        if create_views and all(r.success for r in ndc_results):
            create_ndc_views(spark, config)
    
    if "snomed_icd10cm" in loaders_to_run:
        if verbose:
            print("\n[4/6] Loading SNOMED to ICD-10-CM crosswalk...")
        result = load_snomed_icd10cm(spark, config)
        results.append(result)
        _print_result(result, verbose)
        if create_views and result.success:
            create_snomed_icd10cm_views(spark, config)
    
    if "rxnorm" in loaders_to_run:
        if verbose:
            print("\n[5/6] Loading RxNorm reference data...")
        rxnorm_results = load_rxnorm(spark, config)
        results.extend(rxnorm_results)
        for r in rxnorm_results:
            _print_result(r, verbose)
        if create_views and all(r.success for r in rxnorm_results):
            create_rxnorm_views(spark, config)
    
    if "rxnorm_prescribe" in loaders_to_run:
        if verbose:
            print("\n[6/6] Loading RxNorm Prescribe reference data...")
        prescribe_results = load_rxnorm_prescribe(spark, config)
        results.extend(prescribe_results)
        for r in prescribe_results:
            _print_result(r, verbose)
        if create_views and all(r.success for r in prescribe_results):
            create_rxnorm_prescribe_views(spark, config)
    
    # Summary
    total_elapsed = time.time() - total_start
    if verbose:
        _print_summary(results, total_elapsed)
    
    return results


def _print_result(result: ReferenceLoadResult, verbose: bool) -> None:
    """Print result for a single table."""
    if not verbose:
        return
    status = "OK" if result.success else f"FAILED: {result.error}"
    print(f"  {result.table_name}: {status} - {result.row_count:,} rows in {result.elapsed_seconds}s")


def _print_summary(results: List[ReferenceLoadResult], total_elapsed: float) -> None:
    """Print summary of all results."""
    print("\n" + "="*70)
    print("LOOKUP DATA INGESTION SUMMARY")
    print("="*70)
    
    successful = [r for r in results if r.success]
    failed = [r for r in results if not r.success]
    total_rows = sum(r.row_count for r in successful)
    
    print(f"\nTables loaded: {len(successful)}/{len(results)}")
    print(f"Total rows: {total_rows:,}")
    print(f"Total time: {round(total_elapsed, 2)}s")
    
    if failed:
        print("\nFailed tables:")
        for r in failed:
            print(f"  - {r.table_name}: {r.error}")
    
    print("\nSuccessful tables:")
    for r in successful:
        print(f"  - {r.full_table_name} ({r.row_count:,} rows)")
    
    print("="*70)


def run_reference_ingestion_single(
    spark: SparkSession,
    config: ReferenceConfig,
    loader: str,
    create_views: bool = True
) -> List[ReferenceLoadResult]:
    """
    Run a single reference data loader.
    
    Args:
        spark: SparkSession
        config: Reference configuration
        loader: Name of loader to run
        create_views: Whether to create views after loading
    
    Returns:
        List of ReferenceLoadResult
    """
    return run_reference_ingestion(
        spark, config, 
        loaders={loader}, 
        create_views=create_views,
        verbose=True
    )


# Quick verification functions
def verify_reference_tables(spark: SparkSession, config: ReferenceConfig) -> None:
    """Verify all lookup tables exist and have data."""
    schema_path = config.full_schema_path
    
    tables = [
        # Core lookup tables
        "lookup_hcpcs_codes",
        "lookup_icd10cm_codes",
        "lookup_ndc_product",
        "lookup_ndc_package",
        "lookup_snomed_icd10cm_map",
        # RxNorm lookup tables
        "lookup_rxnorm_concepts",
        "lookup_rxnorm_ndc_crosswalk",
        "lookup_rxnorm_atoms",
        "lookup_rxnorm_attributes",
        "lookup_rxnorm_relationships",
        "lookup_rxnorm_semantic_types",
        "lookup_rxnorm_sources",
        "lookup_rxnorm_doc",
        "lookup_rxnorm_cui_history",
        "lookup_rxnorm_cui_changes",
        "lookup_rxnorm_atom_archive",
        # RxNorm Prescribe lookup tables
        "lookup_rxnorm_prescribe_drugs",
        "lookup_rxnorm_prescribe_ingredients",
        "lookup_rxnorm_prescribe_ndc",
        "lookup_rxnorm_prescribe_drug_ingredients"
    ]
    
    print(f"\nVerifying lookup tables in {schema_path}:")
    print("-" * 60)
    
    for table in tables:
        try:
            count = spark.sql(f"SELECT COUNT(*) as cnt FROM {schema_path}.{table}").collect()[0]["cnt"]
            print(f"  {table}: {count:,} rows")
        except Exception as e:
            print(f"  {table}: MISSING or ERROR - {str(e)[:50]}")
    
    print("-" * 60)
