"""
Pipeline Orchestration

End-to-end pipeline orchestration for the Synthea Healthcare Data Pipeline.

Usage:
    # Run in Databricks notebook or as a script
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    
    from run_pipeline import run_pipeline
    from config import create_config
    
    config = create_config(catalog="my_catalog")
    result = run_pipeline(spark, config)
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional
import uuid
import json
import time

from pyspark.sql import SparkSession

from .config import (
    PipelineConfig, create_config, create_config_from_widgets,
    setup_catalog_and_schemas, setup_widgets, print_config_summary,
    discover_state_directories
)
from .bronze_ingestion import run_bronze_ingestion, BRONZE_SCHEMAS
from .silver_transformations import run_all_silver_transforms
from .gold_dimensions import run_gold_builds_in_order


@dataclass
class StageResult:
    """Result of a pipeline stage."""
    stage_name: str
    tables_processed: int
    tables_successful: int
    total_rows: int
    elapsed_seconds: float
    success: bool
    errors: List[str] = field(default_factory=list)


@dataclass
class PipelineResult:
    """Result of the full pipeline run."""
    run_id: str
    config: PipelineConfig
    start_time: datetime
    end_time: datetime
    stages: List[StageResult]
    success: bool
    
    @property
    def total_elapsed_seconds(self) -> float:
        return (self.end_time - self.start_time).total_seconds()
    
    @property
    def total_rows(self) -> int:
        return sum(s.total_rows for s in self.stages)
    
    def to_dict(self) -> Dict:
        return {
            "run_id": self.run_id,
            "catalog": self.config.catalog,
            "volume_source": self.config.volume_source,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat(),
            "elapsed_seconds": self.total_elapsed_seconds,
            "success": self.success,
            "total_rows": self.total_rows,
            "stages": [
                {
                    "name": s.stage_name,
                    "tables": s.tables_processed,
                    "successful": s.tables_successful,
                    "rows": s.total_rows,
                    "seconds": s.elapsed_seconds,
                    "success": s.success,
                    "errors": s.errors
                }
                for s in self.stages
            ]
        }


def run_bronze_stage(spark: SparkSession, config: PipelineConfig) -> StageResult:
    """Run bronze ingestion stage."""
    print("\n" + "="*60)
    print("BRONZE STAGE: Raw CSV Ingestion")
    print("="*60)
    
    start_time = time.time()
    
    results = run_bronze_ingestion(spark, config, include_text=True)
    
    successful = [r for r in results if r.success]
    failed = [r for r in results if not r.success]
    total_rows = sum(r.row_count for r in successful)
    elapsed = time.time() - start_time
    
    errors = [f"{r.table_name}: {r.error}" for r in failed]
    
    return StageResult(
        stage_name="bronze",
        tables_processed=len(results),
        tables_successful=len(successful),
        total_rows=total_rows,
        elapsed_seconds=round(elapsed, 2),
        success=len(failed) == 0,
        errors=errors
    )


def run_silver_stage(spark: SparkSession, config: PipelineConfig) -> StageResult:
    """Run silver transformation stage."""
    print("\n" + "="*60)
    print("SILVER STAGE: FHIR Transformations")
    print("="*60)
    
    start_time = time.time()
    
    results = run_all_silver_transforms(spark, config)
    
    successful = [r for r in results if r.success]
    failed = [r for r in results if not r.success]
    total_rows = sum(r.row_count for r in successful)
    elapsed = time.time() - start_time
    
    errors = [f"{r.table_name}: {r.error}" for r in failed]
    
    return StageResult(
        stage_name="silver",
        tables_processed=len(results),
        tables_successful=len(successful),
        total_rows=total_rows,
        elapsed_seconds=round(elapsed, 2),
        success=len(failed) == 0,
        errors=errors
    )


def run_gold_stage(spark: SparkSession, config: PipelineConfig) -> StageResult:
    """Run gold dimensional model stage."""
    print("\n" + "="*60)
    print("GOLD STAGE: Dimensional Models")
    print("="*60)
    
    start_time = time.time()
    
    results = run_gold_builds_in_order(spark, config)
    
    successful = [r for r in results if r.success]
    failed = [r for r in results if not r.success]
    total_rows = sum(r.row_count for r in successful)
    elapsed = time.time() - start_time
    
    errors = [f"{r.table_name}: {r.error}" for r in failed]
    
    return StageResult(
        stage_name="gold",
        tables_processed=len(results),
        tables_successful=len(successful),
        total_rows=total_rows,
        elapsed_seconds=round(elapsed, 2),
        success=len(failed) == 0,
        errors=errors
    )


def run_pipeline(
    spark: SparkSession,
    config: PipelineConfig,
    stages: str = "all",
    dbutils = None
) -> PipelineResult:
    """Run the full data pipeline.
    
    Args:
        spark: SparkSession
        config: Pipeline configuration
        stages: Which stages to run ('all', 'bronze', 'silver', 'gold', 'bronze_silver', 'silver_gold')
        dbutils: Optional dbutils for state discovery
    
    Returns:
        PipelineResult with run metrics
    """
    run_id = str(uuid.uuid4())[:8]
    start_time = datetime.now()
    stage_results = []
    
    print("\n" + "#"*60)
    print(f"SYNTHEA HEALTHCARE DATA PIPELINE - Run: {run_id}")
    print("#"*60)
    
    states = discover_state_directories(dbutils, config.volume_source)
    print_config_summary(config, len(states))
    
    print(f"\nStages to run: {stages}")
    
    # Setup catalog and schemas
    print("\n[Setup] Creating catalog and schemas...")
    setup_catalog_and_schemas(spark, config)
    print("[Setup] Done")
    
    # Determine which stages to run
    run_bronze = stages in ("all", "bronze", "bronze_silver")
    run_silver = stages in ("all", "silver", "bronze_silver", "silver_gold")
    run_gold = stages in ("all", "gold", "silver_gold")
    
    # Run stages
    if run_bronze:
        stage_results.append(run_bronze_stage(spark, config))
    
    if run_silver:
        stage_results.append(run_silver_stage(spark, config))
    
    if run_gold:
        stage_results.append(run_gold_stage(spark, config))
    
    end_time = datetime.now()
    
    result = PipelineResult(
        run_id=run_id,
        config=config,
        start_time=start_time,
        end_time=end_time,
        stages=stage_results,
        success=all(s.success for s in stage_results)
    )
    
    # Print summary
    print("\n" + "#"*60)
    print("PIPELINE EXECUTION SUMMARY")
    print("#"*60)
    print(f"\nRun ID:       {result.run_id}")
    print(f"Status:       {'SUCCESS' if result.success else 'FAILED'}")
    print(f"Duration:     {result.total_elapsed_seconds:.1f} seconds")
    print(f"Total Rows:   {result.total_rows:,}")
    
    print("\nStage Results:")
    print("-" * 60)
    for stage in result.stages:
        status = "OK" if stage.success else "FAILED"
        print(f"  {stage.stage_name.upper():10} | {status:6} | {stage.tables_successful}/{stage.tables_processed} tables | {stage.total_rows:,} rows | {stage.elapsed_seconds}s")
        if stage.errors:
            for err in stage.errors:
                print(f"    ERROR: {err}")
    
    print("\n" + "#"*60)
    
    return result


def main_with_widgets(spark: SparkSession, dbutils) -> PipelineResult:
    """Main entry point when running with Databricks widgets."""
    # Setup widgets
    setup_widgets(dbutils)
    dbutils.widgets.dropdown("stages", "all", ["all", "bronze", "silver", "gold", "bronze_silver", "silver_gold"], "Pipeline Stages")
    
    # Create config from widgets
    config = create_config_from_widgets(dbutils)
    stages = dbutils.widgets.get("stages")
    
    return run_pipeline(spark, config, stages=stages, dbutils=dbutils)


def main(
    spark: SparkSession,
    catalog: str = "healthcare_dev",
    bronze_schema: str = "bronze",
    silver_schema: str = "silver",
    gold_schema: str = "gold",
    volume_source: str = "/Volumes/hls_bronze/hls_bronze/raw/syntheticData",
    source_system: str = "SYNTHEA",
    stages: str = "all"
) -> PipelineResult:
    """Main entry point with explicit parameters."""
    config = create_config(
        catalog=catalog,
        bronze_schema=bronze_schema,
        silver_schema=silver_schema,
        gold_schema=gold_schema,
        volume_source=volume_source,
        source_system=source_system
    )
    
    return run_pipeline(spark, config, stages=stages)


# Entry point for running as a script
if __name__ == "__main__":
    # When running as a script in Databricks, spark is available globally
    result = main(spark)
    print("\nPipeline run metadata (JSON):")
    print(json.dumps(result.to_dict(), indent=2))
