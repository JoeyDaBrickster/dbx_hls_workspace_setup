"""
Synthea Healthcare Data Pipeline

A modular Python pipeline for ingesting and transforming Synthea healthcare data
into a medallion architecture (Bronze/Silver/Gold) on Databricks.

Usage:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    
    from workspaceSetup import run_pipeline, create_config
    
    config = create_config(catalog="my_catalog")
    result = run_pipeline(spark, config)
"""

from .config import (
    PipelineConfig,
    create_config,
    create_config_from_widgets,
    setup_widgets,
    setup_catalog_and_schemas,
    get_full_table_name,
    pipe,
    compose
)

from .run_pipeline import (
    run_pipeline,
    main,
    main_with_widgets,
    PipelineResult,
    StageResult
)

from .cleanup import cleanup, CleanupConfig, CleanupResult

__all__ = [
    # Config
    "PipelineConfig",
    "create_config",
    "create_config_from_widgets",
    "setup_widgets",
    "setup_catalog_and_schemas",
    "get_full_table_name",
    "pipe",
    "compose",
    # Pipeline
    "run_pipeline",
    "main",
    "main_with_widgets",
    "PipelineResult",
    "StageResult",
    # Cleanup
    "cleanup",
    "CleanupConfig",
    "CleanupResult",
]
