"""
RxNorm Reference Data Ingestion

Loads RxNorm (RRF format) data from NLM into Unity Catalog Delta tables.

Source Files (RRF Format - pipe-delimited):
- RXNCONSO.RRF - Concept names and atoms
- RXNSAT.RRF - Attributes (includes NDC codes)
- RXNREL.RRF - Relationships between concepts
- RXNDOC.RRF - Documentation/metadata
- RXNSTY.RRF - Semantic types
- RXNCUI.RRF - CUI history
- RXNCUICHANGES.RRF - CUI changes history
- RXNSAB.RRF - Source abbreviation info
- RXNATOMARCHIVE.RRF - Archived atoms

Output Tables:
- lookup_rxnorm_concepts - RxNorm concepts with preferred names
- lookup_rxnorm_ndc_crosswalk - RxNorm to NDC crosswalk
- lookup_rxnorm_atoms - All concept atoms (RXNCONSO)
- lookup_rxnorm_attributes - Concept attributes (RXNSAT)
- lookup_rxnorm_relationships - Concept relationships (RXNREL)
- lookup_rxnorm_semantic_types - Semantic types (RXNSTY)
- lookup_rxnorm_sources - Source info (RXNSAB)
- lookup_rxnorm_doc - Documentation (RXNDOC)
- lookup_rxnorm_cui_history - CUI history (RXNCUI)
- lookup_rxnorm_cui_changes - CUI changes (RXNCUICHANGES)
- lookup_rxnorm_atom_archive - Archived atoms (RXNATOMARCHIVE)
"""

import time
from typing import Optional, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
# All columns are StringType for bronze layer
from pyspark.sql.window import Window

from .reference_config import (
    ReferenceConfig, ReferenceLoadResult, pipe,
    add_reference_audit_columns, write_reference_table
)


# RxNorm file paths (relative to volume)
# Files are directly in the rxnorm volume
RXNCONSO_FILE = "RXNCONSO.RRF"
RXNSAT_FILE = "RXNSAT.RRF"
RXNREL_FILE = "RXNREL.RRF"
RXNDOC_FILE = "RXNDOC.RRF"
RXNSTY_FILE = "RXNSTY.RRF"
RXNCUI_FILE = "RXNCUI.RRF"
RXNCUICHANGES_FILE = "RXNCUICHANGES.RRF"
RXNSAB_FILE = "RXNSAB.RRF"
RXNATOMARCHIVE_FILE = "RXNATOMARCHIVE.RRF"


def load_rxnconso(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Load and parse RXNCONSO.RRF file.
    
    Pipe-delimited with columns:
    0: RXCUI, 1: LAT, 2: TS, 3: LUI, 4: STT, 5: SUI, 6: ISPREF,
    7: RXAUI, 8: SAUI, 9: SCUI, 10: SDUI, 11: SAB, 12: TTY,
    13: CODE, 14: STR, 15: SRL, 16: SUPPRESS, 17: CVF
    """
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
        .withColumn("is_suppressed", 
                    F.when(F.col("suppress_flag").isin("O", "Y", "E"), "true").otherwise("false"))
        .withColumn("is_preferred_flag", 
                    F.when(F.col("is_preferred") == "Y", "true").otherwise("false"))
    )


def load_rxnsat(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Load and parse RXNSAT.RRF file.
    
    Pipe-delimited with columns:
    0: RXCUI, 1: LUI, 2: SUI, 3: RXAUI, 4: STYPE, 5: CODE,
    6: ATUI, 7: SATUI, 8: ATN, 9: SAB, 10: ATV, 11: SUPPRESS, 12: CVF
    """
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
            F.col("_c11").alias("suppress_flag"),
            F.col("_c12").alias("cvf")
        )
        .withColumn("is_suppressed", 
                    F.when(F.col("suppress_flag").isin("O", "Y", "E"), "true").otherwise("false"))
    )


def load_rxnrel(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Load and parse RXNREL.RRF file (Relationships).
    
    Pipe-delimited with columns:
    0: RXCUI1, 1: RXAUI1, 2: STYPE1, 3: REL, 4: RXCUI2, 5: RXAUI2,
    6: STYPE2, 7: RELA, 8: RUI, 9: SRUI, 10: SAB, 11: SL,
    12: RG, 13: DIR, 14: SUPPRESS, 15: CVF
    """
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
            F.col("_c11").alias("sl"),
            F.col("_c12").alias("rg"),
            F.col("_c13").alias("dir"),
            F.col("_c14").alias("suppress_flag"),
            F.col("_c15").alias("cvf")
        )
        .withColumn("is_suppressed", 
                    F.when(F.col("suppress_flag").isin("O", "Y", "E"), "true").otherwise("false"))
    )


def load_rxnsty(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Load and parse RXNSTY.RRF file (Semantic Types).
    
    Pipe-delimited with columns:
    0: RXCUI, 1: TUI, 2: STN, 3: STY, 4: ATUI, 5: CVF
    """
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
            F.col("_c1").alias("tui"),
            F.col("_c2").alias("stn"),
            F.col("_c3").alias("sty"),
            F.col("_c4").alias("atui"),
            F.col("_c5").alias("cvf")
        )
    )


def load_rxnsab(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Load and parse RXNSAB.RRF file (Source Info).
    
    Pipe-delimited with columns for source vocabulary metadata.
    """
    raw_df = (
        spark.read
        .option("delimiter", "|")
        .option("header", "false")
        .csv(file_path)
    )
    
    return (
        raw_df
        .select(
            F.col("_c0").alias("vcui"),
            F.col("_c1").alias("rcui"),
            F.col("_c2").alias("vsab"),
            F.col("_c3").alias("rsab"),
            F.col("_c4").alias("son"),
            F.col("_c5").alias("sf"),
            F.col("_c6").alias("sver"),
            F.col("_c7").alias("vstart"),
            F.col("_c8").alias("vend"),
            F.col("_c9").alias("imeta"),
            F.col("_c10").alias("rmeta"),
            F.col("_c11").alias("slc"),
            F.col("_c12").alias("scc"),
            F.col("_c13").alias("srl"),
            F.col("_c14").alias("tfr"),
            F.col("_c15").alias("cfr"),
            F.col("_c16").alias("cxty"),
            F.col("_c17").alias("ttyl"),
            F.col("_c18").alias("atnl"),
            F.col("_c19").alias("lat"),
            F.col("_c20").alias("cenc"),
            F.col("_c21").alias("curver"),
            F.col("_c22").alias("sabin"),
            F.col("_c23").alias("ssn"),
            F.col("_c24").alias("scit")
        )
    )


def load_rxndoc(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Load and parse RXNDOC.RRF file (Documentation).
    
    Pipe-delimited with columns:
    0: DOCKEY, 1: VALUE, 2: TYPE, 3: EXPL
    """
    raw_df = (
        spark.read
        .option("delimiter", "|")
        .option("header", "false")
        .csv(file_path)
    )
    
    return (
        raw_df
        .select(
            F.col("_c0").alias("dockey"),
            F.col("_c1").alias("value"),
            F.col("_c2").alias("type"),
            F.col("_c3").alias("expl")
        )
    )


def load_rxncui(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Load and parse RXNCUI.RRF file (CUI History).
    
    Pipe-delimited with columns for CUI history tracking.
    """
    raw_df = (
        spark.read
        .option("delimiter", "|")
        .option("header", "false")
        .csv(file_path)
    )
    
    # Handle variable column count - take first 4 columns if available
    col_count = len(raw_df.columns)
    
    if col_count >= 4:
        return (
            raw_df
            .select(
                F.col("_c0").alias("cui1"),
                F.col("_c1").alias("ver_start"),
                F.col("_c2").alias("ver_end"),
                F.col("_c3").alias("cardinality")
            )
        )
    else:
        return raw_df


def load_rxncuichanges(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Load and parse RXNCUICHANGES.RRF file (CUI Changes).
    
    Pipe-delimited with columns for tracking CUI changes between versions.
    """
    raw_df = (
        spark.read
        .option("delimiter", "|")
        .option("header", "false")
        .csv(file_path)
    )
    
    return (
        raw_df
        .select(
            F.col("_c0").alias("rxaui"),
            F.col("_c1").alias("code"),
            F.col("_c2").alias("sab"),
            F.col("_c3").alias("tty"),
            F.col("_c4").alias("str"),
            F.col("_c5").alias("old_rxcui"),
            F.col("_c6").alias("new_rxcui")
        )
    )


def load_rxnatomarchive(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Load and parse RXNATOMARCHIVE.RRF file (Archived Atoms).
    
    Pipe-delimited with columns for archived/retired atoms.
    """
    raw_df = (
        spark.read
        .option("delimiter", "|")
        .option("header", "false")
        .csv(file_path)
    )
    
    return (
        raw_df
        .select(
            F.col("_c0").alias("rxaui"),
            F.col("_c1").alias("aui"),
            F.col("_c2").alias("str"),
            F.col("_c3").alias("archive_timestamp"),
            F.col("_c4").alias("created_timestamp"),
            F.col("_c5").alias("updated_timestamp"),
            F.col("_c6").alias("code"),
            F.col("_c7").alias("is_brand"),
            F.col("_c8").alias("lat"),
            F.col("_c9").alias("last_released"),
            F.col("_c10").alias("saui"),
            F.col("_c11").alias("vsab"),
            F.col("_c12").alias("rxcui"),
            F.col("_c13").alias("sab"),
            F.col("_c14").alias("tty"),
            F.col("_c15").alias("merged_to_rxcui")
        )
    )


def create_rxnorm_concepts(rxnconso_df: DataFrame) -> DataFrame:
    """Create preferred concept names from RXNCONSO."""
    return (
        rxnconso_df
        .filter(
            (F.col("source_abbrev") == "RXNORM") & 
            (F.col("is_suppressed") == "false") &
            (F.col("language") == "ENG")
        )
        .withColumn(
            "tty_priority",
            F.when(F.col("term_type") == "SCD", "01")   # Semantic Clinical Drug
            .when(F.col("term_type") == "SBD", "02")   # Semantic Branded Drug
            .when(F.col("term_type") == "GPCK", "03")  # Generic Pack
            .when(F.col("term_type") == "BPCK", "04")  # Branded Pack
            .when(F.col("term_type") == "IN", "05")    # Ingredient
            .when(F.col("term_type") == "PIN", "06")   # Precise Ingredient
            .when(F.col("term_type") == "BN", "07")    # Brand Name
            .when(F.col("term_type") == "MIN", "08")   # Multiple Ingredients
            .when(F.col("term_type") == "SCDC", "09")  # Semantic Clinical Drug Component
            .when(F.col("term_type") == "SBDC", "10") # Semantic Branded Drug Component
            .otherwise("99")
        )
        .withColumn(
            "row_num",
            F.row_number().over(
                Window.partitionBy("rxcui").orderBy(F.col("tty_priority"), F.desc("is_preferred_flag"))
            )
        )
        .filter(F.col("row_num") == 1)
        .select(
            "rxcui",
            "rxaui",
            F.col("name").alias("rxnorm_name"),
            F.col("term_type").alias("tty"),
            "source_code"
        )
    )


def create_rxnorm_ndc_crosswalk(rxnsat_df: DataFrame, concepts_df: DataFrame) -> DataFrame:
    """Create RxNorm to NDC crosswalk."""
    ndc_df = (
        rxnsat_df
        .filter(
            (F.col("atn") == "NDC") & 
            (F.col("is_suppressed") == "false") &
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
        .join(concepts_df.select("rxcui", "rxnorm_name", "tty"), "rxcui", "left")
        .select(
            "rxcui",
            "rxnorm_name",
            "tty",
            F.col("ndc_code").alias("ndc_original"),
            "ndc_11_digit",
            "source_abbrev"
        )
        .distinct()
    )


def load_rxnorm(
    spark: SparkSession,
    config: ReferenceConfig
) -> List[ReferenceLoadResult]:
    """
    Load all RxNorm reference data files.
    
    Args:
        spark: SparkSession
        config: Reference configuration
    
    Returns:
        List of ReferenceLoadResult for each table
    """
    results = []
    volume = config.rxnorm_volume
    
    # Helper to load a single table
    def _load_table(table_name: str, file_name: str, loader_func, source_name: str, 
                    filter_suppressed: bool = False) -> ReferenceLoadResult:
        start_time = time.time()
        try:
            file_path = f"{volume}/{file_name}"
            df = loader_func(spark, file_path)
            if filter_suppressed and "is_suppressed" in df.columns:
                df = df.filter(F.col("is_suppressed") == "false")
            df = pipe(df, add_reference_audit_columns(source_name))
            row_count = df.count()
            full_table_name = write_reference_table(spark, config, table_name, df)
            elapsed = time.time() - start_time
            return ReferenceLoadResult(table_name, full_table_name, row_count, round(elapsed, 2), True)
        except Exception as e:
            elapsed = time.time() - start_time
            return ReferenceLoadResult(table_name, "", 0, round(elapsed, 2), False, str(e))
    
    # 1. Load RXNCONSO (atoms)
    conso_path = f"{volume}/{RXNCONSO_FILE}"
    rxnconso_df = load_rxnconso(spark, conso_path)
    
    result = _load_table("lookup_rxnorm_atoms", RXNCONSO_FILE, load_rxnconso, "RXNORM_ATOMS", filter_suppressed=True)
    results.append(result)
    
    # 2. Create and save concepts (derived from RXNCONSO)
    start_time = time.time()
    table_name = "lookup_rxnorm_concepts"
    try:
        df = pipe(
            create_rxnorm_concepts(rxnconso_df),
            add_reference_audit_columns("RXNORM_CONCEPTS")
        )
        row_count = df.count()
        full_table_name = write_reference_table(spark, config, table_name, df)
        elapsed = time.time() - start_time
        results.append(ReferenceLoadResult(table_name, full_table_name, row_count, round(elapsed, 2), True))
        concepts_df = df
    except Exception as e:
        elapsed = time.time() - start_time
        results.append(ReferenceLoadResult(table_name, "", 0, round(elapsed, 2), False, str(e)))
        concepts_df = create_rxnorm_concepts(rxnconso_df)
    
    # 3. Load RXNSAT (attributes)
    sat_path = f"{volume}/{RXNSAT_FILE}"
    rxnsat_df = load_rxnsat(spark, sat_path)
    
    result = _load_table("lookup_rxnorm_attributes", RXNSAT_FILE, load_rxnsat, "RXNORM_ATTRIBUTES", filter_suppressed=True)
    results.append(result)
    
    # 4. Create and save NDC crosswalk (derived from RXNSAT + concepts)
    start_time = time.time()
    table_name = "lookup_rxnorm_ndc_crosswalk"
    try:
        df = pipe(
            create_rxnorm_ndc_crosswalk(rxnsat_df, concepts_df.drop("_load_dts", "_rec_src")),
            add_reference_audit_columns("RXNORM_NDC")
        )
        row_count = df.count()
        full_table_name = write_reference_table(spark, config, table_name, df)
        elapsed = time.time() - start_time
        results.append(ReferenceLoadResult(table_name, full_table_name, row_count, round(elapsed, 2), True))
    except Exception as e:
        elapsed = time.time() - start_time
        results.append(ReferenceLoadResult(table_name, "", 0, round(elapsed, 2), False, str(e)))
    
    # 5. Load RXNREL (relationships)
    result = _load_table("lookup_rxnorm_relationships", RXNREL_FILE, load_rxnrel, "RXNORM_REL", filter_suppressed=True)
    results.append(result)
    
    # 6. Load RXNSTY (semantic types)
    result = _load_table("lookup_rxnorm_semantic_types", RXNSTY_FILE, load_rxnsty, "RXNORM_STY")
    results.append(result)
    
    # 7. Load RXNSAB (sources)
    result = _load_table("lookup_rxnorm_sources", RXNSAB_FILE, load_rxnsab, "RXNORM_SAB")
    results.append(result)
    
    # 8. Load RXNDOC (documentation)
    result = _load_table("lookup_rxnorm_doc", RXNDOC_FILE, load_rxndoc, "RXNORM_DOC")
    results.append(result)
    
    # 9. Load RXNCUI (CUI history)
    result = _load_table("lookup_rxnorm_cui_history", RXNCUI_FILE, load_rxncui, "RXNORM_CUI")
    results.append(result)
    
    # 10. Load RXNCUICHANGES (CUI changes)
    result = _load_table("lookup_rxnorm_cui_changes", RXNCUICHANGES_FILE, load_rxncuichanges, "RXNORM_CUICHANGES")
    results.append(result)
    
    # 11. Load RXNATOMARCHIVE (archived atoms)
    result = _load_table("lookup_rxnorm_atom_archive", RXNATOMARCHIVE_FILE, load_rxnatomarchive, "RXNORM_ARCHIVE")
    results.append(result)
    
    return results


def create_rxnorm_views(spark: SparkSession, config: ReferenceConfig) -> None:
    """Create useful RxNorm views."""
    schema_path = config.full_schema_path
    
    # Clinical drugs (SCD)
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_rxnorm_clinical_drugs AS
    SELECT rxcui, rxnorm_name, tty, source_code
    FROM {schema_path}.lookup_rxnorm_concepts
    WHERE tty = 'SCD'
    """)
    
    # Branded drugs (SBD)
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_rxnorm_branded_drugs AS
    SELECT rxcui, rxnorm_name, tty, source_code
    FROM {schema_path}.lookup_rxnorm_concepts
    WHERE tty = 'SBD'
    """)
    
    # Ingredients
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_rxnorm_ingredients AS
    SELECT rxcui, rxnorm_name, tty, source_code
    FROM {schema_path}.lookup_rxnorm_concepts
    WHERE tty IN ('IN', 'PIN', 'MIN')
    """)
    
    # NDC lookup
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_rxnorm_ndc_lookup AS
    SELECT 
        ndc_11_digit,
        ndc_original,
        rxcui,
        rxnorm_name,
        tty as drug_type
    FROM {schema_path}.lookup_rxnorm_ndc_crosswalk
    """)
    
    # RXCUI to all NDCs
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_rxcui_ndc_all AS
    SELECT 
        rxcui,
        rxnorm_name,
        tty,
        COLLECT_SET(ndc_11_digit) as ndc_codes,
        COUNT(DISTINCT ndc_11_digit) as ndc_count
    FROM {schema_path}.lookup_rxnorm_ndc_crosswalk
    GROUP BY rxcui, rxnorm_name, tty
    """)
    
    # Drug relationships view
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_rxnorm_drug_relationships AS
    SELECT 
        r.rxcui1,
        c1.rxnorm_name as concept1_name,
        r.rel,
        r.rela,
        r.rxcui2,
        c2.rxnorm_name as concept2_name
    FROM {schema_path}.lookup_rxnorm_relationships r
    LEFT JOIN {schema_path}.lookup_rxnorm_concepts c1 ON r.rxcui1 = c1.rxcui
    LEFT JOIN {schema_path}.lookup_rxnorm_concepts c2 ON r.rxcui2 = c2.rxcui
    WHERE r.is_suppressed = 'false'
    """)
    
    # Drug ingredients (has_ingredient relationship)
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_rxnorm_drug_ingredients AS
    SELECT 
        r.rxcui1 as drug_rxcui,
        c1.rxnorm_name as drug_name,
        c1.tty as drug_type,
        r.rxcui2 as ingredient_rxcui,
        c2.rxnorm_name as ingredient_name
    FROM {schema_path}.lookup_rxnorm_relationships r
    JOIN {schema_path}.lookup_rxnorm_concepts c1 ON r.rxcui1 = c1.rxcui
    JOIN {schema_path}.lookup_rxnorm_concepts c2 ON r.rxcui2 = c2.rxcui
    WHERE r.rela = 'has_ingredient'
      AND r.is_suppressed = 'false'
      AND c1.tty IN ('SCD', 'SBD', 'GPCK', 'BPCK')
      AND c2.tty IN ('IN', 'PIN', 'MIN')
    """)
    
    # Semantic types view
    spark.sql(f"""
    CREATE OR REPLACE VIEW {schema_path}.v_rxnorm_concepts_with_types AS
    SELECT 
        c.rxcui,
        c.rxnorm_name,
        c.tty,
        s.tui,
        s.sty as semantic_type
    FROM {schema_path}.lookup_rxnorm_concepts c
    LEFT JOIN {schema_path}.lookup_rxnorm_semantic_types s ON c.rxcui = s.rxcui
    """)
    
    print(f"Created RxNorm views in {schema_path}")
