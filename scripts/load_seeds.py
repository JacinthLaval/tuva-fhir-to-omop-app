import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, DateType


TABLE_SCHEMA_MAP = {
    "CONCEPT.csv": {
        "table": "terminology.concept",
        "columns": [
            "concept_id", "concept_name", "domain_id", "vocabulary_id",
            "concept_class_id", "standard_concept", "concept_code",
            "valid_start_date", "valid_end_date", "invalid_reason",
        ],
    },
    "CONCEPT_RELATIONSHIP.csv": {
        "table": "terminology.concept_relationship",
        "columns": [
            "concept_id_1", "concept_id_2", "relationship_id",
            "valid_start_date", "valid_end_date", "invalid_reason",
        ],
    },
    "LOINC_TO_OMOP.csv": {
        "table": "terminology.loinc_to_omop",
        "columns": [
            "loinc_code", "loinc_description", "omop_concept_id",
            "omop_concept_name", "omop_domain_id", "omop_vocabulary_id",
            "standard_concept",
        ],
    },
    "SNOMED_TO_OMOP.csv": {
        "table": "terminology.snomed_to_omop",
        "columns": [
            "snomed_code", "snomed_description", "omop_concept_id",
            "omop_concept_name", "omop_domain_id", "omop_vocabulary_id",
            "standard_concept",
        ],
    },
    "RXNORM_TO_OMOP.csv": {
        "table": "terminology.rxnorm_to_omop",
        "columns": [
            "rxnorm_code", "rxnorm_description", "omop_concept_id",
            "omop_concept_name", "omop_domain_id", "omop_vocabulary_id",
            "standard_concept",
        ],
    },
    "ICD10CM_TO_OMOP.csv": {
        "table": "terminology.icd10cm_to_omop",
        "columns": [
            "icd10cm_code", "icd10cm_description", "omop_concept_id",
            "omop_concept_name", "omop_domain_id", "omop_vocabulary_id",
            "standard_concept",
        ],
    },
    "ICD10PCS_TO_OMOP.csv": {
        "table": "terminology.icd10pcs_to_omop",
        "columns": [
            "icd10pcs_code", "icd10pcs_description", "omop_concept_id",
            "omop_concept_name", "omop_domain_id", "omop_vocabulary_id",
            "standard_concept",
        ],
    },
    "CPT_TO_OMOP.csv": {
        "table": "terminology.cpt_to_omop",
        "columns": [
            "cpt_code", "cpt_description", "omop_concept_id",
            "omop_concept_name", "omop_domain_id", "omop_vocabulary_id",
            "standard_concept",
        ],
    },
    "HCPCS_TO_OMOP.csv": {
        "table": "terminology.hcpcs_to_omop",
        "columns": [
            "hcpcs_code", "hcpcs_description", "omop_concept_id",
            "omop_concept_name", "omop_domain_id", "omop_vocabulary_id",
            "standard_concept",
        ],
    },
    "DEMOGRAPHIC_TO_OMOP.csv": {
        "table": "terminology.demographic_to_omop",
        "columns": [
            "source_system", "source_code", "source_description",
            "omop_concept_id", "omop_concept_name", "omop_domain_id",
            "category",
        ],
    },
}


def load_vocabulary_seeds(session: Session, stage_path: str) -> str:
    """
    Load Athena vocabulary CSVs from a Snowflake stage into terminology tables.

    Parameters
    ----------
    session : snowpark Session  (injected by Snowflake when run as stored proc)
    stage_path : str
        Fully-qualified stage path containing the CSV files,
        e.g. '@my_db.my_schema.vocab_stage/athena_export/'

    Returns
    -------
    str  Summary of rows loaded per table.
    """
    stage_path = stage_path.rstrip("/")
    results = []
    files_found = 0

    for csv_filename, meta in TABLE_SCHEMA_MAP.items():
        file_path = f"{stage_path}/{csv_filename}"
        target_table = meta["table"]
        columns = meta["columns"]

        try:
            df = (
                session.read
                .option("FIELD_DELIMITER", "\t")
                .option("SKIP_HEADER", 1)
                .option("FIELD_OPTIONALLY_ENCLOSED_BY", '"')
                .option("NULL_IF", ("\\\\N", ""))
                .option("ENCODING", "UTF8")
                .csv(file_path)
            )

            for i, col_name in enumerate(columns):
                df = df.with_column_renamed(df.columns[i], col_name)

            df = df.select(columns)

            row_count = df.count()
            if row_count == 0:
                results.append(f"  {csv_filename} -> {target_table}: EMPTY (skipped)")
                continue

            session.sql(f"TRUNCATE TABLE IF EXISTS {target_table}").collect()
            df.write.mode("append").save_as_table(target_table, table_type="")

            results.append(f"  {csv_filename} -> {target_table}: {row_count} rows loaded")
            files_found += 1

        except Exception as e:
            err_msg = str(e)
            if "does not exist" in err_msg.lower() or "file not found" in err_msg.lower():
                results.append(f"  {csv_filename} -> {target_table}: FILE NOT FOUND (skipped)")
            else:
                results.append(f"  {csv_filename} -> {target_table}: ERROR — {err_msg[:200]}")

    summary = (
        f"Vocabulary seed load complete.\n"
        f"Stage: {stage_path}\n"
        f"Files processed: {files_found}/{len(TABLE_SCHEMA_MAP)}\n\n"
        + "\n".join(results)
    )
    return summary
