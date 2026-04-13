-- =============================================================================
-- FHIR-to-OMOP CDM — Transformation Stored Procedures (custom-built)
--
-- FHIR parsing: uses Snowflake VARIANT + LATERAL FLATTEN (inspired by
--   Tuva Health's FHIR Inferno patterns, Apache 2.0)
-- OMOP mapping: original logic mapping FHIR R4 resources to OMOP CDM v5.4
--   tables via vocabulary crosswalk lookups
-- Multi-cloud: pure Snowflake SQL + Python, no cloud-specific features
-- =============================================================================

-- ---------------------------------------------------------------------------
-- FHIR Bundle Parser — extracts resources from FHIR R4 JSON bundles
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.parse_fhir_bundles(
    source_table VARCHAR,
    json_column  VARCHAR DEFAULT 'RAW_JSON'
)
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, source_table: str, json_column: str) -> str:
    import json
    from snowflake.snowpark.functions import col, parse_json, lit, current_timestamp

    session.sql("CREATE SCHEMA IF NOT EXISTS app_state").collect()
    session.sql("""
        CREATE TABLE IF NOT EXISTS app_state.fhir_resources (
            resource_id     VARCHAR(256),
            resource_type   VARCHAR(100),
            bundle_id       VARCHAR(256),
            resource_json   VARIANT,
            parsed_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """).collect()

    extract_sql = f"""
        INSERT INTO app_state.fhir_resources (resource_id, resource_type, bundle_id, resource_json)
        SELECT
            r.value:resource:id::VARCHAR           AS resource_id,
            r.value:resource:resourceType::VARCHAR  AS resource_type,
            src.{json_column}:id::VARCHAR           AS bundle_id,
            r.value:resource                        AS resource_json
        FROM {source_table} src,
            LATERAL FLATTEN(input => src.{json_column}:entry) r
        WHERE r.value:resource:resourceType IS NOT NULL
    """
    result = session.sql(extract_sql).collect()
    count = session.sql("SELECT COUNT(*) AS cnt FROM app_state.fhir_resources").collect()[0]['CNT']
    return f"Parsed {count} FHIR resources from {source_table}"
$$;
GRANT USAGE ON PROCEDURE core.parse_fhir_bundles(VARCHAR, VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Person Mapper — FHIR Patient → OMOP person
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_persons(output_schema VARCHAR DEFAULT 'omop_cdm')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {output_schema}.person (
            person_id                   INTEGER,
            gender_concept_id           INTEGER,
            year_of_birth               INTEGER,
            month_of_birth              INTEGER,
            day_of_birth                INTEGER,
            birth_datetime              TIMESTAMP_NTZ,
            race_concept_id             INTEGER,
            ethnicity_concept_id        INTEGER,
            location_id                 INTEGER,
            provider_id                 INTEGER,
            care_site_id                INTEGER,
            person_source_value         VARCHAR(256),
            gender_source_value         VARCHAR(50),
            gender_source_concept_id    INTEGER,
            race_source_value           VARCHAR(50),
            race_source_concept_id      INTEGER,
            ethnicity_source_value      VARCHAR(50),
            ethnicity_source_concept_id INTEGER
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.person
        SELECT
            ABS(HASH(r.resource_json:id::VARCHAR)) % 2147483647  AS person_id,
            COALESCE(dg.omop_concept_id, 0)                       AS gender_concept_id,
            YEAR(r.resource_json:birthDate::DATE)                  AS year_of_birth,
            MONTH(r.resource_json:birthDate::DATE)                 AS month_of_birth,
            DAY(r.resource_json:birthDate::DATE)                   AS day_of_birth,
            r.resource_json:birthDate::TIMESTAMP_NTZ               AS birth_datetime,
            COALESCE(dr.omop_concept_id, 0)                        AS race_concept_id,
            COALESCE(de.omop_concept_id, 0)                        AS ethnicity_concept_id,
            NULL                                                   AS location_id,
            NULL                                                   AS provider_id,
            NULL                                                   AS care_site_id,
            r.resource_json:id::VARCHAR                            AS person_source_value,
            r.resource_json:gender::VARCHAR                        AS gender_source_value,
            0                                                      AS gender_source_concept_id,
            ext_race.value:valueCoding:display::VARCHAR             AS race_source_value,
            0                                                      AS race_source_concept_id,
            ext_eth.value:valueCoding:display::VARCHAR              AS ethnicity_source_value,
            0                                                      AS ethnicity_source_concept_id
        FROM app_state.fhir_resources r
        LEFT JOIN terminology.demographic_to_omop dg
            ON dg.source_code = LOWER(r.resource_json:gender::VARCHAR)
            AND dg.category = 'gender'
        LEFT JOIN LATERAL FLATTEN(input => r.resource_json:extension, OUTER => TRUE) ext_race
            ON ext_race.value:url::VARCHAR LIKE '%us-core-race'
        LEFT JOIN terminology.demographic_to_omop dr
            ON dr.source_code = ext_race.value:valueCoding:code::VARCHAR
            AND dr.category = 'race'
        LEFT JOIN LATERAL FLATTEN(input => r.resource_json:extension, OUTER => TRUE) ext_eth
            ON ext_eth.value:url::VARCHAR LIKE '%us-core-ethnicity'
        LEFT JOIN terminology.demographic_to_omop de
            ON de.source_code = ext_eth.value:valueCoding:code::VARCHAR
            AND de.category = 'ethnicity'
        WHERE r.resource_type = 'Patient'
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.person").collect()[0]['CNT']
    return f"Mapped {count} persons to {output_schema}.person"
$$;
GRANT USAGE ON PROCEDURE core.map_persons(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Condition Mapper — FHIR Condition → OMOP condition_occurrence
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_conditions(output_schema VARCHAR DEFAULT 'omop_cdm')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {output_schema}.condition_occurrence (
            condition_occurrence_id     INTEGER,
            person_id                   INTEGER,
            condition_concept_id        INTEGER,
            condition_start_date        DATE,
            condition_start_datetime    TIMESTAMP_NTZ,
            condition_end_date          DATE,
            condition_end_datetime      TIMESTAMP_NTZ,
            condition_type_concept_id   INTEGER DEFAULT 32817,
            condition_status_concept_id INTEGER DEFAULT 0,
            stop_reason                 VARCHAR(256),
            provider_id                 INTEGER,
            visit_occurrence_id         INTEGER,
            condition_source_value      VARCHAR(256),
            condition_source_concept_id INTEGER DEFAULT 0
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.condition_occurrence
        SELECT
            ABS(HASH(r.resource_json:id::VARCHAR)) % 2147483647          AS condition_occurrence_id,
            ABS(HASH(
                SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1)
            )) % 2147483647                                               AS person_id,
            COALESCE(sm.omop_concept_id, 0)                               AS condition_concept_id,
            COALESCE(
                r.resource_json:onsetDateTime::DATE,
                r.resource_json:onsetPeriod:start::DATE,
                r.resource_json:recordedDate::DATE
            )                                                             AS condition_start_date,
            COALESCE(
                r.resource_json:onsetDateTime::TIMESTAMP_NTZ,
                r.resource_json:onsetPeriod:start::TIMESTAMP_NTZ
            )                                                             AS condition_start_datetime,
            r.resource_json:abatementDateTime::DATE                       AS condition_end_date,
            r.resource_json:abatementDateTime::TIMESTAMP_NTZ              AS condition_end_datetime,
            32817                                                         AS condition_type_concept_id,
            0                                                             AS condition_status_concept_id,
            NULL                                                          AS stop_reason,
            NULL                                                          AS provider_id,
            NULL                                                          AS visit_occurrence_id,
            cc.value:coding[0]:code::VARCHAR                              AS condition_source_value,
            0                                                             AS condition_source_concept_id
        FROM app_state.fhir_resources r,
            LATERAL FLATTEN(input => r.resource_json:code:coding, OUTER => TRUE) cc
        LEFT JOIN terminology.snomed_to_omop sm
            ON sm.snomed_code = cc.value:code::VARCHAR
        WHERE r.resource_type = 'Condition'
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.condition_occurrence").collect()[0]['CNT']
    return f"Mapped {count} conditions to {output_schema}.condition_occurrence"
$$;
GRANT USAGE ON PROCEDURE core.map_conditions(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Measurement Mapper — FHIR Observation → OMOP measurement
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_measurements(output_schema VARCHAR DEFAULT 'omop_cdm')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {output_schema}.measurement (
            measurement_id              INTEGER,
            person_id                   INTEGER,
            measurement_concept_id      INTEGER,
            measurement_date            DATE,
            measurement_datetime        TIMESTAMP_NTZ,
            measurement_type_concept_id INTEGER DEFAULT 32817,
            operator_concept_id         INTEGER DEFAULT 0,
            value_as_number             FLOAT,
            value_as_concept_id         INTEGER DEFAULT 0,
            unit_concept_id             INTEGER DEFAULT 0,
            range_low                   FLOAT,
            range_high                  FLOAT,
            provider_id                 INTEGER,
            visit_occurrence_id         INTEGER,
            measurement_source_value    VARCHAR(256),
            measurement_source_concept_id INTEGER DEFAULT 0,
            unit_source_value           VARCHAR(50),
            value_source_value          VARCHAR(256)
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.measurement
        SELECT
            ABS(HASH(r.resource_json:id::VARCHAR)) % 2147483647          AS measurement_id,
            ABS(HASH(
                SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1)
            )) % 2147483647                                               AS person_id,
            COALESCE(lm.omop_concept_id, 0)                               AS measurement_concept_id,
            COALESCE(
                r.resource_json:effectiveDateTime::DATE,
                r.resource_json:issued::DATE
            )                                                             AS measurement_date,
            COALESCE(
                r.resource_json:effectiveDateTime::TIMESTAMP_NTZ,
                r.resource_json:issued::TIMESTAMP_NTZ
            )                                                             AS measurement_datetime,
            32817                                                         AS measurement_type_concept_id,
            0                                                             AS operator_concept_id,
            r.resource_json:valueQuantity:value::FLOAT                    AS value_as_number,
            0                                                             AS value_as_concept_id,
            0                                                             AS unit_concept_id,
            r.resource_json:referenceRange[0]:low:value::FLOAT            AS range_low,
            r.resource_json:referenceRange[0]:high:value::FLOAT           AS range_high,
            NULL                                                          AS provider_id,
            NULL                                                          AS visit_occurrence_id,
            oc.value:code::VARCHAR                                        AS measurement_source_value,
            0                                                             AS measurement_source_concept_id,
            r.resource_json:valueQuantity:unit::VARCHAR                   AS unit_source_value,
            r.resource_json:valueQuantity:value::VARCHAR                  AS value_source_value
        FROM app_state.fhir_resources r,
            LATERAL FLATTEN(input => r.resource_json:code:coding, OUTER => TRUE) oc
        LEFT JOIN terminology.loinc_to_omop lm
            ON lm.loinc_code = oc.value:code::VARCHAR
        WHERE r.resource_type = 'Observation'
            AND r.resource_json:valueQuantity IS NOT NULL
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.measurement").collect()[0]['CNT']
    return f"Mapped {count} measurements to {output_schema}.measurement"
$$;
GRANT USAGE ON PROCEDURE core.map_measurements(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Visit Mapper — FHIR Encounter → OMOP visit_occurrence
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_visits(output_schema VARCHAR DEFAULT 'omop_cdm')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {output_schema}.visit_occurrence (
            visit_occurrence_id         INTEGER,
            person_id                   INTEGER,
            visit_concept_id            INTEGER,
            visit_start_date            DATE,
            visit_start_datetime        TIMESTAMP_NTZ,
            visit_end_date              DATE,
            visit_end_datetime          TIMESTAMP_NTZ,
            visit_type_concept_id       INTEGER DEFAULT 32817,
            provider_id                 INTEGER,
            care_site_id                INTEGER,
            visit_source_value          VARCHAR(256),
            visit_source_concept_id     INTEGER DEFAULT 0,
            admitted_from_concept_id    INTEGER DEFAULT 0,
            discharged_to_concept_id    INTEGER DEFAULT 0
        )
    """).collect()

    visit_type_map = {
        'AMB': 9202, 'IMP': 9201, 'EMER': 9203, 'HH': 581476,
        'FLD': 38004193, 'VR': 5083, 'SS': 9202,
    }
    cases = " ".join([f"WHEN '{k}' THEN {v}" for k, v in visit_type_map.items()])

    session.sql(f"""
        INSERT INTO {output_schema}.visit_occurrence
        SELECT
            ABS(HASH(r.resource_json:id::VARCHAR)) % 2147483647      AS visit_occurrence_id,
            ABS(HASH(
                SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1)
            )) % 2147483647                                           AS person_id,
            CASE r.resource_json:class:code::VARCHAR
                {cases}
                ELSE 0
            END                                                       AS visit_concept_id,
            r.resource_json:period:start::DATE                        AS visit_start_date,
            r.resource_json:period:start::TIMESTAMP_NTZ               AS visit_start_datetime,
            COALESCE(
                r.resource_json:period:end::DATE,
                r.resource_json:period:start::DATE
            )                                                         AS visit_end_date,
            r.resource_json:period:end::TIMESTAMP_NTZ                 AS visit_end_datetime,
            32817                                                     AS visit_type_concept_id,
            NULL                                                      AS provider_id,
            NULL                                                      AS care_site_id,
            r.resource_json:class:code::VARCHAR                       AS visit_source_value,
            0                                                         AS visit_source_concept_id,
            0                                                         AS admitted_from_concept_id,
            0                                                         AS discharged_to_concept_id
        FROM app_state.fhir_resources r
        WHERE r.resource_type = 'Encounter'
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.visit_occurrence").collect()[0]['CNT']
    return f"Mapped {count} visits to {output_schema}.visit_occurrence"
$$;
GRANT USAGE ON PROCEDURE core.map_visits(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Drug Exposure Mapper — FHIR MedicationRequest → OMOP drug_exposure
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_drug_exposures(output_schema VARCHAR DEFAULT 'omop_cdm')
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, output_schema: str) -> str:
    session.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}").collect()

    session.sql(f"""
        CREATE TABLE IF NOT EXISTS {output_schema}.drug_exposure (
            drug_exposure_id            INTEGER,
            person_id                   INTEGER,
            drug_concept_id             INTEGER,
            drug_exposure_start_date    DATE,
            drug_exposure_start_datetime TIMESTAMP_NTZ,
            drug_exposure_end_date      DATE,
            drug_exposure_end_datetime  TIMESTAMP_NTZ,
            verbatim_end_date           DATE,
            drug_type_concept_id        INTEGER DEFAULT 32817,
            stop_reason                 VARCHAR(256),
            refills                     INTEGER,
            quantity                    FLOAT,
            days_supply                 INTEGER,
            sig                         VARCHAR(1024),
            route_concept_id            INTEGER DEFAULT 0,
            lot_number                  VARCHAR(50),
            provider_id                 INTEGER,
            visit_occurrence_id         INTEGER,
            drug_source_value           VARCHAR(256),
            drug_source_concept_id      INTEGER DEFAULT 0,
            route_source_value          VARCHAR(256),
            dose_unit_source_value      VARCHAR(256)
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.drug_exposure
        SELECT
            ABS(HASH(r.resource_json:id::VARCHAR)) % 2147483647          AS drug_exposure_id,
            ABS(HASH(
                SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1)
            )) % 2147483647                                               AS person_id,
            COALESCE(rx.omop_concept_id, 0)                               AS drug_concept_id,
            COALESCE(
                r.resource_json:authoredOn::DATE,
                r.resource_json:dispenseRequest:validityPeriod:start::DATE
            )                                                             AS drug_exposure_start_date,
            r.resource_json:authoredOn::TIMESTAMP_NTZ                     AS drug_exposure_start_datetime,
            r.resource_json:dispenseRequest:validityPeriod:end::DATE      AS drug_exposure_end_date,
            NULL                                                          AS drug_exposure_end_datetime,
            NULL                                                          AS verbatim_end_date,
            32817                                                         AS drug_type_concept_id,
            NULL                                                          AS stop_reason,
            r.resource_json:dispenseRequest:numberOfRepeatsAllowed::INTEGER AS refills,
            r.resource_json:dispenseRequest:quantity:value::FLOAT          AS quantity,
            r.resource_json:dispenseRequest:expectedSupplyDuration:value::INTEGER AS days_supply,
            r.resource_json:dosageInstruction[0]:text::VARCHAR            AS sig,
            0                                                             AS route_concept_id,
            NULL                                                          AS lot_number,
            NULL                                                          AS provider_id,
            NULL                                                          AS visit_occurrence_id,
            mc.value:code::VARCHAR                                        AS drug_source_value,
            0                                                             AS drug_source_concept_id,
            r.resource_json:dosageInstruction[0]:route:coding[0]:display::VARCHAR AS route_source_value,
            r.resource_json:dosageInstruction[0]:doseAndRate[0]:doseQuantity:unit::VARCHAR AS dose_unit_source_value
        FROM app_state.fhir_resources r,
            LATERAL FLATTEN(input => r.resource_json:medicationCodeableConcept:coding, OUTER => TRUE) mc
        LEFT JOIN terminology.rxnorm_to_omop rx
            ON rx.rxnorm_code = mc.value:code::VARCHAR
        WHERE r.resource_type = 'MedicationRequest'
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.drug_exposure").collect()[0]['CNT']
    return f"Mapped {count} drug exposures to {output_schema}.drug_exposure"
$$;
GRANT USAGE ON PROCEDURE core.map_drug_exposures(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Orchestrator — runs all mappers in sequence
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.run_full_transformation(
    source_table   VARCHAR,
    json_column    VARCHAR DEFAULT 'RAW_JSON',
    output_schema  VARCHAR DEFAULT 'omop_cdm'
)
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
import json

def run(session, source_table: str, json_column: str, output_schema: str) -> str:
    run_id = session.sql("SELECT UUID_STRING()").collect()[0][0]
    session.sql(f"""
        INSERT INTO app_state.run_history (run_id, status)
        VALUES ('{run_id}', 'RUNNING')
    """).collect()

    results = {}
    errors = []
    try:
        r = session.call('core.parse_fhir_bundles', source_table, json_column)
        results['parse'] = r

        for mapper, key in [
            ('core.map_persons', 'persons'),
            ('core.map_conditions', 'conditions'),
            ('core.map_measurements', 'measurements'),
            ('core.map_visits', 'visits'),
            ('core.map_drug_exposures', 'drugs'),
        ]:
            try:
                r = session.call(mapper, output_schema)
                results[key] = r
            except Exception as e:
                errors.append(f"{mapper}: {str(e)}")

        counts = {}
        for tbl, col in [
            ('person', 'persons_mapped'),
            ('condition_occurrence', 'conditions_mapped'),
            ('measurement', 'measurements_mapped'),
            ('visit_occurrence', 'visits_mapped'),
        ]:
            try:
                cnt = session.sql(f"SELECT COUNT(*) AS c FROM {output_schema}.{tbl}").collect()[0]['C']
                counts[col] = cnt
            except:
                counts[col] = 0

        bundles = session.sql(f"SELECT COUNT(*) AS c FROM {source_table}").collect()[0]['C']

        session.sql(f"""
            UPDATE app_state.run_history SET
                status = '{'COMPLETED_WITH_ERRORS' if errors else 'COMPLETED'}',
                completed_at = CURRENT_TIMESTAMP(),
                fhir_bundles = {bundles},
                persons_mapped = {counts.get('persons_mapped', 0)},
                conditions_mapped = {counts.get('conditions_mapped', 0)},
                measurements_mapped = {counts.get('measurements_mapped', 0)},
                visits_mapped = {counts.get('visits_mapped', 0)},
                errors = {len(errors)},
                error_detail = '{json.dumps(errors).replace("'", "''")}'
            WHERE run_id = '{run_id}'
        """).collect()

    except Exception as e:
        session.sql(f"""
            UPDATE app_state.run_history SET
                status = 'FAILED',
                completed_at = CURRENT_TIMESTAMP(),
                errors = 1,
                error_detail = '{str(e).replace("'", "''")}'
            WHERE run_id = '{run_id}'
        """).collect()
        return f"FAILED: {str(e)}"

    summary = f"Run {run_id}: {', '.join(f'{k}={v}' for k,v in counts.items())}"
    if errors:
        summary += f" | {len(errors)} errors: {'; '.join(errors)}"
    return summary
$$;
GRANT USAGE ON PROCEDURE core.run_full_transformation(VARCHAR, VARCHAR, VARCHAR)
    TO APPLICATION ROLE app_admin;
