-- =============================================================================
-- FHIR-to-OMOP CDM — Transformation Stored Procedures (custom-built)
--
-- FHIR parsing: uses Snowflake VARIANT + LATERAL FLATTEN (inspired by
--   Tuva Health's FHIR Inferno patterns, Apache 2.0)
-- OMOP mapping: original logic mapping FHIR R4 resources to OMOP CDM v5.4
--   tables via vocabulary crosswalk lookups
-- Multi-cloud: pure Snowflake SQL + Python, no cloud-specific features
--
-- NOTE: All mappers use the CTE pattern for LATERAL FLATTEN + LEFT JOIN
--   because Snowflake does not support LEFT JOIN LATERAL FLATTEN with ON clause.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- FHIR Bundle Parser — extracts resources from FHIR R4 JSON bundles
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.parse_fhir_bundles(
    source_table VARCHAR,
    json_column  VARCHAR DEFAULT 'BUNDLE_DATA',
    bundle_id_column VARCHAR DEFAULT 'BUNDLE_ID'
)
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
def run(session, source_table: str, json_column: str, bundle_id_column: str) -> str:
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

    session.sql("TRUNCATE TABLE IF EXISTS app_state.fhir_resources").collect()

    extract_sql = f"""
        INSERT INTO app_state.fhir_resources (resource_id, resource_type, bundle_id, resource_json)
        SELECT
            r.value:resource:id::VARCHAR           AS resource_id,
            r.value:resource:resourceType::VARCHAR  AS resource_type,
            src.{bundle_id_column}                  AS bundle_id,
            r.value:resource                        AS resource_json
        FROM {source_table} src,
            LATERAL FLATTEN(input => src.{json_column}:entry) r
        WHERE r.value:resource:resourceType IS NOT NULL
    """
    session.sql(extract_sql).collect()
    count = session.sql("SELECT COUNT(*) AS cnt FROM app_state.fhir_resources").collect()[0]['CNT']
    return f"Parsed {count} FHIR resources from {source_table}"
$$;
GRANT USAGE ON PROCEDURE core.parse_fhir_bundles(VARCHAR, VARCHAR, VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Person Mapper — FHIR Patient → OMOP person (CTE pattern)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_persons(output_schema VARCHAR DEFAULT 'omop_staging')
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
        CREATE OR REPLACE TABLE {output_schema}.person (
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
        WITH patients AS (
            SELECT
                resource_json:id::VARCHAR AS patient_id,
                resource_json:gender::VARCHAR AS gender,
                resource_json:birthDate::DATE AS birth_date,
                resource_json:birthDate::TIMESTAMP_NTZ AS birth_datetime,
                resource_json AS rj
            FROM app_state.fhir_resources
            WHERE resource_type = 'Patient'
        ),
        race_ext AS (
            SELECT p.patient_id,
                   e.value:valueCoding:code::VARCHAR AS race_code,
                   e.value:valueCoding:display::VARCHAR AS race_display
            FROM patients p,
                LATERAL FLATTEN(input => p.rj:extension, OUTER => TRUE) e
            WHERE e.value:url::VARCHAR LIKE '%us-core-race'
        ),
        eth_ext AS (
            SELECT p.patient_id,
                   e.value:valueCoding:code::VARCHAR AS eth_code,
                   e.value:valueCoding:display::VARCHAR AS eth_display
            FROM patients p,
                LATERAL FLATTEN(input => p.rj:extension, OUTER => TRUE) e
            WHERE e.value:url::VARCHAR LIKE '%us-core-ethnicity'
        )
        SELECT
            ABS(HASH(p.patient_id)) % 2147483647    AS person_id,
            COALESCE(dg.omop_concept_id, 0)          AS gender_concept_id,
            YEAR(p.birth_date)                        AS year_of_birth,
            MONTH(p.birth_date)                       AS month_of_birth,
            DAY(p.birth_date)                         AS day_of_birth,
            p.birth_datetime,
            COALESCE(dr.omop_concept_id, 0)           AS race_concept_id,
            COALESCE(de.omop_concept_id, 0)           AS ethnicity_concept_id,
            NULL AS location_id,
            NULL AS provider_id,
            NULL AS care_site_id,
            p.patient_id                              AS person_source_value,
            p.gender                                  AS gender_source_value,
            0 AS gender_source_concept_id,
            r.race_display                            AS race_source_value,
            0 AS race_source_concept_id,
            e.eth_display                             AS ethnicity_source_value,
            0 AS ethnicity_source_concept_id
        FROM patients p
        LEFT JOIN terminology.demographic_to_omop dg
            ON dg.source_code = LOWER(p.gender) AND dg.category = 'gender'
        LEFT JOIN race_ext r ON r.patient_id = p.patient_id
        LEFT JOIN terminology.demographic_to_omop dr
            ON dr.source_code = r.race_code AND dr.category = 'race'
        LEFT JOIN eth_ext e ON e.patient_id = p.patient_id
        LEFT JOIN terminology.demographic_to_omop de
            ON de.source_code = e.eth_code AND de.category = 'ethnicity'
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.person").collect()[0]['CNT']
    return f"Mapped {count} persons to {output_schema}.person"
$$;
GRANT USAGE ON PROCEDURE core.map_persons(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Condition Mapper — FHIR Condition → OMOP condition_occurrence (CTE pattern)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_conditions(output_schema VARCHAR DEFAULT 'omop_staging')
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
        CREATE OR REPLACE TABLE {output_schema}.condition_occurrence (
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
        WITH cond_flat AS (
            SELECT
                r.resource_json:id::VARCHAR AS cond_id,
                SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1) AS patient_ref,
                COALESCE(r.resource_json:onsetDateTime::DATE, r.resource_json:onsetPeriod:start::DATE, r.resource_json:recordedDate::DATE) AS start_date,
                COALESCE(r.resource_json:onsetDateTime::TIMESTAMP_NTZ, r.resource_json:onsetPeriod:start::TIMESTAMP_NTZ) AS start_dt,
                r.resource_json:abatementDateTime::DATE AS end_date,
                r.resource_json:abatementDateTime::TIMESTAMP_NTZ AS end_dt,
                cc.value:code::VARCHAR AS source_code
            FROM app_state.fhir_resources r,
                LATERAL FLATTEN(input => r.resource_json:code:coding, OUTER => TRUE) cc
            WHERE r.resource_type = 'Condition'
        )
        SELECT
            ABS(HASH(c.cond_id)) % 2147483647 AS condition_occurrence_id,
            ABS(HASH(c.patient_ref)) % 2147483647 AS person_id,
            COALESCE(sm.omop_concept_id, 0) AS condition_concept_id,
            c.start_date AS condition_start_date,
            c.start_dt AS condition_start_datetime,
            c.end_date AS condition_end_date,
            c.end_dt AS condition_end_datetime,
            32817 AS condition_type_concept_id,
            0 AS condition_status_concept_id,
            NULL AS stop_reason,
            NULL AS provider_id,
            NULL AS visit_occurrence_id,
            c.source_code AS condition_source_value,
            0 AS condition_source_concept_id
        FROM cond_flat c
        LEFT JOIN terminology.snomed_to_omop sm
            ON sm.snomed_code = c.source_code
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.condition_occurrence").collect()[0]['CNT']
    return f"Mapped {count} conditions to {output_schema}.condition_occurrence"
$$;
GRANT USAGE ON PROCEDURE core.map_conditions(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Measurement Mapper — FHIR Observation (numeric) → OMOP measurement (CTE pattern)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_measurements(output_schema VARCHAR DEFAULT 'omop_staging')
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
        CREATE OR REPLACE TABLE {output_schema}.measurement (
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
        WITH obs_flat AS (
            SELECT
                r.resource_json:id::VARCHAR AS obs_id,
                SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1) AS patient_ref,
                COALESCE(r.resource_json:effectiveDateTime::DATE, r.resource_json:issued::DATE) AS meas_date,
                COALESCE(r.resource_json:effectiveDateTime::TIMESTAMP_NTZ, r.resource_json:issued::TIMESTAMP_NTZ) AS meas_dt,
                r.resource_json:valueQuantity:value::FLOAT AS val_num,
                r.resource_json:referenceRange[0]:low:value::FLOAT AS range_low,
                r.resource_json:referenceRange[0]:high:value::FLOAT AS range_high,
                oc.value:code::VARCHAR AS source_code,
                r.resource_json:valueQuantity:unit::VARCHAR AS unit_src,
                r.resource_json:valueQuantity:value::VARCHAR AS val_src
            FROM app_state.fhir_resources r,
                LATERAL FLATTEN(input => r.resource_json:code:coding, OUTER => TRUE) oc
            WHERE r.resource_type = 'Observation'
                AND r.resource_json:valueQuantity IS NOT NULL
        )
        SELECT
            ABS(HASH(o.obs_id)) % 2147483647 AS measurement_id,
            ABS(HASH(o.patient_ref)) % 2147483647 AS person_id,
            COALESCE(lm.omop_concept_id, 0) AS measurement_concept_id,
            o.meas_date AS measurement_date,
            o.meas_dt AS measurement_datetime,
            32817 AS measurement_type_concept_id,
            0 AS operator_concept_id,
            o.val_num AS value_as_number,
            0 AS value_as_concept_id,
            0 AS unit_concept_id,
            o.range_low, o.range_high,
            NULL AS provider_id,
            NULL AS visit_occurrence_id,
            o.source_code AS measurement_source_value,
            0 AS measurement_source_concept_id,
            o.unit_src AS unit_source_value,
            o.val_src AS value_source_value
        FROM obs_flat o
        LEFT JOIN terminology.loinc_to_omop lm
            ON lm.loinc_code = o.source_code
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.measurement").collect()[0]['CNT']
    return f"Mapped {count} measurements to {output_schema}.measurement"
$$;
GRANT USAGE ON PROCEDURE core.map_measurements(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Visit Mapper — FHIR Encounter → OMOP visit_occurrence
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_visits(output_schema VARCHAR DEFAULT 'omop_staging')
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
        CREATE OR REPLACE TABLE {output_schema}.visit_occurrence (
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
            ABS(HASH(r.resource_json:id::VARCHAR)) % 2147483647 AS visit_occurrence_id,
            ABS(HASH(SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1))) % 2147483647 AS person_id,
            CASE r.resource_json:class:code::VARCHAR {cases} ELSE 0 END AS visit_concept_id,
            r.resource_json:period:start::DATE AS visit_start_date,
            r.resource_json:period:start::TIMESTAMP_NTZ AS visit_start_datetime,
            COALESCE(r.resource_json:period:end::DATE, r.resource_json:period:start::DATE) AS visit_end_date,
            r.resource_json:period:end::TIMESTAMP_NTZ AS visit_end_datetime,
            32817 AS visit_type_concept_id,
            NULL AS provider_id,
            NULL AS care_site_id,
            r.resource_json:class:code::VARCHAR AS visit_source_value,
            0 AS visit_source_concept_id,
            0 AS admitted_from_concept_id,
            0 AS discharged_to_concept_id
        FROM app_state.fhir_resources r
        WHERE r.resource_type = 'Encounter'
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.visit_occurrence").collect()[0]['CNT']
    return f"Mapped {count} visits to {output_schema}.visit_occurrence"
$$;
GRANT USAGE ON PROCEDURE core.map_visits(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Drug Exposure Mapper — FHIR MedicationRequest → OMOP drug_exposure (CTE pattern)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_drug_exposures(output_schema VARCHAR DEFAULT 'omop_staging')
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
        CREATE OR REPLACE TABLE {output_schema}.drug_exposure (
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
        WITH med_flat AS (
            SELECT
                r.resource_json:id::VARCHAR AS med_id,
                SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1) AS patient_ref,
                COALESCE(r.resource_json:authoredOn::DATE, r.resource_json:dispenseRequest:validityPeriod:start::DATE) AS start_date,
                r.resource_json:authoredOn::TIMESTAMP_NTZ AS start_dt,
                r.resource_json:dispenseRequest:validityPeriod:end::DATE AS end_date,
                r.resource_json:dispenseRequest:numberOfRepeatsAllowed::INTEGER AS refills,
                r.resource_json:dispenseRequest:quantity:value::FLOAT AS quantity,
                r.resource_json:dispenseRequest:expectedSupplyDuration:value::INTEGER AS days_supply,
                r.resource_json:dosageInstruction[0]:text::VARCHAR AS sig,
                mc.value:code::VARCHAR AS source_code,
                r.resource_json:dosageInstruction[0]:route:coding[0]:display::VARCHAR AS route_src,
                r.resource_json:dosageInstruction[0]:doseAndRate[0]:doseQuantity:unit::VARCHAR AS dose_unit_src
            FROM app_state.fhir_resources r,
                LATERAL FLATTEN(input => r.resource_json:medicationCodeableConcept:coding, OUTER => TRUE) mc
            WHERE r.resource_type = 'MedicationRequest'
        )
        SELECT
            ABS(HASH(m.med_id)) % 2147483647 AS drug_exposure_id,
            ABS(HASH(m.patient_ref)) % 2147483647 AS person_id,
            COALESCE(rx.omop_concept_id, 0) AS drug_concept_id,
            m.start_date AS drug_exposure_start_date,
            m.start_dt AS drug_exposure_start_datetime,
            m.end_date AS drug_exposure_end_date,
            NULL::TIMESTAMP_NTZ AS drug_exposure_end_datetime,
            NULL::DATE AS verbatim_end_date,
            32817 AS drug_type_concept_id,
            NULL::VARCHAR(256) AS stop_reason,
            m.refills, m.quantity, m.days_supply, m.sig,
            0 AS route_concept_id,
            NULL::VARCHAR(50) AS lot_number,
            NULL AS provider_id,
            NULL AS visit_occurrence_id,
            m.source_code AS drug_source_value,
            0 AS drug_source_concept_id,
            m.route_src AS route_source_value,
            m.dose_unit_src AS dose_unit_source_value
        FROM med_flat m
        LEFT JOIN terminology.rxnorm_to_omop rx
            ON rx.rxnorm_code = m.source_code
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.drug_exposure").collect()[0]['CNT']
    return f"Mapped {count} drug exposures to {output_schema}.drug_exposure"
$$;
GRANT USAGE ON PROCEDURE core.map_drug_exposures(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Procedure Mapper — FHIR Procedure → OMOP procedure_occurrence (CTE pattern)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_procedures(output_schema VARCHAR DEFAULT 'omop_staging')
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
        CREATE OR REPLACE TABLE {output_schema}.procedure_occurrence (
            procedure_occurrence_id      INTEGER,
            person_id                    INTEGER,
            procedure_concept_id         INTEGER,
            procedure_date               DATE,
            procedure_datetime           TIMESTAMP_NTZ,
            procedure_end_date           DATE,
            procedure_type_concept_id    INTEGER DEFAULT 32817,
            provider_id                  INTEGER,
            visit_occurrence_id          INTEGER,
            procedure_source_value       VARCHAR(256),
            procedure_source_concept_id  INTEGER DEFAULT 0
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.procedure_occurrence
        WITH proc_flat AS (
            SELECT
                r.resource_json:id::VARCHAR AS proc_id,
                SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1) AS patient_ref,
                COALESCE(r.resource_json:performedDateTime::DATE, r.resource_json:performedPeriod:start::DATE) AS proc_date,
                COALESCE(r.resource_json:performedDateTime::TIMESTAMP_NTZ, r.resource_json:performedPeriod:start::TIMESTAMP_NTZ) AS proc_dt,
                r.resource_json:performedPeriod:end::DATE AS proc_end_date,
                pc.value:code::VARCHAR AS source_code,
                pc.value:system::VARCHAR AS code_system
            FROM app_state.fhir_resources r,
                LATERAL FLATTEN(input => r.resource_json:code:coding, OUTER => TRUE) pc
            WHERE r.resource_type = 'Procedure'
        )
        SELECT
            ABS(HASH(pf.proc_id)) % 2147483647 AS procedure_occurrence_id,
            ABS(HASH(pf.patient_ref)) % 2147483647 AS person_id,
            COALESCE(sm.omop_concept_id, cpt.omop_concept_id, hc.omop_concept_id, 0) AS procedure_concept_id,
            pf.proc_date AS procedure_date,
            pf.proc_dt AS procedure_datetime,
            pf.proc_end_date AS procedure_end_date,
            32817 AS procedure_type_concept_id,
            NULL AS provider_id,
            NULL AS visit_occurrence_id,
            pf.source_code AS procedure_source_value,
            0 AS procedure_source_concept_id
        FROM proc_flat pf
        LEFT JOIN terminology.snomed_to_omop sm
            ON sm.snomed_code = pf.source_code AND pf.code_system LIKE '%snomed%'
        LEFT JOIN terminology.cpt_to_omop cpt
            ON cpt.cpt_code = pf.source_code AND pf.code_system LIKE '%cpt%'
        LEFT JOIN terminology.hcpcs_to_omop hc
            ON hc.hcpcs_code = pf.source_code AND pf.code_system LIKE '%hcpcs%'
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.procedure_occurrence").collect()[0]['CNT']
    return f"Mapped {count} procedures to {output_schema}.procedure_occurrence"
$$;
GRANT USAGE ON PROCEDURE core.map_procedures(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Observation (Qualitative) Mapper — FHIR Observation → OMOP observation (CTE pattern)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_observations_qual(output_schema VARCHAR DEFAULT 'omop_staging')
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
        CREATE OR REPLACE TABLE {output_schema}.observation (
            observation_id                INTEGER,
            person_id                     INTEGER,
            observation_concept_id        INTEGER,
            observation_date              DATE,
            observation_datetime          TIMESTAMP_NTZ,
            observation_type_concept_id   INTEGER DEFAULT 32817,
            value_as_string               VARCHAR(1024),
            value_as_concept_id           INTEGER DEFAULT 0,
            observation_source_value      VARCHAR(256),
            observation_source_concept_id INTEGER DEFAULT 0
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.observation
        WITH obs_qual AS (
            SELECT
                r.resource_json:id::VARCHAR AS obs_id,
                SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1) AS patient_ref,
                COALESCE(r.resource_json:effectiveDateTime::DATE, r.resource_json:issued::DATE) AS obs_date,
                COALESCE(r.resource_json:effectiveDateTime::TIMESTAMP_NTZ, r.resource_json:issued::TIMESTAMP_NTZ) AS obs_dt,
                COALESCE(r.resource_json:valueCodeableConcept:text::VARCHAR, r.resource_json:valueCodeableConcept:coding[0]:display::VARCHAR, r.resource_json:valueString::VARCHAR) AS val_str,
                oc.value:code::VARCHAR AS source_code
            FROM app_state.fhir_resources r,
                LATERAL FLATTEN(input => r.resource_json:code:coding, OUTER => TRUE) oc
            WHERE r.resource_type = 'Observation'
                AND r.resource_json:valueQuantity IS NULL
                AND (r.resource_json:valueCodeableConcept IS NOT NULL OR r.resource_json:valueString IS NOT NULL)
        )
        SELECT
            ABS(HASH(q.obs_id)) % 2147483647 AS observation_id,
            ABS(HASH(q.patient_ref)) % 2147483647 AS person_id,
            COALESCE(lm.omop_concept_id, 0) AS observation_concept_id,
            q.obs_date AS observation_date,
            q.obs_dt AS observation_datetime,
            32817 AS observation_type_concept_id,
            q.val_str AS value_as_string,
            0 AS value_as_concept_id,
            q.source_code AS observation_source_value,
            0 AS observation_source_concept_id
        FROM obs_qual q
        LEFT JOIN terminology.loinc_to_omop lm
            ON lm.loinc_code = q.source_code
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.observation").collect()[0]['CNT']
    return f"Mapped {count} qualitative observations to {output_schema}.observation"
$$;
GRANT USAGE ON PROCEDURE core.map_observations_qual(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Death Mapper — FHIR Patient (deceasedDateTime/deceasedBoolean) → OMOP death
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.map_death(output_schema VARCHAR DEFAULT 'omop_staging')
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
        CREATE OR REPLACE TABLE {output_schema}.death (
            person_id                INTEGER,
            death_date               DATE,
            death_datetime           TIMESTAMP_NTZ,
            death_type_concept_id    INTEGER DEFAULT 32817,
            cause_concept_id         INTEGER DEFAULT 0,
            cause_source_value       VARCHAR(256)
        )
    """).collect()

    session.sql(f"""
        INSERT INTO {output_schema}.death
        SELECT
            ABS(HASH(r.resource_json:id::VARCHAR)) % 2147483647 AS person_id,
            r.resource_json:deceasedDateTime::DATE AS death_date,
            r.resource_json:deceasedDateTime::TIMESTAMP_NTZ AS death_datetime,
            32817 AS death_type_concept_id,
            0 AS cause_concept_id,
            NULL AS cause_source_value
        FROM app_state.fhir_resources r
        WHERE r.resource_type = 'Patient'
            AND (r.resource_json:deceasedDateTime IS NOT NULL
                 OR r.resource_json:deceasedBoolean::BOOLEAN = TRUE)
    """).collect()

    count = session.sql(f"SELECT COUNT(*) AS cnt FROM {output_schema}.death").collect()[0]['CNT']
    return f"Mapped {count} death records to {output_schema}.death"
$$;
GRANT USAGE ON PROCEDURE core.map_death(VARCHAR)
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- FHIR Quality Validator — read-only diagnostic returning JSON summary
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.validate_fhir_quality()
    RETURNS VARCHAR
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'run'
    EXECUTE AS OWNER
AS
$$
import json

def run(session) -> str:
    summary = {}

    total_bundles = session.sql(
        "SELECT COUNT(DISTINCT bundle_id) AS cnt FROM app_state.fhir_resources"
    ).collect()[0]['CNT']
    summary['total_bundles'] = total_bundles

    type_rows = session.sql(
        "SELECT resource_type, COUNT(*) AS cnt FROM app_state.fhir_resources GROUP BY resource_type ORDER BY cnt DESC"
    ).collect()
    summary['resources_by_type'] = {row['RESOURCE_TYPE']: row['CNT'] for row in type_rows}

    issues = []

    checks = [
        ("Patient missing birthDate", "resource_type = 'Patient' AND resource_json:birthDate IS NULL"),
        ("Condition missing code", "resource_type = 'Condition' AND resource_json:code IS NULL"),
        ("Observation missing value", "resource_type = 'Observation' AND resource_json:valueQuantity IS NULL AND resource_json:valueCodeableConcept IS NULL AND resource_json:valueString IS NULL AND resource_json:component IS NULL"),
        ("Encounter missing period.start", "resource_type = 'Encounter' AND resource_json:period:start IS NULL"),
        ("MedicationRequest missing medication", "resource_type = 'MedicationRequest' AND resource_json:medicationCodeableConcept IS NULL AND resource_json:medicationReference IS NULL"),
        ("Procedure missing code", "resource_type = 'Procedure' AND resource_json:code IS NULL"),
        ("Patient missing id", "resource_type = 'Patient' AND resource_json:id IS NULL"),
    ]

    for rule, where in checks:
        cnt = session.sql(f"SELECT COUNT(*) AS cnt FROM app_state.fhir_resources WHERE {where}").collect()[0]['CNT']
        if cnt > 0:
            issues.append({'rule': rule, 'count': cnt})

    summary['quality_issues'] = issues
    summary['total_issues'] = sum(i['count'] for i in issues)

    return json.dumps(summary, indent=2)
$$;
GRANT USAGE ON PROCEDURE core.validate_fhir_quality()
    TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- Orchestrator — runs all mappers in sequence
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE core.run_full_transformation(
    source_table   VARCHAR,
    json_column    VARCHAR DEFAULT 'BUNDLE_DATA',
    output_schema  VARCHAR DEFAULT 'omop_staging'
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
        bundle_id_col = 'BUNDLE_ID'
        try:
            cols = [r['COLUMN_NAME'] for r in session.sql(f"SHOW COLUMNS IN TABLE {source_table}").collect()]
            if 'BUNDLE_ID' not in [c.upper() for c in cols]:
                bundle_id_col = json_column
        except:
            pass

        r = session.call('core.parse_fhir_bundles', source_table, json_column, bundle_id_col)
        results['parse'] = r

        for mapper, key in [
            ('core.map_persons', 'persons'),
            ('core.map_conditions', 'conditions'),
            ('core.map_measurements', 'measurements'),
            ('core.map_visits', 'visits'),
            ('core.map_drug_exposures', 'drugs'),
            ('core.map_procedures', 'procedures'),
            ('core.map_observations_qual', 'observations_qual'),
            ('core.map_death', 'death'),
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
            ('drug_exposure', 'drugs_mapped'),
            ('procedure_occurrence', 'procedures_mapped'),
            ('observation', 'observations_mapped'),
            ('death', 'death_mapped'),
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
