"""
Graduated FHIR-to-OMOP/Tuva transformation test.

Usage:
    python tests/test_transformation.py                       # Level 1: 1 bundle, OMOP output
    python tests/test_transformation.py --level 2             # Level 2: 10 bundles, OMOP
    python tests/test_transformation.py --level 3             # Level 3: full 1,000 bundles vs ground truth
    python tests/test_transformation.py --format tuva         # Level 1 with Tuva output
    python tests/test_transformation.py --level 2 --keep      # Keep test schema after run
"""

import argparse
import os
import sys
import time

import snowflake.connector

SOURCE_DB = "TRE_HEALTHCARE_DB"
SOURCE_SCHEMA = "FHIR_STAGING"
SOURCE_TABLE = f"{SOURCE_DB}.{SOURCE_SCHEMA}.RAW_BUNDLES"
TEST_SCHEMA = "TUVA_TEST_OMOP"
TEST_DB = SOURCE_DB

GROUND_TRUTH = {
    "person": (f"{SOURCE_DB}.{SOURCE_SCHEMA}.OMOP_GT_PERSON", 2000),
    "condition_occurrence": (f"{SOURCE_DB}.{SOURCE_SCHEMA}.OMOP_GT_CONDITION", 2851),
    "measurement": (f"{SOURCE_DB}.{SOURCE_SCHEMA}.OMOP_GT_MEASUREMENT", 10000),
    "visit_occurrence": (f"{SOURCE_DB}.{SOURCE_SCHEMA}.OMOP_GT_VISIT", 2000),
    "death": (f"{SOURCE_DB}.{SOURCE_SCHEMA}.OMOP_GT_DEATH", 9),
}

OMOP_TABLES = [
    "person", "condition_occurrence", "measurement", "visit_occurrence",
    "drug_exposure", "procedure_occurrence", "observation", "death",
    "location", "care_site", "provider", "observation_period", "cdm_source",
]

TUVA_TABLES = [
    "patient", "encounter", "condition", "lab_result", "observation",
    "medication", "immunization", "procedure", "location", "practitioner",
    "medical_claim", "eligibility", "appointment",
]

FHIR_RESOURCE_TYPES = [
    "Patient", "Condition", "Observation", "Encounter",
    "MedicationRequest", "Procedure", "Immunization",
    "AllergyIntolerance", "DiagnosticReport", "CarePlan",
    "CareTeam", "Claim", "Organization", "Practitioner", "Location",
]


def connect():
    return snowflake.connector.connect(
        connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME") or "HealthcareDemos"
    )


def sql(cur, stmt):
    return cur.execute(stmt).fetchall()


def count(cur, table):
    rows = sql(cur, f"SELECT COUNT(*) FROM {table}")
    return rows[0][0]


def elapsed(start):
    return f"{time.time() - start:.1f}s"


def pf(ok, msg, detail=""):
    tag = "PASS" if ok else "FAIL"
    print(f"  [{tag}] {msg}" + (f"  ({detail})" if detail else ""))
    return ok


class TestRunner:
    def __init__(self, level, keep, output_format="omop"):
        self.level = level
        self.keep = keep
        self.output_format = output_format
        self.conn = connect()
        self.cur = self.conn.cursor()
        self.passed = 0
        self.failed = 0
        self.fq_schema = f"{TEST_DB}.{TEST_SCHEMA}"
        self.test_source = f"{self.fq_schema}.TEST_RAW_BUNDLES"
        self.fhir_resources = f"{self.fq_schema}.FHIR_RESOURCES"

    def check(self, ok, msg, detail=""):
        result = pf(ok, msg, detail)
        if result:
            self.passed += 1
        else:
            self.failed += 1
        return result

    def setup(self):
        t = time.time()
        sql(self.cur, f"USE WAREHOUSE SI_DEMO_WH")
        sql(self.cur, f"CREATE DATABASE IF NOT EXISTS {TEST_DB}")
        sql(self.cur, f"CREATE SCHEMA IF NOT EXISTS {self.fq_schema}")

        if self.level == 3:
            limit_clause = ""
            expected_label = "all 1,000"
        elif self.level == 2:
            limit_clause = "LIMIT 10"
            expected_label = "10"
        else:
            limit_clause = "LIMIT 1"
            expected_label = "1"

        sql(self.cur, f"""
            CREATE OR REPLACE TABLE {self.test_source} AS
            SELECT * FROM {SOURCE_TABLE} {limit_clause}
        """)
        n = count(self.cur, self.test_source)
        print(f"  Setup: created test table with {n} bundles ({expected_label} requested) [{elapsed(t)}]")
        return n

    def create_terminology_tables(self):
        t = time.time()
        s = self.fq_schema

        for src_tbl in ["LOINC_TO_OMOP", "SNOMED_TO_OMOP", "RXNORM_TO_OMOP",
                         "DEMOGRAPHIC_TO_OMOP", "CPT_TO_OMOP", "HCPCS_TO_OMOP"]:
            if self._table_exists(f"{TEST_DB}.{SOURCE_SCHEMA}.{src_tbl}"):
                sql(self.cur, f"CREATE TABLE IF NOT EXISTS {s}.{src_tbl} AS SELECT * FROM {TEST_DB}.{SOURCE_SCHEMA}.{src_tbl}")

        if not self._table_exists(f"{s}.LOINC_TO_OMOP"):
            self._seed_terminology_inline()

        loaded = []
        for tbl in ["LOINC_TO_OMOP", "SNOMED_TO_OMOP", "RXNORM_TO_OMOP",
                     "DEMOGRAPHIC_TO_OMOP", "CPT_TO_OMOP", "HCPCS_TO_OMOP"]:
            if self._table_exists(f"{s}.{tbl}"):
                loaded.append(tbl)
        print(f"  Terminology tables ready ({len(loaded)} tables) [{elapsed(t)}]")

    def _seed_terminology_inline(self):
        s = self.fq_schema
        sql(self.cur, f"""CREATE TABLE IF NOT EXISTS {s}.LOINC_TO_OMOP (
            loinc_code VARCHAR(20) NOT NULL, loinc_description VARCHAR(512),
            omop_concept_id INTEGER NOT NULL, omop_concept_name VARCHAR(512),
            omop_domain_id VARCHAR(50), omop_vocabulary_id VARCHAR(50) DEFAULT 'LOINC',
            standard_concept VARCHAR(1) DEFAULT 'S', PRIMARY KEY (loinc_code))""")
        sql(self.cur, f"""CREATE TABLE IF NOT EXISTS {s}.SNOMED_TO_OMOP (
            snomed_code VARCHAR(20) NOT NULL, snomed_description VARCHAR(512),
            omop_concept_id INTEGER NOT NULL, omop_concept_name VARCHAR(512),
            omop_domain_id VARCHAR(50), omop_vocabulary_id VARCHAR(50) DEFAULT 'SNOMED',
            standard_concept VARCHAR(1) DEFAULT 'S', PRIMARY KEY (snomed_code))""")
        sql(self.cur, f"""CREATE TABLE IF NOT EXISTS {s}.RXNORM_TO_OMOP (
            rxnorm_code VARCHAR(20) NOT NULL, rxnorm_description VARCHAR(512),
            omop_concept_id INTEGER NOT NULL, omop_concept_name VARCHAR(512),
            omop_domain_id VARCHAR(50), omop_vocabulary_id VARCHAR(50) DEFAULT 'RxNorm',
            standard_concept VARCHAR(1) DEFAULT 'S', PRIMARY KEY (rxnorm_code))""")
        sql(self.cur, f"""CREATE TABLE IF NOT EXISTS {s}.DEMOGRAPHIC_TO_OMOP (
            source_system VARCHAR(50) NOT NULL, source_code VARCHAR(50) NOT NULL,
            source_description VARCHAR(256), omop_concept_id INTEGER NOT NULL,
            omop_concept_name VARCHAR(256), omop_domain_id VARCHAR(50),
            category VARCHAR(50) NOT NULL, PRIMARY KEY (source_system, source_code, category))""")
        sql(self.cur, f"""CREATE TABLE IF NOT EXISTS {s}.CPT_TO_OMOP (
            cpt_code VARCHAR(20) NOT NULL, cpt_description VARCHAR(512),
            omop_concept_id INTEGER NOT NULL, omop_concept_name VARCHAR(512),
            omop_domain_id VARCHAR(50) DEFAULT 'Procedure', omop_vocabulary_id VARCHAR(50) DEFAULT 'CPT4',
            standard_concept VARCHAR(1), PRIMARY KEY (cpt_code))""")
        sql(self.cur, f"""CREATE TABLE IF NOT EXISTS {s}.HCPCS_TO_OMOP (
            hcpcs_code VARCHAR(20) NOT NULL, hcpcs_description VARCHAR(512),
            omop_concept_id INTEGER NOT NULL, omop_concept_name VARCHAR(512),
            omop_domain_id VARCHAR(50), omop_vocabulary_id VARCHAR(50) DEFAULT 'HCPCS',
            standard_concept VARCHAR(1), PRIMARY KEY (hcpcs_code))""")
        sql(self.cur, f"""CREATE TABLE IF NOT EXISTS {s}.ICD10CM_TO_OMOP (
            icd10cm_code VARCHAR(20) NOT NULL, icd10cm_description VARCHAR(512),
            omop_concept_id INTEGER NOT NULL, omop_concept_name VARCHAR(512),
            omop_domain_id VARCHAR(50) DEFAULT 'Condition', omop_vocabulary_id VARCHAR(50) DEFAULT 'ICD10CM',
            standard_concept VARCHAR(1), PRIMARY KEY (icd10cm_code))""")
        sql(self.cur, f"""CREATE TABLE IF NOT EXISTS {s}.ICD10PCS_TO_OMOP (
            icd10pcs_code VARCHAR(20) NOT NULL, icd10pcs_description VARCHAR(512),
            omop_concept_id INTEGER NOT NULL, omop_concept_name VARCHAR(512),
            omop_domain_id VARCHAR(50) DEFAULT 'Procedure', omop_vocabulary_id VARCHAR(50) DEFAULT 'ICD10PCS',
            standard_concept VARCHAR(1), PRIMARY KEY (icd10pcs_code))""")
        sql(self.cur, f"""CREATE TABLE IF NOT EXISTS {s}.CONCEPT (
            concept_id INTEGER NOT NULL, concept_name VARCHAR(512) NOT NULL,
            domain_id VARCHAR(50) NOT NULL, vocabulary_id VARCHAR(50) NOT NULL,
            concept_class_id VARCHAR(50), standard_concept VARCHAR(1),
            concept_code VARCHAR(50), valid_start_date DATE,
            valid_end_date DATE, invalid_reason VARCHAR(1),
            PRIMARY KEY (concept_id))""")
        sql(self.cur, f"""CREATE TABLE IF NOT EXISTS {s}.CONCEPT_RELATIONSHIP (
            concept_id_1 INTEGER NOT NULL, concept_id_2 INTEGER NOT NULL,
            relationship_id VARCHAR(50) NOT NULL, valid_start_date DATE,
            valid_end_date DATE, invalid_reason VARCHAR(1),
            PRIMARY KEY (concept_id_1, concept_id_2, relationship_id))""")

        seed_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "scripts", "seed_vocabulary_data.sql")
        if not os.path.exists(seed_file):
            print(f"  WARNING: Seed file not found at {seed_file}, terminology tables will be empty")
            return

        with open(seed_file, "r") as f:
            raw = f.read()

        raw = raw.replace("terminology.", f"{s}.")

        statements = []
        current = []
        for line in raw.split("\n"):
            stripped = line.strip()
            if stripped.startswith("--") or stripped == "":
                continue
            current.append(line)
            if stripped.endswith(";"):
                statements.append("\n".join(current))
                current = []

        seeded = 0
        for stmt in statements:
            try:
                sql(self.cur, stmt)
                seeded += 1
            except Exception as e:
                if "duplicate" not in str(e).lower():
                    print(f"  WARNING: Seed statement failed: {str(e)[:80]}")
        print(f"  Seeded {seeded} vocabulary batches from seed file")

    def _table_exists(self, fq_table):
        try:
            sql(self.cur, f"SELECT 1 FROM {fq_table} LIMIT 0")
            return True
        except Exception:
            return False

    def parse_bundles(self):
        t = time.time()

        sql(self.cur, f"""
            CREATE OR REPLACE TABLE {self.fhir_resources} (
                resource_id     VARCHAR(256),
                resource_type   VARCHAR(100),
                bundle_id       VARCHAR(256),
                resource_json   VARIANT,
                parsed_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)

        sql(self.cur, f"""
            INSERT INTO {self.fhir_resources} (resource_id, resource_type, bundle_id, resource_json)
            SELECT
                r.value:resource:id::VARCHAR           AS resource_id,
                r.value:resource:resourceType::VARCHAR  AS resource_type,
                src.BUNDLE_ID                           AS bundle_id,
                r.value:resource                        AS resource_json
            FROM {self.test_source} src,
                LATERAL FLATTEN(input => src.BUNDLE_DATA:entry) r
            WHERE r.value:resource:resourceType IS NOT NULL
        """)

        n = count(self.cur, self.fhir_resources)
        print(f"  Parsed {n} FHIR resources [{elapsed(t)}]")
        return n

    def map_persons(self):
        t = time.time()
        sql(self.cur, f"""
            CREATE OR REPLACE TABLE {self.fq_schema}.PERSON AS
            WITH patients AS (
                SELECT
                    resource_json:id::VARCHAR AS patient_id,
                    resource_json:gender::VARCHAR AS gender,
                    resource_json:birthDate::DATE AS birth_date,
                    resource_json:birthDate::TIMESTAMP_NTZ AS birth_datetime,
                    resource_json AS rj
                FROM {self.fhir_resources}
                WHERE resource_type = 'Patient'
            ),
            race_ext AS (
                SELECT
                    p.patient_id,
                    e.value:valueCoding:code::VARCHAR AS race_code,
                    e.value:valueCoding:display::VARCHAR AS race_display
                FROM patients p,
                    LATERAL FLATTEN(input => p.rj:extension, OUTER => TRUE) e
                WHERE e.value:url::VARCHAR LIKE '%us-core-race'
            ),
            eth_ext AS (
                SELECT
                    p.patient_id,
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
                p.birth_datetime                          AS birth_datetime,
                COALESCE(dr.omop_concept_id, 0)           AS race_concept_id,
                COALESCE(de.omop_concept_id, 0)           AS ethnicity_concept_id,
                NULL::INTEGER                             AS location_id,
                NULL::INTEGER                             AS provider_id,
                NULL::INTEGER                             AS care_site_id,
                p.patient_id                              AS person_source_value,
                p.gender                                  AS gender_source_value,
                0                                         AS gender_source_concept_id,
                r.race_display                            AS race_source_value,
                0                                         AS race_source_concept_id,
                e.eth_display                             AS ethnicity_source_value,
                0                                         AS ethnicity_source_concept_id
            FROM patients p
            LEFT JOIN {self.fq_schema}.DEMOGRAPHIC_TO_OMOP dg
                ON dg.source_code = LOWER(p.gender) AND dg.category = 'gender'
            LEFT JOIN race_ext r ON r.patient_id = p.patient_id
            LEFT JOIN {self.fq_schema}.DEMOGRAPHIC_TO_OMOP dr
                ON dr.source_code = r.race_code AND dr.category = 'race'
            LEFT JOIN eth_ext e ON e.patient_id = p.patient_id
            LEFT JOIN {self.fq_schema}.DEMOGRAPHIC_TO_OMOP de
                ON de.source_code = e.eth_code AND de.category = 'ethnicity'
        """)
        n = count(self.cur, f"{self.fq_schema}.PERSON")
        print(f"  Mapped {n} persons [{elapsed(t)}]")
        return n

    def map_conditions(self):
        t = time.time()
        sql(self.cur, f"""
            CREATE OR REPLACE TABLE {self.fq_schema}.CONDITION_OCCURRENCE AS
            WITH cond_flat AS (
                SELECT
                    r.resource_json:id::VARCHAR AS cond_id,
                    SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1) AS patient_ref,
                    COALESCE(r.resource_json:onsetDateTime::DATE, r.resource_json:onsetPeriod:start::DATE, r.resource_json:recordedDate::DATE) AS start_date,
                    COALESCE(r.resource_json:onsetDateTime::TIMESTAMP_NTZ, r.resource_json:onsetPeriod:start::TIMESTAMP_NTZ) AS start_dt,
                    r.resource_json:abatementDateTime::DATE AS end_date,
                    r.resource_json:abatementDateTime::TIMESTAMP_NTZ AS end_dt,
                    cc.value:code::VARCHAR AS source_code
                FROM {self.fhir_resources} r,
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
                NULL::VARCHAR(256) AS stop_reason,
                NULL::INTEGER AS provider_id,
                NULL::INTEGER AS visit_occurrence_id,
                c.source_code AS condition_source_value,
                0 AS condition_source_concept_id
            FROM cond_flat c
            LEFT JOIN {self.fq_schema}.SNOMED_TO_OMOP sm
                ON sm.snomed_code = c.source_code
        """)
        n = count(self.cur, f"{self.fq_schema}.CONDITION_OCCURRENCE")
        print(f"  Mapped {n} conditions [{elapsed(t)}]")
        return n

    def map_measurements(self):
        t = time.time()
        sql(self.cur, f"""
            CREATE OR REPLACE TABLE {self.fq_schema}.MEASUREMENT AS
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
                FROM {self.fhir_resources} r,
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
                NULL::INTEGER AS provider_id,
                NULL::INTEGER AS visit_occurrence_id,
                o.source_code AS measurement_source_value,
                0 AS measurement_source_concept_id,
                o.unit_src AS unit_source_value,
                o.val_src AS value_source_value
            FROM obs_flat o
            LEFT JOIN {self.fq_schema}.LOINC_TO_OMOP lm
                ON lm.loinc_code = o.source_code
        """)
        n = count(self.cur, f"{self.fq_schema}.MEASUREMENT")
        print(f"  Mapped {n} measurements [{elapsed(t)}]")
        return n

    def map_visits(self):
        t = time.time()
        visit_type_map = {
            'AMB': 9202, 'IMP': 9201, 'EMER': 9203, 'HH': 581476,
            'FLD': 38004193, 'VR': 5083, 'SS': 9202,
        }
        cases = " ".join([f"WHEN '{k}' THEN {v}" for k, v in visit_type_map.items()])
        sql(self.cur, f"""
            CREATE OR REPLACE TABLE {self.fq_schema}.VISIT_OCCURRENCE AS
            SELECT
                ABS(HASH(r.resource_json:id::VARCHAR)) % 2147483647 AS visit_occurrence_id,
                ABS(HASH(SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1))) % 2147483647 AS person_id,
                CASE r.resource_json:class:code::VARCHAR {cases} ELSE 0 END AS visit_concept_id,
                r.resource_json:period:start::DATE AS visit_start_date,
                r.resource_json:period:start::TIMESTAMP_NTZ AS visit_start_datetime,
                COALESCE(r.resource_json:period:end::DATE, r.resource_json:period:start::DATE) AS visit_end_date,
                r.resource_json:period:end::TIMESTAMP_NTZ AS visit_end_datetime,
                32817 AS visit_type_concept_id,
                NULL::INTEGER AS provider_id,
                NULL::INTEGER AS care_site_id,
                r.resource_json:class:code::VARCHAR AS visit_source_value,
                0 AS visit_source_concept_id,
                0 AS admitted_from_concept_id,
                0 AS discharged_to_concept_id
            FROM {self.fhir_resources} r
            WHERE r.resource_type = 'Encounter'
        """)
        n = count(self.cur, f"{self.fq_schema}.VISIT_OCCURRENCE")
        print(f"  Mapped {n} visits [{elapsed(t)}]")
        return n

    def map_drug_exposures(self):
        t = time.time()
        sql(self.cur, f"""
            CREATE OR REPLACE TABLE {self.fq_schema}.DRUG_EXPOSURE AS
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
                FROM {self.fhir_resources} r,
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
                NULL::INTEGER AS provider_id,
                NULL::INTEGER AS visit_occurrence_id,
                m.source_code AS drug_source_value,
                0 AS drug_source_concept_id,
                m.route_src AS route_source_value,
                m.dose_unit_src AS dose_unit_source_value
            FROM med_flat m
            LEFT JOIN {self.fq_schema}.RXNORM_TO_OMOP rx
                ON rx.rxnorm_code = m.source_code
        """)
        n = count(self.cur, f"{self.fq_schema}.DRUG_EXPOSURE")
        print(f"  Mapped {n} drug exposures [{elapsed(t)}]")
        return n

    def map_procedures(self):
        t = time.time()
        sql(self.cur, f"""
            CREATE OR REPLACE TABLE {self.fq_schema}.PROCEDURE_OCCURRENCE AS
            WITH proc_flat AS (
                SELECT
                    r.resource_json:id::VARCHAR AS proc_id,
                    SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1) AS patient_ref,
                    COALESCE(r.resource_json:performedDateTime::DATE, r.resource_json:performedPeriod:start::DATE) AS proc_date,
                    COALESCE(r.resource_json:performedDateTime::TIMESTAMP_NTZ, r.resource_json:performedPeriod:start::TIMESTAMP_NTZ) AS proc_dt,
                    r.resource_json:performedPeriod:end::DATE AS proc_end_date,
                    pc.value:code::VARCHAR AS source_code,
                    pc.value:system::VARCHAR AS code_system
                FROM {self.fhir_resources} r,
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
                NULL::INTEGER AS provider_id,
                NULL::INTEGER AS visit_occurrence_id,
                pf.source_code AS procedure_source_value,
                0 AS procedure_source_concept_id
            FROM proc_flat pf
            LEFT JOIN {self.fq_schema}.SNOMED_TO_OMOP sm
                ON sm.snomed_code = pf.source_code AND pf.code_system LIKE '%snomed%'
            LEFT JOIN {self.fq_schema}.CPT_TO_OMOP cpt
                ON cpt.cpt_code = pf.source_code AND pf.code_system LIKE '%cpt%'
            LEFT JOIN {self.fq_schema}.HCPCS_TO_OMOP hc
                ON hc.hcpcs_code = pf.source_code AND pf.code_system LIKE '%hcpcs%'
        """)
        n = count(self.cur, f"{self.fq_schema}.PROCEDURE_OCCURRENCE")
        print(f"  Mapped {n} procedures [{elapsed(t)}]")
        return n

    def map_observations_qual(self):
        t = time.time()
        sql(self.cur, f"""
            CREATE OR REPLACE TABLE {self.fq_schema}.OBSERVATION AS
            WITH obs_qual AS (
                SELECT
                    r.resource_json:id::VARCHAR AS obs_id,
                    SPLIT_PART(r.resource_json:subject:reference::VARCHAR, '/', -1) AS patient_ref,
                    COALESCE(r.resource_json:effectiveDateTime::DATE, r.resource_json:issued::DATE) AS obs_date,
                    COALESCE(r.resource_json:effectiveDateTime::TIMESTAMP_NTZ, r.resource_json:issued::TIMESTAMP_NTZ) AS obs_dt,
                    COALESCE(r.resource_json:valueCodeableConcept:text::VARCHAR, r.resource_json:valueCodeableConcept:coding[0]:display::VARCHAR, r.resource_json:valueString::VARCHAR) AS val_str,
                    oc.value:code::VARCHAR AS source_code
                FROM {self.fhir_resources} r,
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
            LEFT JOIN {self.fq_schema}.LOINC_TO_OMOP lm
                ON lm.loinc_code = q.source_code
        """)
        n = count(self.cur, f"{self.fq_schema}.OBSERVATION")
        print(f"  Mapped {n} qualitative observations [{elapsed(t)}]")
        return n

    def map_death(self):
        t = time.time()
        sql(self.cur, f"""
            CREATE OR REPLACE TABLE {self.fq_schema}.DEATH AS
            SELECT
                ABS(HASH(r.resource_json:id::VARCHAR)) % 2147483647 AS person_id,
                r.resource_json:deceasedDateTime::DATE AS death_date,
                r.resource_json:deceasedDateTime::TIMESTAMP_NTZ AS death_datetime,
                32817 AS death_type_concept_id,
                0 AS cause_concept_id,
                NULL::VARCHAR(256) AS cause_source_value
            FROM {self.fhir_resources} r
            WHERE r.resource_type = 'Patient'
                AND (r.resource_json:deceasedDateTime IS NOT NULL OR r.resource_json:deceasedBoolean::BOOLEAN = TRUE)
        """)
        n = count(self.cur, f"{self.fq_schema}.DEATH")
        print(f"  Mapped {n} death records [{elapsed(t)}]")
        return n

    def run_pipeline(self):
        t = time.time()
        print("\n--- Pipeline Execution ---")
        self.create_terminology_tables()
        resource_count = self.parse_bundles()
        self.map_persons()
        self.map_conditions()
        self.map_measurements()
        self.map_visits()
        self.map_drug_exposures()
        self.map_procedures()
        self.map_observations_qual()
        self.map_death()
        self.map_immunizations()
        self.map_locations()
        self.map_organizations()
        self.map_practitioners()
        self.build_observation_periods()
        self.build_cdm_source()
        if self.output_format == "tuva":
            self.run_tuva_pipeline()
        print(f"  Pipeline total [{elapsed(t)}]")
        return resource_count

    def validate_level1(self, bundle_count, resource_count):
        print("\n--- Level 1 Checks: Smoke Test (1 bundle) ---")
        expected_bundles = {1: 1, 2: 10, 3: 1000}.get(self.level, 1)
        self.check(bundle_count == expected_bundles, f"Test table has {expected_bundles} bundle(s)", f"got {bundle_count}")
        self.check(resource_count > 0, f"FHIR resources parsed", f"{resource_count} resources")

        person_count = count(self.cur, f"{self.fq_schema}.PERSON")
        self.check(person_count >= 1, f"At least 1 person mapped", f"got {person_count}")

        tables_to_check = OMOP_TABLES
        if self.output_format == "tuva":
            tables_to_check = tables_to_check + ["patient", "encounter", "condition"]
        for tbl in tables_to_check:
            try:
                schema = self.fq_schema
                if tbl in [t.lower() for t in ["PATIENT", "ENCOUNTER", "CONDITION", "LAB_RESULT", "OBSERVATION", "MEDICATION", "IMMUNIZATION", "PROCEDURE", "LOCATION", "PRACTITIONER", "MEDICAL_CLAIM", "ELIGIBILITY", "APPOINTMENT"]] and self.output_format == "tuva":
                    schema = f"{TEST_DB}.TUVA_TEST_INPUT"
                sql(self.cur, f"SELECT * FROM {schema}.{tbl.upper()} LIMIT 0")
                self.check(True, f"Table {tbl} created")
            except Exception:
                self.check(False, f"Table {tbl} created")

        type_rows = sql(self.cur, f"SELECT DISTINCT resource_type FROM {self.fhir_resources}")
        types = [r[0] for r in type_rows]
        self.check(len(types) >= 2, f"Multiple resource types parsed", f"{', '.join(types)}")

    def validate_level2(self, bundle_count):
        print("\n--- Level 2 Checks: 10 Bundle Validation ---")

        person_count = count(self.cur, f"{self.fq_schema}.PERSON")
        # Each Synthea bundle has ~2 patients; 10 bundles ≈ ~20 persons
        self.check(
            person_count >= 5,
            f"Proportional person output",
            f"{person_count} persons from {bundle_count} bundles"
        )

        for tbl, concept_col in [
            ("PERSON", "GENDER_CONCEPT_ID"),
            ("CONDITION_OCCURRENCE", "CONDITION_CONCEPT_ID"),
            ("MEASUREMENT", "MEASUREMENT_CONCEPT_ID"),
        ]:
            try:
                rows = sql(self.cur, f"""
                    SELECT COUNT(*) AS total,
                           SUM(CASE WHEN {concept_col} != 0 THEN 1 ELSE 0 END) AS mapped
                    FROM {self.fq_schema}.{tbl}
                """)
                total, mapped = rows[0]
                if total > 0:
                    pct = (mapped / total) * 100
                    self.check(mapped > 0, f"{tbl} has non-zero concept_ids", f"{mapped}/{total} = {pct:.1f}%")
                else:
                    self.check(False, f"{tbl} has rows", "0 rows")
            except Exception as e:
                self.check(False, f"{tbl} concept_id check", str(e)[:100])

        # Referential integrity: condition.person_id exists in person
        orphan_rows = sql(self.cur, f"""
            SELECT COUNT(*) FROM {self.fq_schema}.CONDITION_OCCURRENCE c
            WHERE NOT EXISTS (
                SELECT 1 FROM {self.fq_schema}.PERSON p WHERE p.person_id = c.person_id
            )
        """)
        orphans = orphan_rows[0][0]
        total_conds = count(self.cur, f"{self.fq_schema}.CONDITION_OCCURRENCE")
        self.check(
            orphans == 0 or (total_conds > 0 and orphans / total_conds < 0.5),
            f"Condition→Person referential integrity",
            f"{orphans} orphans out of {total_conds} conditions"
        )

        print("\n  --- Mapping Coverage by Domain ---")
        for tbl, concept_col in [
            ("PERSON", "GENDER_CONCEPT_ID"),
            ("CONDITION_OCCURRENCE", "CONDITION_CONCEPT_ID"),
            ("MEASUREMENT", "MEASUREMENT_CONCEPT_ID"),
            ("VISIT_OCCURRENCE", "VISIT_CONCEPT_ID"),
            ("DRUG_EXPOSURE", "DRUG_CONCEPT_ID"),
            ("PROCEDURE_OCCURRENCE", "PROCEDURE_CONCEPT_ID"),
        ]:
            try:
                rows = sql(self.cur, f"""
                    SELECT COUNT(*) AS total,
                           SUM(CASE WHEN {concept_col} != 0 THEN 1 ELSE 0 END) AS mapped
                    FROM {self.fq_schema}.{tbl}
                """)
                total, mapped = rows[0]
                pct = (mapped / total * 100) if total > 0 else 0
                print(f"    {tbl:<30s}  {mapped:>6d} / {total:>6d}  ({pct:5.1f}%)")
            except Exception:
                print(f"    {tbl:<30s}  [not available]")

    def validate_level3(self):
        print("\n--- Level 3 Checks: Full Dataset vs Ground Truth (±10% tolerance) ---")

        for tbl, (gt_table, gt_expected) in GROUND_TRUTH.items():
            try:
                actual = count(self.cur, f"{self.fq_schema}.{tbl.upper()}")
                lo = gt_expected * 0.9
                hi = gt_expected * 1.1
                within = lo <= actual <= hi
                self.check(
                    within,
                    f"{tbl} count within ±10% of ground truth",
                    f"actual={actual}, expected={gt_expected}, range=[{lo:.0f}, {hi:.0f}]"
                )
            except Exception as e:
                self.check(False, f"{tbl} ground truth comparison", str(e)[:100])

        print("\n  --- Full Coverage Report ---")
        for tbl, concept_col in [
            ("PERSON", "GENDER_CONCEPT_ID"),
            ("CONDITION_OCCURRENCE", "CONDITION_CONCEPT_ID"),
            ("MEASUREMENT", "MEASUREMENT_CONCEPT_ID"),
            ("VISIT_OCCURRENCE", "VISIT_CONCEPT_ID"),
            ("DRUG_EXPOSURE", "DRUG_CONCEPT_ID"),
            ("PROCEDURE_OCCURRENCE", "PROCEDURE_CONCEPT_ID"),
            ("OBSERVATION", "OBSERVATION_CONCEPT_ID"),
        ]:
            try:
                rows = sql(self.cur, f"""
                    SELECT COUNT(*) AS total,
                           SUM(CASE WHEN {concept_col} != 0 THEN 1 ELSE 0 END) AS mapped
                    FROM {self.fq_schema}.{tbl}
                """)
                total, mapped = rows[0]
                pct = (mapped / total * 100) if total > 0 else 0
                print(f"    {tbl:<30s}  {mapped:>8d} / {total:>8d}  ({pct:5.1f}%)")
            except Exception:
                print(f"    {tbl:<30s}  [not available]")

        print("\n  --- Quality Summary ---")
        for tbl in OMOP_TABLES:
            try:
                n = count(self.cur, f"{self.fq_schema}.{tbl.upper()}")
                print(f"    {tbl:<30s}  {n:>8d} rows")
            except Exception:
                print(f"    {tbl:<30s}  [missing]")

    def map_immunizations(self):
        t = time.time()
        sql(self.cur, f"""
            CREATE OR REPLACE TABLE {self.fq_schema}.DRUG_EXPOSURE_IMMUNIZATION AS
            SELECT
                ABS(HASH(r.resource_json:id::VARCHAR)) % 2147483647 AS drug_exposure_id,
                ABS(HASH(SPLIT_PART(r.resource_json:patient:reference::VARCHAR, '/', -1))) % 2147483647 AS person_id,
                0 AS drug_concept_id,
                r.resource_json:occurrenceDateTime::DATE AS drug_exposure_start_date,
                r.resource_json:occurrenceDateTime::TIMESTAMP_NTZ AS drug_exposure_start_datetime,
                NULL::DATE AS drug_exposure_end_date,
                NULL::TIMESTAMP_NTZ AS drug_exposure_end_datetime,
                NULL::DATE AS verbatim_end_date,
                32817 AS drug_type_concept_id,
                NULL::VARCHAR(256) AS stop_reason,
                NULL::INTEGER AS refills,
                NULL::FLOAT AS quantity,
                NULL::INTEGER AS days_supply,
                NULL::VARCHAR(1000) AS sig,
                0 AS route_concept_id,
                r.resource_json:lotNumber::VARCHAR AS lot_number,
                NULL::INTEGER AS provider_id,
                NULL::INTEGER AS visit_occurrence_id,
                vc.value:code::VARCHAR AS drug_source_value,
                0 AS drug_source_concept_id,
                NULL::VARCHAR(256) AS route_source_value,
                NULL::VARCHAR(256) AS dose_unit_source_value
            FROM {self.fhir_resources} r,
                LATERAL FLATTEN(input => r.resource_json:vaccineCode:coding, OUTER => TRUE) vc
            WHERE r.resource_type = 'Immunization'
        """)
        n = count(self.cur, f"{self.fq_schema}.DRUG_EXPOSURE_IMMUNIZATION")
        print(f"  Mapped {n} immunizations [{elapsed(t)}]")
        return n

    def map_locations(self):
        t = time.time()
        sql(self.cur, f"""
            CREATE OR REPLACE TABLE {self.fq_schema}.LOCATION AS
            SELECT
                ABS(HASH(r.resource_json:id::VARCHAR)) % 2147483647 AS location_id,
                r.resource_json:address:line[0]::VARCHAR AS address_1,
                NULL::VARCHAR(256) AS address_2,
                r.resource_json:address:city::VARCHAR AS city,
                r.resource_json:address:state::VARCHAR AS state,
                r.resource_json:address:postalCode::VARCHAR AS zip,
                NULL::VARCHAR(256) AS county,
                r.resource_json:address:country::VARCHAR AS country_concept_id,
                NULL::FLOAT AS latitude,
                NULL::FLOAT AS longitude,
                r.resource_json:id::VARCHAR AS location_source_value
            FROM {self.fhir_resources} r
            WHERE r.resource_type = 'Location'
        """)
        n = count(self.cur, f"{self.fq_schema}.LOCATION")
        print(f"  Mapped {n} locations [{elapsed(t)}]")
        return n

    def map_organizations(self):
        t = time.time()
        sql(self.cur, f"""
            CREATE OR REPLACE TABLE {self.fq_schema}.CARE_SITE AS
            SELECT
                ABS(HASH(r.resource_json:id::VARCHAR)) % 2147483647 AS care_site_id,
                r.resource_json:name::VARCHAR AS care_site_name,
                0 AS place_of_service_concept_id,
                NULL::INTEGER AS location_id,
                r.resource_json:id::VARCHAR AS care_site_source_value,
                NULL::VARCHAR(256) AS place_of_service_source_value
            FROM {self.fhir_resources} r
            WHERE r.resource_type = 'Organization'
        """)
        n = count(self.cur, f"{self.fq_schema}.CARE_SITE")
        print(f"  Mapped {n} care sites [{elapsed(t)}]")
        return n

    def map_practitioners(self):
        t = time.time()
        sql(self.cur, f"""
            CREATE OR REPLACE TABLE {self.fq_schema}.PROVIDER AS
            SELECT
                ABS(HASH(r.resource_json:id::VARCHAR)) % 2147483647 AS provider_id,
                COALESCE(r.resource_json:name[0]:family::VARCHAR, '') || ', ' ||
                    COALESCE(r.resource_json:name[0]:given[0]::VARCHAR, '') AS provider_name,
                NULL::VARCHAR(256) AS npi,
                NULL::VARCHAR(256) AS dea,
                0 AS specialty_concept_id,
                NULL::INTEGER AS care_site_id,
                NULL::INTEGER AS year_of_birth,
                0 AS gender_concept_id,
                r.resource_json:id::VARCHAR AS provider_source_value,
                NULL::VARCHAR(256) AS specialty_source_value,
                0 AS specialty_source_concept_id,
                NULL::VARCHAR(256) AS gender_source_value,
                0 AS gender_source_concept_id
            FROM {self.fhir_resources} r
            WHERE r.resource_type = 'Practitioner'
        """)
        n = count(self.cur, f"{self.fq_schema}.PROVIDER")
        print(f"  Mapped {n} providers [{elapsed(t)}]")
        return n

    def build_observation_periods(self):
        t = time.time()
        sql(self.cur, f"""
            CREATE OR REPLACE TABLE {self.fq_schema}.OBSERVATION_PERIOD AS
            WITH all_events AS (
                SELECT person_id, condition_start_date AS event_date FROM {self.fq_schema}.CONDITION_OCCURRENCE WHERE condition_start_date IS NOT NULL
                UNION ALL
                SELECT person_id, measurement_date AS event_date FROM {self.fq_schema}.MEASUREMENT WHERE measurement_date IS NOT NULL
                UNION ALL
                SELECT person_id, visit_start_date AS event_date FROM {self.fq_schema}.VISIT_OCCURRENCE WHERE visit_start_date IS NOT NULL
                UNION ALL
                SELECT person_id, drug_exposure_start_date AS event_date FROM {self.fq_schema}.DRUG_EXPOSURE WHERE drug_exposure_start_date IS NOT NULL
            )
            SELECT
                ROW_NUMBER() OVER (ORDER BY person_id) AS observation_period_id,
                person_id,
                MIN(event_date) AS observation_period_start_date,
                MAX(event_date) AS observation_period_end_date,
                32817 AS period_type_concept_id
            FROM all_events
            GROUP BY person_id
        """)
        n = count(self.cur, f"{self.fq_schema}.OBSERVATION_PERIOD")
        print(f"  Built {n} observation periods [{elapsed(t)}]")
        return n

    def build_cdm_source(self):
        t = time.time()
        sql(self.cur, f"""
            CREATE OR REPLACE TABLE {self.fq_schema}.CDM_SOURCE AS
            SELECT
                'Snowflake Health Data Forge' AS cdm_source_name,
                'Health Data Forge Test' AS cdm_source_abbreviation,
                'Test run' AS cdm_holder,
                NULL::VARCHAR(256) AS source_description,
                NULL::VARCHAR(256) AS source_documentation_reference,
                NULL::VARCHAR(256) AS cdm_etl_reference,
                CURRENT_DATE() AS source_release_date,
                CURRENT_DATE() AS cdm_release_date,
                'OMOP CDM v5.4' AS cdm_version,
                756265 AS cdm_version_concept_id,
                0 AS vocabulary_version
        """)
        print(f"  Built cdm_source [{elapsed(t)}]")

    def run_tuva_pipeline(self):
        t = time.time()
        tuva_schema = f"{TEST_DB}.TUVA_TEST_INPUT"
        sql(self.cur, f"CREATE SCHEMA IF NOT EXISTS {tuva_schema}")

        sql(self.cur, f"""
            CREATE OR REPLACE TABLE {tuva_schema}.PATIENT AS
            SELECT
                resource_json:id::VARCHAR AS patient_id,
                resource_json:gender::VARCHAR AS sex,
                resource_json:birthDate::DATE AS birth_date,
                NULL::DATE AS death_date,
                CASE WHEN resource_json:deceasedBoolean::BOOLEAN = TRUE THEN 1 ELSE 0 END AS death_flag,
                'fhir' AS data_source
            FROM {self.fhir_resources}
            WHERE resource_type = 'Patient'
        """)

        sql(self.cur, f"""
            CREATE OR REPLACE TABLE {tuva_schema}.ENCOUNTER AS
            SELECT
                resource_json:id::VARCHAR AS encounter_id,
                SPLIT_PART(resource_json:subject:reference::VARCHAR, '/', -1) AS patient_id,
                resource_json:class:code::VARCHAR AS encounter_type,
                resource_json:period:start::TIMESTAMP_NTZ AS encounter_start_date,
                resource_json:period:end::TIMESTAMP_NTZ AS encounter_end_date,
                'fhir' AS data_source
            FROM {self.fhir_resources}
            WHERE resource_type = 'Encounter'
        """)

        sql(self.cur, f"""
            CREATE OR REPLACE TABLE {tuva_schema}.CONDITION AS
            SELECT
                resource_json:id::VARCHAR AS condition_id,
                SPLIT_PART(resource_json:subject:reference::VARCHAR, '/', -1) AS patient_id,
                cc.value:code::VARCHAR AS source_code,
                cc.value:system::VARCHAR AS source_code_type,
                COALESCE(resource_json:onsetDateTime::DATE, resource_json:recordedDate::DATE) AS condition_date,
                'fhir' AS data_source
            FROM {self.fhir_resources},
                LATERAL FLATTEN(input => resource_json:code:coding, OUTER => TRUE) cc
            WHERE resource_type = 'Condition'
        """)

        for tbl in ["PATIENT", "ENCOUNTER", "CONDITION"]:
            n = count(self.cur, f"{tuva_schema}.{tbl}")
            print(f"  Tuva {tbl}: {n} rows")

        if not self.keep:
            sql(self.cur, f"DROP SCHEMA IF EXISTS {tuva_schema} CASCADE")
        print(f"  Tuva pipeline [{elapsed(t)}]")

    def cleanup(self):
        if not self.keep:
            t = time.time()
            sql(self.cur, f"DROP SCHEMA IF EXISTS {self.fq_schema} CASCADE")
            print(f"\n  Cleaned up test schema {self.fq_schema} [{elapsed(t)}]")
        else:
            print(f"\n  --keep flag set: test schema {self.fq_schema} retained")

    def run(self):
        total_start = time.time()
        print(f"{'='*60}")
        print(f"FHIR-to-OMOP Transformation Test — Level {self.level}")
        print(f"{'='*60}")

        try:
            print("\n--- Setup ---")
            bundle_count = self.setup()
            resource_count = self.run_pipeline()

            self.validate_level1(bundle_count, resource_count)

            if self.level >= 2:
                self.validate_level2(bundle_count)

            if self.level >= 3:
                self.validate_level3()

        except Exception as e:
            print(f"\n  FATAL: {e}")
            self.failed += 1
        finally:
            self.cleanup()

        print(f"\n{'='*60}")
        print(f"Results: {self.passed} passed, {self.failed} failed [{elapsed(total_start)}]")
        print(f"{'='*60}")

        self.cur.close()
        self.conn.close()
        return 1 if self.failed > 0 else 0


def main():
    parser = argparse.ArgumentParser(description="Graduated FHIR-to-OMOP transformation test")
    parser.add_argument("--level", type=int, choices=[1, 2, 3], default=1,
                        help="Test level: 1=smoke (1 bundle), 2=ten bundles, 3=full dataset")
    parser.add_argument("--keep", action="store_true",
                        help="Keep test schema after run")
    parser.add_argument("--format", choices=["omop", "tuva"], default="omop",
                        help="Output format: omop (default) or tuva")
    args = parser.parse_args()
    sys.exit(TestRunner(args.level, args.keep, args.format).run())


if __name__ == "__main__":
    main()
