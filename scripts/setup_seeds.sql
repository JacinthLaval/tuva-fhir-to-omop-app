-- =============================================================================
-- Vocabulary / Terminology Crosswalk Tables
--
-- Source data: OHDSI Athena (https://athena.ohdsi.org/)
-- Table structure: follows patterns from Tuva Health (Apache 2.0,
--   https://github.com/tuva-health/tuva) terminology seeds
-- Purpose: map source coding systems (LOINC, SNOMED, RxNorm, ICD-10, etc.)
--   to OMOP standard concept IDs for the FHIR → OMOP CDM transformation
-- Multi-cloud: pure Snowflake SQL, no cloud-specific features
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS terminology;
GRANT USAGE ON SCHEMA terminology TO APPLICATION ROLE app_user;
GRANT USAGE ON SCHEMA terminology TO APPLICATION ROLE app_admin;

-- ---------------------------------------------------------------------------
-- LOINC → OMOP Concept mapping
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS terminology.loinc_to_omop (
    loinc_code          VARCHAR(20)     NOT NULL,
    loinc_description   VARCHAR(512),
    omop_concept_id     INTEGER         NOT NULL,
    omop_concept_name   VARCHAR(512),
    omop_domain_id      VARCHAR(50),
    omop_vocabulary_id  VARCHAR(50)     DEFAULT 'LOINC',
    standard_concept    VARCHAR(1)      DEFAULT 'S',
    PRIMARY KEY (loinc_code)
);
GRANT SELECT ON TABLE terminology.loinc_to_omop TO APPLICATION ROLE app_user;

-- ---------------------------------------------------------------------------
-- SNOMED CT → OMOP Concept mapping
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS terminology.snomed_to_omop (
    snomed_code         VARCHAR(20)     NOT NULL,
    snomed_description  VARCHAR(512),
    omop_concept_id     INTEGER         NOT NULL,
    omop_concept_name   VARCHAR(512),
    omop_domain_id      VARCHAR(50),
    omop_vocabulary_id  VARCHAR(50)     DEFAULT 'SNOMED',
    standard_concept    VARCHAR(1)      DEFAULT 'S',
    PRIMARY KEY (snomed_code)
);
GRANT SELECT ON TABLE terminology.snomed_to_omop TO APPLICATION ROLE app_user;

-- ---------------------------------------------------------------------------
-- RxNorm → OMOP Drug Concept mapping
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS terminology.rxnorm_to_omop (
    rxnorm_code         VARCHAR(20)     NOT NULL,
    rxnorm_description  VARCHAR(512),
    omop_concept_id     INTEGER         NOT NULL,
    omop_concept_name   VARCHAR(512),
    omop_domain_id      VARCHAR(50)     DEFAULT 'Drug',
    omop_vocabulary_id  VARCHAR(50)     DEFAULT 'RxNorm',
    standard_concept    VARCHAR(1)      DEFAULT 'S',
    PRIMARY KEY (rxnorm_code)
);
GRANT SELECT ON TABLE terminology.rxnorm_to_omop TO APPLICATION ROLE app_user;

-- ---------------------------------------------------------------------------
-- ICD-10-CM → OMOP Condition mapping
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS terminology.icd10cm_to_omop (
    icd10cm_code        VARCHAR(20)     NOT NULL,
    icd10cm_description VARCHAR(512),
    omop_concept_id     INTEGER         NOT NULL,
    omop_concept_name   VARCHAR(512),
    omop_domain_id      VARCHAR(50)     DEFAULT 'Condition',
    omop_vocabulary_id  VARCHAR(50)     DEFAULT 'ICD10CM',
    standard_concept    VARCHAR(1),
    PRIMARY KEY (icd10cm_code)
);
GRANT SELECT ON TABLE terminology.icd10cm_to_omop TO APPLICATION ROLE app_user;

-- ---------------------------------------------------------------------------
-- ICD-10-PCS → OMOP Procedure mapping
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS terminology.icd10pcs_to_omop (
    icd10pcs_code       VARCHAR(20)     NOT NULL,
    icd10pcs_description VARCHAR(512),
    omop_concept_id     INTEGER         NOT NULL,
    omop_concept_name   VARCHAR(512),
    omop_domain_id      VARCHAR(50)     DEFAULT 'Procedure',
    omop_vocabulary_id  VARCHAR(50)     DEFAULT 'ICD10PCS',
    standard_concept    VARCHAR(1),
    PRIMARY KEY (icd10pcs_code)
);
GRANT SELECT ON TABLE terminology.icd10pcs_to_omop TO APPLICATION ROLE app_user;

-- ---------------------------------------------------------------------------
-- CPT → OMOP Procedure mapping
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS terminology.cpt_to_omop (
    cpt_code            VARCHAR(20)     NOT NULL,
    cpt_description     VARCHAR(512),
    omop_concept_id     INTEGER         NOT NULL,
    omop_concept_name   VARCHAR(512),
    omop_domain_id      VARCHAR(50)     DEFAULT 'Procedure',
    omop_vocabulary_id  VARCHAR(50)     DEFAULT 'CPT4',
    standard_concept    VARCHAR(1),
    PRIMARY KEY (cpt_code)
);
GRANT SELECT ON TABLE terminology.cpt_to_omop TO APPLICATION ROLE app_user;

-- ---------------------------------------------------------------------------
-- HCPCS → OMOP mapping
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS terminology.hcpcs_to_omop (
    hcpcs_code          VARCHAR(20)     NOT NULL,
    hcpcs_description   VARCHAR(512),
    omop_concept_id     INTEGER         NOT NULL,
    omop_concept_name   VARCHAR(512),
    omop_domain_id      VARCHAR(50),
    omop_vocabulary_id  VARCHAR(50)     DEFAULT 'HCPCS',
    standard_concept    VARCHAR(1),
    PRIMARY KEY (hcpcs_code)
);
GRANT SELECT ON TABLE terminology.hcpcs_to_omop TO APPLICATION ROLE app_user;

-- ---------------------------------------------------------------------------
-- Gender / Race / Ethnicity → OMOP Concept
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS terminology.demographic_to_omop (
    source_system       VARCHAR(50)     NOT NULL,
    source_code         VARCHAR(50)     NOT NULL,
    source_description  VARCHAR(256),
    omop_concept_id     INTEGER         NOT NULL,
    omop_concept_name   VARCHAR(256),
    omop_domain_id      VARCHAR(50),
    category            VARCHAR(50)     NOT NULL,
    PRIMARY KEY (source_system, source_code, category)
);
GRANT SELECT ON TABLE terminology.demographic_to_omop TO APPLICATION ROLE app_user;

-- ---------------------------------------------------------------------------
-- OMOP Concept table (core reference — subset of OHDSI Athena)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS terminology.concept (
    concept_id          INTEGER         NOT NULL,
    concept_name        VARCHAR(512)    NOT NULL,
    domain_id           VARCHAR(50)     NOT NULL,
    vocabulary_id       VARCHAR(50)     NOT NULL,
    concept_class_id    VARCHAR(50),
    standard_concept    VARCHAR(1),
    concept_code        VARCHAR(50),
    valid_start_date    DATE,
    valid_end_date      DATE,
    invalid_reason      VARCHAR(1),
    PRIMARY KEY (concept_id)
);
GRANT SELECT ON TABLE terminology.concept TO APPLICATION ROLE app_user;

-- ---------------------------------------------------------------------------
-- OMOP Concept Relationship (for mapping non-standard → standard)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS terminology.concept_relationship (
    concept_id_1        INTEGER         NOT NULL,
    concept_id_2        INTEGER         NOT NULL,
    relationship_id     VARCHAR(50)     NOT NULL,
    valid_start_date    DATE,
    valid_end_date      DATE,
    invalid_reason      VARCHAR(1),
    PRIMARY KEY (concept_id_1, concept_id_2, relationship_id)
);
GRANT SELECT ON TABLE terminology.concept_relationship TO APPLICATION ROLE app_user;
