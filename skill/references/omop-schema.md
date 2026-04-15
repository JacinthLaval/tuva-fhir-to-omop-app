# OMOP CDM v5.4 Target Schema

## Terminology Crosswalk Tables (10 tables)

All tables live in the `terminology` schema. Grant SELECT to `app_user`, USAGE on schema to both roles.

### loinc_to_omop
```sql
CREATE TABLE IF NOT EXISTS terminology.loinc_to_omop (
    loinc_code VARCHAR(20) NOT NULL PRIMARY KEY,
    loinc_description VARCHAR(512),
    omop_concept_id INTEGER NOT NULL,
    omop_concept_name VARCHAR(512),
    omop_domain_id VARCHAR(50),
    omop_vocabulary_id VARCHAR(50) DEFAULT 'LOINC',
    standard_concept VARCHAR(1) DEFAULT 'S'
);
```

### snomed_to_omop
```sql
CREATE TABLE IF NOT EXISTS terminology.snomed_to_omop (
    snomed_code VARCHAR(20) NOT NULL PRIMARY KEY,
    snomed_description VARCHAR(512),
    omop_concept_id INTEGER NOT NULL,
    omop_concept_name VARCHAR(512),
    omop_domain_id VARCHAR(50),
    omop_vocabulary_id VARCHAR(50) DEFAULT 'SNOMED',
    standard_concept VARCHAR(1) DEFAULT 'S'
);
```

### rxnorm_to_omop
```sql
CREATE TABLE IF NOT EXISTS terminology.rxnorm_to_omop (
    rxnorm_code VARCHAR(20) NOT NULL PRIMARY KEY,
    rxnorm_description VARCHAR(512),
    omop_concept_id INTEGER NOT NULL,
    omop_concept_name VARCHAR(512),
    omop_domain_id VARCHAR(50) DEFAULT 'Drug',
    omop_vocabulary_id VARCHAR(50) DEFAULT 'RxNorm',
    standard_concept VARCHAR(1) DEFAULT 'S'
);
```

### icd10cm_to_omop
```sql
CREATE TABLE IF NOT EXISTS terminology.icd10cm_to_omop (
    icd10cm_code VARCHAR(20) NOT NULL PRIMARY KEY,
    icd10cm_description VARCHAR(512),
    omop_concept_id INTEGER NOT NULL,
    omop_concept_name VARCHAR(512),
    omop_domain_id VARCHAR(50) DEFAULT 'Condition',
    omop_vocabulary_id VARCHAR(50) DEFAULT 'ICD10CM',
    standard_concept VARCHAR(1)
);
```

### icd10pcs_to_omop
```sql
CREATE TABLE IF NOT EXISTS terminology.icd10pcs_to_omop (
    icd10pcs_code VARCHAR(20) NOT NULL PRIMARY KEY,
    icd10pcs_description VARCHAR(512),
    omop_concept_id INTEGER NOT NULL,
    omop_concept_name VARCHAR(512),
    omop_domain_id VARCHAR(50) DEFAULT 'Procedure',
    omop_vocabulary_id VARCHAR(50) DEFAULT 'ICD10PCS',
    standard_concept VARCHAR(1)
);
```

### cpt_to_omop
```sql
CREATE TABLE IF NOT EXISTS terminology.cpt_to_omop (
    cpt_code VARCHAR(20) NOT NULL PRIMARY KEY,
    cpt_description VARCHAR(512),
    omop_concept_id INTEGER NOT NULL,
    omop_concept_name VARCHAR(512),
    omop_domain_id VARCHAR(50) DEFAULT 'Procedure',
    omop_vocabulary_id VARCHAR(50) DEFAULT 'CPT4',
    standard_concept VARCHAR(1)
);
```

### hcpcs_to_omop
```sql
CREATE TABLE IF NOT EXISTS terminology.hcpcs_to_omop (
    hcpcs_code VARCHAR(20) NOT NULL PRIMARY KEY,
    hcpcs_description VARCHAR(512),
    omop_concept_id INTEGER NOT NULL,
    omop_concept_name VARCHAR(512),
    omop_domain_id VARCHAR(50),
    omop_vocabulary_id VARCHAR(50) DEFAULT 'HCPCS',
    standard_concept VARCHAR(1)
);
```

### demographic_to_omop
```sql
CREATE TABLE IF NOT EXISTS terminology.demographic_to_omop (
    source_system VARCHAR(50) NOT NULL,
    source_code VARCHAR(50) NOT NULL,
    source_description VARCHAR(256),
    omop_concept_id INTEGER NOT NULL,
    omop_concept_name VARCHAR(256),
    omop_domain_id VARCHAR(50),
    category VARCHAR(50) NOT NULL,
    PRIMARY KEY (source_system, source_code, category)
);
```

### concept
```sql
CREATE TABLE IF NOT EXISTS terminology.concept (
    concept_id INTEGER NOT NULL PRIMARY KEY,
    concept_name VARCHAR(512) NOT NULL,
    domain_id VARCHAR(50) NOT NULL,
    vocabulary_id VARCHAR(50) NOT NULL,
    concept_class_id VARCHAR(50),
    standard_concept VARCHAR(1),
    concept_code VARCHAR(50),
    valid_start_date DATE,
    valid_end_date DATE,
    invalid_reason VARCHAR(1)
);
```

### concept_relationship
```sql
CREATE TABLE IF NOT EXISTS terminology.concept_relationship (
    concept_id_1 INTEGER NOT NULL,
    concept_id_2 INTEGER NOT NULL,
    relationship_id VARCHAR(50) NOT NULL,
    valid_start_date DATE,
    valid_end_date DATE,
    invalid_reason VARCHAR(1),
    PRIMARY KEY (concept_id_1, concept_id_2, relationship_id)
);
```

## OMOP CDM Output Tables (8 tables)

Created by mapper procs in the user-specified output schema (default: `omop_staging`).

### person
| Column | Type | Source |
|--------|------|--------|
| person_id | INTEGER | `ABS(HASH(patient_id)) % 2147483647` |
| gender_concept_id | INTEGER | demographic_to_omop (category='gender') |
| year/month/day_of_birth | INTEGER | Patient.birthDate |
| birth_datetime | TIMESTAMP_NTZ | Patient.birthDate |
| race_concept_id | INTEGER | us-core-race extension → demographic_to_omop |
| ethnicity_concept_id | INTEGER | us-core-ethnicity extension → demographic_to_omop |
| person_source_value | VARCHAR | Patient.id (FHIR UUID) |

### condition_occurrence
| Column | Type | Source |
|--------|------|--------|
| condition_occurrence_id | INTEGER | HASH(Condition.id) |
| person_id | INTEGER | HASH(subject.reference) |
| condition_concept_id | INTEGER | snomed_to_omop lookup |
| condition_start_date | DATE | onsetDateTime / onsetPeriod.start / recordedDate |
| condition_source_value | VARCHAR | Condition.code.coding[].code |

### measurement
| Column | Type | Source |
|--------|------|--------|
| measurement_id | INTEGER | HASH(Observation.id) |
| measurement_concept_id | INTEGER | loinc_to_omop lookup |
| value_as_number | FLOAT | valueQuantity.value |
| unit_source_value | VARCHAR | valueQuantity.unit |
| Filter: `valueQuantity IS NOT NULL` (numeric observations only) |

### visit_occurrence
| Column | Type | Source |
|--------|------|--------|
| visit_concept_id | INTEGER | Hardcoded: AMB→9202, IMP→9201, EMER→9203, HH→581476, VR→5083 |
| visit_start/end_date | DATE | Encounter.period.start/end |
| visit_source_value | VARCHAR | Encounter.class.code |

### drug_exposure
| Column | Type | Source |
|--------|------|--------|
| drug_concept_id | INTEGER | rxnorm_to_omop lookup |
| drug_source_value | VARCHAR | MedicationRequest.medicationCodeableConcept.coding[].code |
| days_supply, quantity, refills | from dispenseRequest |

### procedure_occurrence
| Column | Type | Source |
|--------|------|--------|
| procedure_concept_id | INTEGER | COALESCE(snomed, cpt, hcpcs) — multi-vocabulary lookup |
| procedure_source_value | VARCHAR | Procedure.code.coding[].code |
| code_system used to route to correct crosswalk table |

### observation (qualitative)
| Column | Type | Source |
|--------|------|--------|
| Filter: `valueQuantity IS NULL AND (valueCodeableConcept IS NOT NULL OR valueString IS NOT NULL)` |
| value_as_string | VARCHAR | valueCodeableConcept.text or valueString |

### death
| Column | Type | Source |
|--------|------|--------|
| Filter: Patient where `deceasedDateTime IS NOT NULL OR deceasedBoolean = TRUE` |
| death_date | DATE | Patient.deceasedDateTime |

## Staging Table

### app_state.fhir_resources
Created by `parse_fhir_bundles` — intermediate staging for all extracted FHIR resources.
```sql
CREATE TABLE IF NOT EXISTS app_state.fhir_resources (
    resource_id VARCHAR(256),
    resource_type VARCHAR(100),
    bundle_id VARCHAR(256),
    resource_json VARIANT,
    parsed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```
