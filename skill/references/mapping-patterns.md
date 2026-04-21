# FHIR-to-OMOP Mapping Patterns

## CTE-First Pattern (CRITICAL)

Snowflake does NOT support `LEFT JOIN LATERAL FLATTEN` with an ON clause. Every mapper must use this pattern:

```sql
WITH flat_cte AS (
    SELECT
        r.resource_json:id::VARCHAR AS id,
        -- extract fields from FHIR JSON
        coded.value:code::VARCHAR AS source_code
    FROM app_state.fhir_resources r,
        LATERAL FLATTEN(input => r.resource_json:code:coding, OUTER => TRUE) coded
    WHERE r.resource_type = '<ResourceType>'
)
SELECT
    ABS(HASH(f.id)) % 2147483647 AS <omop_id>,
    COALESCE(xw.omop_concept_id, 0) AS <concept_id>,
    -- other fields
FROM flat_cte f
LEFT JOIN terminology.<crosswalk_table> xw
    ON xw.<code_column> = f.source_code
```

**Key rules:**
- LATERAL FLATTEN goes in the CTE with a comma join (not JOIN ... ON)
- Use `OUTER => TRUE` to keep resources with no coded values
- LEFT JOIN to terminology happens OUTSIDE the CTE
- `COALESCE(..., 0)` for unmapped concepts (0 = "No matching concept")

## ID Generation

All OMOP integer IDs from FHIR UUIDs:
```sql
ABS(HASH(fhir_uuid)) % 2147483647
```
Deterministic, repeatable, fits INTEGER range.

## Parser (parse_fhir_bundles)

3 parameters: `source_table`, `json_column` (default 'BUNDLE_DATA'), `bundle_id_column` (default 'BUNDLE_ID')

Extracts all resources from FHIR bundles into `app_state.fhir_resources`:
```sql
INSERT INTO app_state.fhir_resources (resource_id, resource_type, bundle_id, resource_json)
SELECT
    r.value:resource:id::VARCHAR,
    r.value:resource:resourceType::VARCHAR,
    src.<bundle_id_column>,
    r.value:resource
FROM <source_table> src,
    LATERAL FLATTEN(input => src.<json_column>:entry) r
WHERE r.value:resource:resourceType IS NOT NULL
```

## Person Mapper (map_persons)

FHIR Patient → OMOP person. Three CTEs for extensions:

1. `patients` — base patient fields (id, gender, birthDate)
2. `race_ext` — LATERAL FLATTEN on extensions, filter `url LIKE '%us-core-race'`, extract `valueCoding:code`
3. `eth_ext` — same for `%us-core-ethnicity`

Then LEFT JOIN all three CTEs + three `demographic_to_omop` lookups (gender, race, ethnicity by `category` column).

## Condition Mapper (map_conditions)

FHIR Condition → OMOP condition_occurrence.

CTE `cond_flat`: LATERAL FLATTEN on `resource_json:code:coding`. Date priority: `onsetDateTime` → `onsetPeriod:start` → `recordedDate`.

LEFT JOIN `terminology.snomed_to_omop` on source_code.

## Measurement Mapper (map_measurements)

FHIR Observation (numeric only) → OMOP measurement.

Filter: `resource_type = 'Observation' AND resource_json:valueQuantity IS NOT NULL`

CTE `obs_flat`: LATERAL FLATTEN on `resource_json:code:coding`. Extract `valueQuantity:value`, `referenceRange[0]:low/high`.

LEFT JOIN `terminology.loinc_to_omop`.

## Visit Mapper (map_visits)

FHIR Encounter → OMOP visit_occurrence. No LATERAL FLATTEN needed.

Hardcoded visit type map:
```python
{'AMB': 9202, 'IMP': 9201, 'EMER': 9203, 'HH': 581476, 'FLD': 38004193, 'VR': 5083, 'SS': 9202}
```
Build as SQL CASE expression from `resource_json:class:code`.

## Drug Exposure Mapper (map_drug_exposures)

FHIR MedicationRequest → OMOP drug_exposure.

CTE `med_flat`: LATERAL FLATTEN on `resource_json:medicationCodeableConcept:coding`.

Extract from `dispenseRequest`: `numberOfRepeatsAllowed` (refills), `quantity:value`, `expectedSupplyDuration:value` (days_supply), `validityPeriod` (dates).

LEFT JOIN `terminology.rxnorm_to_omop`.

## Procedure Mapper (map_procedures)

FHIR Procedure → OMOP procedure_occurrence. Multi-vocabulary lookup.

CTE `proc_flat`: LATERAL FLATTEN on `resource_json:code:coding`. Also extract `value:system` as `code_system`.

Three LEFT JOINs — route by code_system:
```sql
LEFT JOIN terminology.snomed_to_omop sm ON sm.snomed_code = pf.source_code AND pf.code_system LIKE '%snomed%'
LEFT JOIN terminology.cpt_to_omop cpt ON cpt.cpt_code = pf.source_code AND pf.code_system LIKE '%cpt%'
LEFT JOIN terminology.hcpcs_to_omop hc ON hc.hcpcs_code = pf.source_code AND pf.code_system LIKE '%hcpcs%'
```
concept_id = `COALESCE(sm.omop_concept_id, cpt.omop_concept_id, hc.omop_concept_id, 0)`

## Observation (Qualitative) Mapper (map_observations_qual)

FHIR Observation (non-numeric) → OMOP observation.

Filter: `valueQuantity IS NULL AND (valueCodeableConcept IS NOT NULL OR valueString IS NOT NULL)`

value_as_string = `COALESCE(valueCodeableConcept:text, valueCodeableConcept:coding[0]:display, valueString)`

LEFT JOIN `terminology.loinc_to_omop`.

## Death Mapper (map_death)

FHIR Patient (deceased) → OMOP death. No LATERAL FLATTEN needed.

Filter: `resource_type = 'Patient' AND (deceasedDateTime IS NOT NULL OR deceasedBoolean = TRUE)`

## HL7v2 Parser (parse_hl7v2_to_fhir)

3 parameters: `source_table`, `message_column` (default 'RAW_MESSAGE'), `message_id_column` (default 'MESSAGE_ID')

Pure Python, zero dependencies. Auto-detected when first chars are `MSH|`.

10 segment converters:
- PID → Patient
- PV1 → Encounter
- DG1 → Condition
- OBX → Observation
- OBR → DiagnosticReport
- RXA → MedicationAdministration
- PR1 → Procedure
- AL1 → AllergyIntolerance
- IN1 → Coverage
- NK1 → RelatedPerson

Converts HL7v2 → FHIR R4 JSON bundles → inserts into `app_state.fhir_resources`. Uses batch INSERT with UNION ALL (500 per batch).

## Additional OMOP Mappers (V1.2+)

### Immunizations (map_immunizations)
FHIR Immunization → OMOP drug_exposure (APPEND). Uses CVX vocabulary. `CREATE TABLE IF NOT EXISTS` + `INSERT INTO` pattern.

### Med Administrations (map_med_administrations)
FHIR MedicationAdministration → OMOP drug_exposure (APPEND). Uses rxnorm_to_omop.

### Allergies (map_allergies)
FHIR AllergyIntolerance → OMOP observation (APPEND). Hardcoded concept 439224 (Allergy).

### Devices (map_devices)
FHIR Device → OMOP device_exposure. Uses SNOMED crosswalk.

### Diagnostic Reports (map_diagnostic_reports)
FHIR DiagnosticReport → OMOP measurement (APPEND). Uses loinc_to_omop.

### Imaging Studies (map_imaging_studies)
FHIR ImagingStudy → OMOP procedure_occurrence (APPEND). Uses SNOMED.

### Care Plans (map_care_plans)
FHIR CarePlan → OMOP observation (APPEND). Hardcoded concept 4149299.

### Locations (map_locations)
FHIR Location + Patient.address → OMOP location. APPEND pattern (Location first, then extract from Patient addresses).

### Organizations (map_organizations)
FHIR Organization → OMOP care_site.

### Practitioners (map_practitioners)
FHIR Practitioner → OMOP provider. Extracts NPI from identifiers.

### Claims (map_claims)
FHIR Claim + ExplanationOfBenefit → OMOP cost + payer_plan_period. Uses LATERAL FLATTEN on items for line-level costs.

### Care Teams (map_care_teams)
FHIR CareTeam → OMOP fact_relationship. Links Person (domain 56) to Provider (domain 58).

### Observation Periods (build_observation_periods)
Derived REQUIRED table. Scans all clinical event tables for min/max dates per person.

### CDM Source (build_cdm_source)
Required metadata table. Records CDM version, source description, vocabulary version.

## Tuva Input Layer Mapper Patterns

All 13 Tuva mappers follow the same signature: `(output_schema VARCHAR DEFAULT 'tuva_staging', data_source VARCHAR DEFAULT 'fhir')`

All read from `app_state.fhir_resources` and use the CTE pattern. Key differences from OMOP:
- No concept_id mapping — Tuva uses source codes directly
- `data_source` column stamped on every row for lineage tracking
- Lab vs non-lab split: `map_tuva_lab_result` filters `category='laboratory' OR loinc system AND valueQuantity IS NOT NULL`; `map_tuva_observation` filters NOT laboratory
- APPEND patterns: Medication (MedicationRequest + MedicationAdmin), Location (Location + Organization), Medical Claim (Claim + EOB with LATERAL FLATTEN on items)
- Appointment uses 3 CTEs for participant extraction (patient, practitioner, location)

## Orchestrator (run_full_transformation)

Runs all mappers in sequence with error handling. Format-aware:
- **OMOP mode**: Parse → 21 OMOP mappers → CDM source (23 steps total)
- **Tuva mode**: Parse → 13 Tuva mappers (14 steps total)

Per-step progress bar + log expander in Streamlit UI. Each step in try/except, collects errors.
Records run in `app_state.run_history` with counts + status.

## Quality Validator (validate_fhir_quality)

Read-only diagnostic. Returns JSON with:
- `total_bundles` count
- `resources_by_type` breakdown
- `quality_issues` array: Patient missing birthDate, Condition missing code, Observation missing value, Encounter missing period.start, MedicationRequest missing medication, Procedure missing code, Patient missing id

## Common Type Concept IDs

- `32817` = "EHR" (used as default type_concept_id for all domains)
- Visit: AMB=9202, IMP=9201, EMER=9203, HH=581476, FLD=38004193, VR=5083
- `0` = "No matching concept" (unmapped)
- `439224` = Allergy (used for AllergyIntolerance → observation)
- `4149299` = Care plan (used for CarePlan → observation)
- `44818668` = USD currency (used in cost table)
- `44818821` = "Has care provider" (used in fact_relationship for CareTeam)
- `756265` = CDM v5.4 (used in cdm_source)
- Domain 56 = Person, Domain 58 = Provider (used in fact_relationship)
