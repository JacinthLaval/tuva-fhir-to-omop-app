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

## Orchestrator (run_full_transformation)

Runs all mappers in sequence with error handling:
1. Parse bundles
2. Run all 8 mappers (each in try/except, collect errors)
3. Count output rows per table
4. Record run in `app_state.run_history` with counts + status

## Quality Validator (validate_fhir_quality)

Read-only diagnostic. Returns JSON with:
- `total_bundles` count
- `resources_by_type` breakdown
- `quality_issues` array: Patient missing birthDate, Condition missing code, Observation missing value, Encounter missing period.start, MedicationRequest missing medication, Procedure missing code, Patient missing id

## Common Type Concept IDs

- `32817` = "EHR" (used as default type_concept_id for all domains)
- Visit: AMB=9202, IMP=9201, EMER=9203, HH=581476, FLD=38004193, VR=5083
- `0` = "No matching concept" (unmapped)
