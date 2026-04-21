---
name: snowflake-health-data-forge
description: "Build a Snowflake Native App that transforms HL7v2, FHIR R4 (JSON & XML) bundles into OMOP CDM v5.4 or Tuva Input Layer tables. Uses Tuva Health vocabulary patterns (Apache 2.0) + OHDSI Athena crosswalks. Custom OMOP mapping logic + 13 Tuva mappers. Use when: FHIR to OMOP, FHIR to Tuva, HL7v2 to OMOP, HL7v2 to Tuva, healthcare data transformation, clinical data pipeline, OMOP CDM, Tuva Input Layer, FHIR R4 parser, HL7v2 parser, healthcare native app, medical data harmonization, build FHIR app, OMOP mapping, Tuva mapping, vocabulary crosswalks, health data forge, clinical interop, i2b2, unstructured clinical data."
---

# HL7v2/FHIR to OMOP/Tuva Native App Builder

Build a Snowflake Native App that transforms HL7v2 messages and FHIR R4 bundles (JSON & XML) into OMOP CDM v5.4 or Tuva Input Layer tables with a Streamlit UI.

## Architecture Overview

```
HL7v2 Messages / FHIR R4 Bundles (JSON or XML)
  → Auto-detect format (MSH|=HL7v2, <=XML, {=JSON)
  → HL7v2: parse_hl7v2_to_fhir (10 segment converters) → FHIR R4 JSON
  → Parser (parse_fhir_bundles) → app_state.fhir_resources staging
  → Output Format Toggle:
    → OMOP CDM v5.4: 23 Mappers → 17 OMOP tables
    → Tuva Input Layer: 13 Mappers → 13 Tuva tables
  → Vocabulary crosswalks (LOINC, SNOMED, RxNorm, ICD-10, CPT, HCPCS, CVX, Demographics)
```

**Key design decisions:**
- CTE-first pattern for LATERAL FLATTEN — Snowflake does NOT support `LEFT JOIN LATERAL FLATTEN` with ON clause
- `ABS(HASH(id)) % 2147483647` for deterministic integer IDs from FHIR UUIDs
- Python stored procs with inline SQL (not pure SQL procs) for error handling + orchestration
- manifest_version 2 with reference bindings for consumer source table + warehouse
- Dual output: OMOP CDM v5.4 (23 mappers, 17 tables) OR Tuva Input Layer (13 mappers, 13 tables)
- HL7v2 auto-detection: 10 segment converters (PID, PV1, DG1, OBX, OBR, RXA, PR1, AL1, IN1, NK1)
- APPEND pattern for multi-resource → single-table mappings (e.g., MedicationRequest + MedicationAdmin → drug_exposure)

## Prerequisites

- ACCOUNTADMIN role (for Native App packaging)
- A warehouse (X-Small is sufficient — 1,000 bundles processes in ~65 seconds)
- HL7v2, FHIR R4 JSON, or FHIR R4 XML data in a Snowflake table with a VARIANT or VARCHAR column

## Workflow

### Step 1: Gather Requirements

**Ask user:**
1. **App name** — e.g. `MY_FHIR_TO_OMOP_APP`
2. **Database for app package** — e.g. `MY_DB`
3. **FHIR source table** — fully qualified name, e.g. `MY_DB.RAW.FHIR_BUNDLES`
4. **Data column name** — column containing HL7v2/FHIR data (default: `BUNDLE_DATA`; HL7v2 tables often use `RAW_MESSAGE`)
5. **Output format** — `OMOP CDM v5.4` or `Tuva Input Layer` (selectable via radio toggle in UI)
6. **Output schema** — where output tables land (OMOP default: `omop_staging`; Tuva default: `tuva_input`)
7. **Data source label** (Tuva only) — short label stamped on every row, e.g. `fhir`, `claims`, `ehr`

**⚠️ MANDATORY STOP**: Confirm requirements before scaffolding.

### Step 2: Scaffold Project

Create this directory structure:
```
<app_name>/
├── manifest.yml
├── README.md
├── LICENSE
├── scripts/
│   ├── setup.sql              (main entry — creates schemas, roles, loads sub-scripts)
│   ├── setup_seeds.sql        (terminology crosswalk table DDL)
│   ├── seed_vocabulary_data.sql  (starter INSERT data for crosswalks)
│   ├── setup_procs.sql        (23 OMOP mappers + 13 Tuva mappers + HL7v2 parser + orchestrator + quality validator)
│   └── setup_streamlit.sql    (registers Streamlit app)
├── streamlit/
│   ├── main.py                (7-tab Streamlit UI with OMOP/Tuva toggle)
│   ├── environment.yml        (conda YAML — see SiS constraints)
│   └── .streamlit/
│       └── config.toml        (Snowflake theme)
└── tests/
    ├── test_transformation.py (graduated 3-level pipeline test)
    └── test_vocabulary.py     (terminology seed validation)
```

**Load** `references/mapping-patterns.md` for the 9 mapper implementations.

**Actions:**
1. Create `manifest.yml` from `assets/templates/manifest.yml` — customize app name, version label
2. Create `scripts/setup.sql` from `assets/templates/setup.sql` — the main entry point
3. Create `scripts/setup_seeds.sql` — **Load** `references/omop-schema.md` for all 10 terminology table DDLs
4. Create `scripts/seed_vocabulary_data.sql` — starter crosswalk data (LOINC top 50, SNOMED top 50, RxNorm top 30, ICD-10 top 30, demographics 14, concepts, relationships)
5. Create `scripts/setup_procs.sql` — **Load** `references/mapping-patterns.md` for all 9 mappers + orchestrator
6. Create `scripts/setup_streamlit.sql` — single CREATE STREAMLIT statement
7. Create `streamlit/main.py` — 7-tab UI (Configure, Run, History, Vocabulary, Coverage, Quality, Explore)
8. Create `streamlit/environment.yml` from `assets/templates/environment.yml`
9. Create `streamlit/.streamlit/config.toml` from `assets/templates/config.toml`

### Step 3: Build Vocabulary Seeds

Terminology crosswalk tables map source codes to OMOP standard concept IDs.

**Sources:** OHDSI Athena (https://athena.ohdsi.org/), structured per Tuva Health patterns.

**7 crosswalk tables:**
| Table | Maps From | Maps To | Starter Rows |
|-------|-----------|---------|--------------|
| loinc_to_omop | LOINC codes | Measurement concepts | ~50 |
| snomed_to_omop | SNOMED CT | Condition concepts | ~50 |
| rxnorm_to_omop | RxNorm | Drug concepts | ~30 |
| icd10cm_to_omop | ICD-10-CM | Condition concepts | ~30 |
| demographic_to_omop | Gender/Race/Ethnicity | Person concepts | 14 |
| concept | OMOP concept reference | N/A | ~170 |
| concept_relationship | Non-standard → Standard | N/A | ~30 |

**3 additional tables** (DDL only, populated via Athena CSV or custom): `icd10pcs_to_omop`, `cpt_to_omop`, `hcpcs_to_omop`

**For production:** Use `load_seeds.py` (Python stored proc) to bulk-load full Athena CSV exports from a stage.

### Step 4: Build Mapper Procs

**Load** `references/mapping-patterns.md` for implementation details.

#### OMOP CDM v5.4 Pipeline (23 mappers + orchestrator + validator):

| Proc | FHIR Resource | OMOP Table | Crosswalk |
|------|---------------|------------|----------|
| parse_fhir_bundles | Bundle.entry | fhir_resources | N/A (auto-detects JSON/XML/HL7v2) |
| parse_hl7v2_to_fhir | HL7v2 messages | fhir_resources (via FHIR) | N/A |
| map_persons | Patient | person | demographic_to_omop |
| map_conditions | Condition | condition_occurrence | snomed_to_omop |
| map_measurements | Observation (numeric) | measurement | loinc_to_omop |
| map_visits | Encounter | visit_occurrence | hardcoded map |
| map_drug_exposures | MedicationRequest | drug_exposure | rxnorm_to_omop |
| map_procedures | Procedure | procedure_occurrence | snomed/cpt/hcpcs |
| map_observations_qual | Observation (non-numeric) | observation | loinc_to_omop |
| map_death | Patient (deceased) | death | N/A |
| map_immunizations | Immunization | drug_exposure (APPEND) | CVX vocab |
| map_med_administrations | MedicationAdministration | drug_exposure (APPEND) | rxnorm_to_omop |
| map_allergies | AllergyIntolerance | observation (APPEND) | concept 439224 |
| map_devices | Device | device_exposure | SNOMED |
| map_diagnostic_reports | DiagnosticReport | measurement (APPEND) | loinc_to_omop |
| map_imaging_studies | ImagingStudy | procedure_occurrence (APPEND) | SNOMED |
| map_care_plans | CarePlan | observation (APPEND) | concept 4149299 |
| map_locations | Location + Patient.address | location | N/A |
| map_organizations | Organization | care_site | N/A |
| map_practitioners | Practitioner | provider | N/A |
| map_claims | Claim + EOB | cost + payer_plan_period | N/A |
| map_care_teams | CareTeam | fact_relationship | domains 56/58 |
| build_observation_periods | Derived from all events | observation_period | N/A |
| build_cdm_source | Metadata | cdm_source | N/A |
| validate_fhir_quality | All | JSON report (15 checks) | N/A |
| run_full_transformation | Orchestrator | All 17 tables | All |

**17 OMOP output tables:** person, observation_period, condition_occurrence, measurement, visit_occurrence, drug_exposure, procedure_occurrence, device_exposure, observation, death, location, care_site, provider, payer_plan_period, cost, fact_relationship, cdm_source

#### Tuva Input Layer Pipeline (13 mappers):

| Proc | FHIR Resource | Tuva Table | Params |
|------|---------------|------------|--------|
| map_tuva_patient | Patient | patient | (output_schema, data_source) |
| map_tuva_encounter | Encounter | encounter | (output_schema, data_source) |
| map_tuva_condition | Condition | condition | (output_schema, data_source) |
| map_tuva_lab_result | Observation (lab) | lab_result | (output_schema, data_source) |
| map_tuva_observation | Observation (non-lab) | observation | (output_schema, data_source) |
| map_tuva_medication | MedicationRequest + MedicationAdmin | medication | (output_schema, data_source) |
| map_tuva_immunization | Immunization | immunization | (output_schema, data_source) |
| map_tuva_procedure | Procedure | procedure | (output_schema, data_source) |
| map_tuva_location | Location + Organization | location | (output_schema, data_source) |
| map_tuva_practitioner | Practitioner | practitioner | (output_schema, data_source) |
| map_tuva_medical_claim | Claim + EOB | medical_claim | (output_schema, data_source) |
| map_tuva_eligibility | Coverage | eligibility | (output_schema, data_source) |
| map_tuva_appointment | Appointment | appointment | (output_schema, data_source) |

**13 Tuva output tables:** patient, encounter, condition, lab_result, observation, medication, immunization, procedure, location, practitioner, medical_claim, eligibility, appointment

**Critical patterns:**
- ALL mappers use CTE pattern: `WITH flat AS (SELECT ... FROM table, LATERAL FLATTEN(...)) SELECT ... FROM flat LEFT JOIN terminology...`
- Never use `LEFT JOIN LATERAL FLATTEN` with ON clause — Snowflake doesn't support it
- Python runtime 3.11, package `snowflake-snowpark-python`
- OMOP output schema parameterized (default `omop_staging`) — protect production `omop_cdm`
- Tuva mappers take 2 params: `output_schema` (default `tuva_input`) and `data_source` (default `fhir`)
- APPEND pattern: Immunizations/MedAdmin → drug_exposure, Allergies/CarePlan → observation, DiagReports → measurement, ImagingStudy → procedure_occurrence
- Tuva: Medication uses APPEND (MedicationRequest first, then MedicationAdministration); Location uses APPEND (Location first, then Organization)

### Step 5: Build Streamlit UI

**Load** `references/sis-constraints.md` for SiS runtime limitations.

**7 tabs:**
1. **Configure** — DB/schema/table pickers, VARIANT column auto-detect, format auto-detect (JSON/XML/HL7v2), **Output Format radio toggle (OMOP CDM v5.4 / Tuva Input Layer)**, data_source label (Tuva-only), output schema with "create new" option, OMOP_CDM overwrite guard
2. **Run** — Full pipeline button with per-step progress bar + log expander, per-mapper individual buttons (23 OMOP or 13 Tuva depending on format), output summary table
3. **History** — Run history from app_state.run_history
4. **Vocabulary** — Browse/search all 7 crosswalk tables, paginated (50 rows/page)
5. **Coverage** — OMOP: concept_id mapped vs unmapped by domain + top 20 unmapped source codes; Tuva: table-level source_code coverage
6. **Quality** — FHIR quality validator results (15 checks), resource distribution bar chart, severity-coded issues
7. **Explore** — Browse all 17 OMOP or 13 Tuva output tables (format-aware)

**Safety guards:**
- `PROTECTED_SCHEMAS = {'OMOP_CDM'}` — show warning + confirmation checkbox + existing row counts
- Default output to `omop_staging`, not `omop_cdm`

**⚠️ MANDATORY STOP**: Present UI plan for approval before writing main.py.

### Step 6: Deploy and Test

**Deploy to Snowflake:**
```sql
CREATE APPLICATION PACKAGE IF NOT EXISTS <pkg_name>;
CREATE SCHEMA IF NOT EXISTS <pkg_name>.STAGING;
CREATE STAGE IF NOT EXISTS <pkg_name>.STAGING.APP_CODE FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = NONE RECORD_DELIMITER = NONE);
```

PUT all files to stage, then:
```sql
ALTER APPLICATION PACKAGE <pkg_name> REGISTER VERSION v1_0 USING '@<pkg_name>.STAGING.APP_CODE';
CREATE APPLICATION <app_name> FROM APPLICATION PACKAGE <pkg_name>;
```

**Test with graduated pattern (3 levels):**
- **Level 1**: 1 bundle — smoke test (tables exist, ≥1 person, resources parsed)
- **Level 2**: 10 bundles — proportional output, non-zero concept_ids, referential integrity
- **Level 3**: 1,000 bundles — full coverage report, performance benchmark

Run Level 1 first. Must pass before Level 2. **MANDATORY PAUSE** between Level 2 and Level 3.

**⚠️ MANDATORY STOP**: Report Level 1/2 results before Level 3.

### Step 7: Version and Share (Optional)

**Load** `references/deployment.md` for release channel workflow.

When release channels are enabled:
1. `REGISTER VERSION` (not `ADD VERSION`)
2. `MODIFY RELEASE CHANNEL DEFAULT ADD VERSION v1_0`
3. `MODIFY RELEASE CHANNEL DEFAULT SET DEFAULT RELEASE DIRECTIVE VERSION = v1_0 PATCH = 0`

To share with another account:
```sql
ALTER APPLICATION PACKAGE <pkg_name> MODIFY RELEASE CHANNEL DEFAULT ADD ACCOUNT <locator>;
```

Consumer needs: `CREATE APPLICATION ... FROM APPLICATION PACKAGE ...` + GRANT USAGE ON DATABASE/SCHEMA + GRANT SELECT ON TABLE (reference binding alone is insufficient).

## Stopping Points

- ✋ Step 1: Requirements confirmed
- ✋ Step 5: UI plan approved
- ✋ Step 6: Level 1/2 test results reviewed before Level 3
- ✋ Step 7: Before sharing to external accounts

## Output

Complete Native App project with:
- Functional HL7v2/FHIR R4 (JSON & XML) → OMOP CDM v5.4 + Tuva Input Layer dual pipeline
- 7-tab Streamlit UI with Snowflake theme and OMOP/Tuva format toggle
- 10 terminology crosswalk tables with starter data
- 23 OMOP mappers + 13 Tuva mappers + HL7v2 parser + orchestrator + quality validator (36 custom procs)
- Graduated test suite (3 levels)
- Ready for versioned release and cross-account sharing

## Troubleshooting

**"LEFT JOIN LATERAL FLATTEN" fails** → Use CTE pattern: extract via LATERAL FLATTEN in a CTE, then LEFT JOIN the CTE to terminology tables.

**"Unsupported Anaconda feature or malformed environment.yml"** → Must use proper conda YAML format with `name`, `channels`, `dependencies` keys. No `>=` version specifiers.

**"unexpected keyword argument 'icon'"** → SiS runtime doesn't support `icon=` on st.button/st.info. Use plain text labels.

**Tab headers show "/settings: Configure"** → SiS runtime doesn't support `:material/icon_name:` syntax. Use plain text tab labels.

**"Database does not exist or not authorized"** → Consumer must GRANT USAGE ON DATABASE + SCHEMA and GRANT SELECT ON TABLE to the application. Reference binding alone is insufficient.

**manifest_version 2 + DEBUG_MODE** → DEBUG_MODE is not supported in manifest_version 2. Remove it.
