---
name: fhir-to-omop-native-app
description: "Build a Snowflake Native App that transforms FHIR R4 bundles into OMOP CDM v5.4 tables. Uses Tuva Health vocabulary patterns (Apache 2.0) + OHDSI Athena crosswalks. Custom OMOP mapping logic. Use when: FHIR to OMOP, healthcare data transformation, clinical data pipeline, OMOP CDM, FHIR R4 parser, healthcare native app, medical data harmonization, build FHIR app, OMOP mapping, vocabulary crosswalks."
---

# FHIR-to-OMOP Native App Builder

Build a Snowflake Native App that transforms FHIR R4 JSON bundles into OMOP CDM v5.4 clinical tables with a Streamlit UI.

## Architecture Overview

```
FHIR R4 Bundles (VARIANT) → Parser → fhir_resources staging
  → 9 Mappers (Person, Condition, Measurement, Visit, Drug, Procedure, Observation, Death, Quality)
  → OMOP CDM v5.4 tables (person, condition_occurrence, measurement, visit_occurrence, drug_exposure, procedure_occurrence, observation, death)
  → Vocabulary crosswalks (LOINC, SNOMED, RxNorm, ICD-10, CPT, HCPCS, Demographics)
```

**Key design decisions:**
- CTE-first pattern for LATERAL FLATTEN — Snowflake does NOT support `LEFT JOIN LATERAL FLATTEN` with ON clause
- `ABS(HASH(id)) % 2147483647` for deterministic integer IDs from FHIR UUIDs
- Python stored procs with inline SQL (not pure SQL procs) for error handling + orchestration
- manifest_version 2 with reference bindings for consumer source table + warehouse

## Prerequisites

- ACCOUNTADMIN role (for Native App packaging)
- A warehouse (X-Small is sufficient — 1,000 bundles processes in ~65 seconds)
- FHIR R4 data in a Snowflake table with a VARIANT column containing JSON bundles

## Workflow

### Step 1: Gather Requirements

**Ask user:**
1. **App name** — e.g. `MY_FHIR_TO_OMOP_APP`
2. **Database for app package** — e.g. `MY_DB`
3. **FHIR source table** — fully qualified name, e.g. `MY_DB.RAW.FHIR_BUNDLES`
4. **VARIANT column name** — column containing FHIR JSON (default: `BUNDLE_DATA`)
5. **Output schema** — where OMOP tables land (default: `omop_staging`, use `omop_cdm` for production)
6. **Additional FHIR resource types** — beyond the standard 7 (Patient, Condition, Observation, Encounter, MedicationRequest, Procedure, death from Patient)

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
│   ├── setup_procs.sql        (9 mapper procs + orchestrator + quality validator)
│   └── setup_streamlit.sql    (registers Streamlit app)
├── streamlit/
│   ├── main.py                (7-tab Streamlit UI)
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

**9 mappers + 1 orchestrator + 1 validator:**

| Proc | FHIR Resource | OMOP Table | Crosswalk |
|------|---------------|------------|-----------|
| parse_fhir_bundles | Bundle.entry | fhir_resources | N/A |
| map_persons | Patient | person | demographic_to_omop |
| map_conditions | Condition | condition_occurrence | snomed_to_omop |
| map_measurements | Observation (numeric) | measurement | loinc_to_omop |
| map_visits | Encounter | visit_occurrence | hardcoded map |
| map_drug_exposures | MedicationRequest | drug_exposure | rxnorm_to_omop |
| map_procedures | Procedure | procedure_occurrence | snomed/cpt/hcpcs |
| map_observations_qual | Observation (non-numeric) | observation | loinc_to_omop |
| map_death | Patient (deceased) | death | N/A |
| validate_fhir_quality | All | JSON report | N/A |
| run_full_transformation | Orchestrator | All tables | All |

**Critical patterns:**
- ALL mappers use CTE pattern: `WITH flat AS (SELECT ... FROM table, LATERAL FLATTEN(...)) SELECT ... FROM flat LEFT JOIN terminology...`
- Never use `LEFT JOIN LATERAL FLATTEN` with ON clause — Snowflake doesn't support it
- Python runtime 3.11, package `snowflake-snowpark-python`
- Output schema parameterized (default `omop_staging`) — protect production `omop_cdm`

### Step 5: Build Streamlit UI

**Load** `references/sis-constraints.md` for SiS runtime limitations.

**7 tabs:**
1. **Configure** — DB/schema/table pickers, VARIANT column selector, output schema with "create new" option, OMOP_CDM overwrite guard
2. **Run** — Full transformation button, per-mapper buttons, progress display
3. **History** — Run history from app_state.run_history
4. **Vocabulary** — Browse/search all 7 crosswalk tables, paginated
5. **Coverage** — Mapped vs unmapped rates by domain, top unmapped codes
6. **Quality** — FHIR quality validator results, resource distribution, issues
7. **Explore** — Browse all 8 OMOP output tables

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
- Functional FHIR R4 → OMOP CDM v5.4 pipeline
- 7-tab Streamlit UI with Snowflake theme
- 10 terminology crosswalk tables with starter data
- 9 mapping procs + orchestrator + quality validator
- Graduated test suite
- Ready for versioned release and cross-account sharing

## Troubleshooting

**"LEFT JOIN LATERAL FLATTEN" fails** → Use CTE pattern: extract via LATERAL FLATTEN in a CTE, then LEFT JOIN the CTE to terminology tables.

**"Unsupported Anaconda feature or malformed environment.yml"** → Must use proper conda YAML format with `name`, `channels`, `dependencies` keys. No `>=` version specifiers.

**"unexpected keyword argument 'icon'"** → SiS runtime doesn't support `icon=` on st.button/st.info. Use plain text labels.

**Tab headers show "/settings: Configure"** → SiS runtime doesn't support `:material/icon_name:` syntax. Use plain text tab labels.

**"Database does not exist or not authorized"** → Consumer must GRANT USAGE ON DATABASE + SCHEMA and GRANT SELECT ON TABLE to the application. Reference binding alone is insufficient.

**manifest_version 2 + DEBUG_MODE** → DEBUG_MODE is not supported in manifest_version 2. Remove it.
