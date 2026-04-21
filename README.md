# Snowflake Health Data Forge

A Snowflake Native App that transforms **HL7v2**, **FHIR R4 (JSON & XML)** clinical data into either **OMOP CDM v5.4** or **Tuva Input Layer** tables.

**Multi-cloud**: Runs on Snowflake across AWS, Azure, and GCP — your data never leaves your account.

## Overview

Health Data Forge is a point-and-click clinical data transformer. Upload HL7v2 messages, FHIR R4 JSON bundles, or FHIR R4 XML bundles, then choose your output format: OMOP CDM v5.4 (standard research model with concept_id mapping) or Tuva Input Layer (analytics-ready tables using source codes directly). The app auto-detects your input format, runs vocabulary crosswalks, and produces clean output tables — all within Snowflake, no external services required.

## Architecture

```
┌───────────────────────────────────────────────────────────────────────┐
│                         Consumer Account                              │
│                                                                       │
│  ┌───────────────┐                                                    │
│  │  HL7v2        │──┐                                                 │
│  │  Messages     │  │                                                 │
│  └───────────────┘  │   ┌──────────────────────────────────────────┐  │
│  ┌───────────────┐  ├──▶│  Snowflake Health Data Forge              │  │
│  │  FHIR R4 JSON │──┤   │                                          │  │
│  │  Bundles      │  │   │  ┌────────────────────────────────────┐  │  │
│  └───────────────┘  │   │  │ Streamlit Config UI                │  │  │
│  ┌───────────────┐  │   │  ├────────────────────────────────────┤  │  │
│  │  FHIR R4 XML  │──┘   │  │ Format Detector (HL7v2/JSON/XML)  │  │  │
│  │  Bundles      │      │  ├────────────────────────────────────┤  │  │
│  └───────────────┘      │  │ FHIR Parser + HL7v2 Converter      │  │  │
│                         │  ├────────────────────────────────────┤  │  │
│                         │  │ Vocabulary Seeds                   │  │  │
│  ┌───────────────┐      │  │ (Tuva/Athena: LOINC, SNOMED,      │  │  │
│  │  OMOP CDM     │◀─┐   │  │  RxNorm, ICD-10, CPT, HCPCS)     │  │  │
│  │  v5.4 Output  │  │   │  ├────────────────────────────────────┤  │  │
│  │  (17 tables)  │  ├───│  │ Output Toggle: OMOP ↔ Tuva        │  │  │
│  └───────────────┘  │   │  └────────────────────────────────────┘  │  │
│  ┌───────────────┐  │   └──────────────────────────────────────────┘  │
│  │  Tuva Input   │◀─┘                                                 │
│  │  Layer Output │                                                    │
│  │  (13 tables)  │                                                    │
│  └───────────────┘                                                    │
└───────────────────────────────────────────────────────────────────────┘
```

## Features

| Feature | Description |
|---------|-------------|
| **Multi-format input** | HL7v2 messages, FHIR R4 JSON bundles, FHIR R4 XML bundles |
| **Auto-detection** | Detects input format automatically (HL7v2 MSH segment, JSON vs XML) |
| **Dual output** | OMOP CDM v5.4 (23 mappers) or Tuva Input Layer (13 mappers) |
| **HL7v2 converter** | Converts 10 segment types (PID, PV1, DG1, OBX, OBR, RXA, PR1, AL1, IN1, NK1) to FHIR resources |
| **Vocabulary crosswalks** | LOINC, SNOMED CT, RxNorm, ICD-10-CM, ICD-10-PCS, CPT, HCPCS, Demographics |
| **Streamlit UI** | Point-and-click configure, run, and explore |
| **Graduated testing** | Level 1 (smoke), Level 2 (10 bundles), Level 3 (full dataset vs ground truth) |
| **Multi-cloud** | AWS, Azure, GCP — pure Snowflake SQL + Python stored procedures |

## Output Tables

### OMOP CDM v5.4 (23 mappers)

| OMOP Table | FHIR Source | Vocabulary Crosswalk |
|---|---|---|
| `person` | Patient | Gender, Race, Ethnicity → OMOP concept |
| `condition_occurrence` | Condition | SNOMED CT → OMOP concept |
| `measurement` | Observation (quantitative) | LOINC → OMOP concept |
| `visit_occurrence` | Encounter | HL7 ActCode → OMOP visit concept |
| `drug_exposure` | MedicationRequest | RxNorm → OMOP concept |
| `procedure_occurrence` | Procedure | SNOMED/CPT/HCPCS → OMOP concept |
| `observation` | Observation (qualitative) | LOINC → OMOP concept |
| `death` | Patient (deceased) | — |
| `drug_exposure` (admin) | MedicationAdministration | RxNorm → OMOP concept |
| `drug_exposure` (immunization) | Immunization | CVX → OMOP concept |
| `observation` (allergy) | AllergyIntolerance | SNOMED → OMOP concept |
| `device_exposure` | Device | SNOMED → OMOP concept |
| `measurement` (diagnostic) | DiagnosticReport | LOINC → OMOP concept |
| `procedure_occurrence` (imaging) | ImagingStudy | DICOM → OMOP concept |
| `observation` (care plan) | CarePlan | SNOMED → OMOP concept |
| `location` | Location | — |
| `care_site` | Organization | — |
| `provider` | Practitioner | — |
| `payer_plan_period` / `cost` | Claim, ExplanationOfBenefit | — |
| `observation` (care team) | CareTeam | — |
| `observation_period` | (derived from all events) | — |
| `cdm_source` | (metadata) | — |

### Tuva Input Layer (13 mappers)

| Tuva Table | FHIR Source |
|---|---|
| `patient` | Patient |
| `encounter` | Encounter |
| `condition` | Condition |
| `lab_result` | Observation (quantitative) |
| `observation` | Observation (qualitative) |
| `medication` | MedicationRequest |
| `immunization` | Immunization |
| `procedure` | Procedure |
| `location` | Location |
| `practitioner` | Practitioner |
| `medical_claim` | Claim |
| `eligibility` | Coverage |
| `appointment` | Encounter (scheduled) |

## Installation

### From Snowflake Marketplace

1. Search for **"Health Data Forge"** in the Snowflake Marketplace
2. Click **Get** to install
3. Grant the requested privileges (CREATE DATABASE, EXECUTE TASK)
4. Open the Streamlit UI from the installed app

### Manual Deployment (Development)

```sql
CREATE APPLICATION PACKAGE tuva_fhir_to_omop_pkg;
CREATE SCHEMA tuva_fhir_to_omop_pkg.staging;
CREATE STAGE tuva_fhir_to_omop_pkg.staging.app_code;

-- Upload files
PUT file://manifest.yml @tuva_fhir_to_omop_pkg.staging.app_code/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://scripts/* @tuva_fhir_to_omop_pkg.staging.app_code/scripts/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://streamlit/* @tuva_fhir_to_omop_pkg.staging.app_code/streamlit/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://README.md @tuva_fhir_to_omop_pkg.staging.app_code/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- With release channels
ALTER APPLICATION PACKAGE tuva_fhir_to_omop_pkg
    REGISTER VERSION V1_6
    USING '@tuva_fhir_to_omop_pkg.staging.app_code'
    LABEL = 'Snowflake Health Data Forge v1.6';

-- Create app from version
CREATE APPLICATION tuva_fhir_to_omop_app
    FROM APPLICATION PACKAGE tuva_fhir_to_omop_pkg
    USING VERSION V1_6;
```

## Usage

### 1. Configure

Open the Streamlit UI → **Configure** tab:
- Select your source database, schema, and table
- The app auto-detects the input format (HL7v2, FHIR JSON, FHIR XML)
- Choose output format: **OMOP CDM** or **Tuva Input Layer**
- Set the output schema name (default: `omop_staging` or `tuva_input`)
- Click **Save Configuration**

### 2. Run Transformation

**Run Transformation** tab:
- Click **Run Full Transformation** for the complete pipeline
- Or run individual mappers (Parse, Persons, Conditions, Measurements, etc.)
- Progress bar shows per-mapper status

### 3. Explore Results

**Explore** tab to browse the generated output tables. The explore view is format-aware — it shows OMOP tables or Tuva tables depending on your output choice.

### 4. Programmatic Access

```sql
-- Full OMOP transformation
CALL core.run_full_transformation(
    'MY_DB.MY_SCHEMA.FHIR_BUNDLES',
    'BUNDLE_DATA',
    'omop_staging',
    'omop'
);

-- Full Tuva transformation
CALL core.run_full_transformation(
    'MY_DB.MY_SCHEMA.FHIR_BUNDLES',
    'BUNDLE_DATA',
    'tuva_input',
    'tuva'
);

-- Individual OMOP mappers
CALL core.parse_fhir_bundles('MY_DB.MY_SCHEMA.FHIR_BUNDLES', 'BUNDLE_DATA');
CALL core.map_persons('omop_staging');
CALL core.map_conditions('omop_staging');
CALL core.map_measurements('omop_staging');
CALL core.map_visits('omop_staging');
CALL core.map_drug_exposures('omop_staging');
CALL core.map_procedures('omop_staging');
CALL core.map_observations_qual('omop_staging');
CALL core.map_death('omop_staging');
CALL core.map_immunizations('omop_staging');
CALL core.map_med_administrations('omop_staging');
CALL core.map_allergies('omop_staging');
CALL core.map_devices('omop_staging');
CALL core.map_diagnostic_reports('omop_staging');
CALL core.map_imaging_studies('omop_staging');
CALL core.map_care_plans('omop_staging');
CALL core.map_locations('omop_staging');
CALL core.map_organizations('omop_staging');
CALL core.map_practitioners('omop_staging');
CALL core.map_claims('omop_staging');
CALL core.map_care_teams('omop_staging');
CALL core.build_observation_periods('omop_staging');
CALL core.build_cdm_source('omop_staging');

-- Individual Tuva mappers
CALL core.map_tuva_patient('tuva_input', 'fhir');
CALL core.map_tuva_encounter('tuva_input', 'fhir');
CALL core.map_tuva_condition('tuva_input', 'fhir');
CALL core.map_tuva_lab_result('tuva_input', 'fhir');
CALL core.map_tuva_observation('tuva_input', 'fhir');
CALL core.map_tuva_medication('tuva_input', 'fhir');
CALL core.map_tuva_immunization('tuva_input', 'fhir');
CALL core.map_tuva_procedure('tuva_input', 'fhir');
CALL core.map_tuva_location('tuva_input', 'fhir');
CALL core.map_tuva_practitioner('tuva_input', 'fhir');
CALL core.map_tuva_medical_claim('tuva_input', 'fhir');
CALL core.map_tuva_eligibility('tuva_input', 'fhir');
CALL core.map_tuva_appointment('tuva_input', 'fhir');

-- HL7v2 conversion (converts to FHIR resources first, then map)
CALL core.parse_hl7v2_to_fhir('MY_DB.MY_SCHEMA.HL7V2_MESSAGES', 'MESSAGE_DATA');
```

## Input Requirements

### FHIR R4 JSON
```sql
CREATE TABLE my_data.staging.fhir_bundles (
    BUNDLE_ID VARCHAR(256),
    BUNDLE_DATA VARIANT     -- FHIR R4 Bundle JSON
);
```

### FHIR R4 XML
```sql
CREATE TABLE my_data.staging.fhir_xml (
    BUNDLE_ID VARCHAR(256),
    BUNDLE_DATA VARCHAR     -- FHIR R4 Bundle XML string
);
```

### HL7v2
```sql
CREATE TABLE my_data.staging.hl7v2_messages (
    MESSAGE_ID VARCHAR(256),
    MESSAGE_DATA VARCHAR    -- HL7v2 pipe-delimited message
);
```

## Vocabulary / Terminology Coverage

Bundled crosswalk tables (sourced from OHDSI Athena, structured following Tuva Health seed patterns):

| Vocabulary   | Mapping Direction        | Approx. Records |
|-------------|--------------------------|-----------------|
| LOINC       | LOINC → OMOP Measurement | ~100K           |
| SNOMED CT   | SNOMED → OMOP Condition  | ~200K           |
| RxNorm      | RxNorm → OMOP Drug       | ~150K           |
| ICD-10-CM   | ICD-10 → OMOP Condition  | ~95K            |
| ICD-10-PCS  | ICD-10 → OMOP Procedure  | ~80K            |
| CPT         | CPT → OMOP Procedure     | ~15K            |
| HCPCS       | HCPCS → OMOP             | ~8K             |
| Demographics| Gender/Race/Ethnicity     | ~50             |

## Testing

Graduated test pattern with 3 levels:

```bash
# Level 1: Smoke test (1 bundle, ~30s)
SNOWFLAKE_CONNECTION_NAME=HealthcareDemos python tests/test_transformation.py --level 1

# Level 2: 10 bundles with coverage validation (~2min)
SNOWFLAKE_CONNECTION_NAME=HealthcareDemos python tests/test_transformation.py --level 2

# Level 3: Full 1,000 bundles vs ground truth (~10min)
SNOWFLAKE_CONNECTION_NAME=HealthcareDemos python tests/test_transformation.py --level 3

# Vocabulary validation
SNOWFLAKE_CONNECTION_NAME=HealthcareDemos python tests/test_vocabulary.py
```

## Version History

| Version | Date | Changes |
|---------|------|---------|
| V1.6 | 2026-04-21 | Tuva Input Layer output (13 mappers), skill rename to snowflake-health-data-forge |
| V1.5 | 2026-04-19 | HL7v2 support (10 segment converters), full OMOP coverage (23 mappers), per-mapper progress UI |
| V1.4 | 2026-04-17 | V1.2+ OMOP mappers (immunizations through cdm_source), FHIR quality validator |
| V1.2 | 2026-04-15 | Procedures, qualitative observations, death mapper, Snowflake theme |
| V1.0 | 2026-04-12 | Initial: FHIR R4 JSON → 5 OMOP tables (person, condition, measurement, visit, drug) |

See [CHANGELOG.md](CHANGELOG.md) for detailed changes.

## What Tuva Provides vs. What This App Builds

| Component | Source | Notes |
|-----------|--------|-------|
| Vocabulary/terminology seed tables | [Tuva Health](https://thetuvaproject.com/) (Apache 2.0) + [OHDSI Athena](https://athena.ohdsi.org/) | LOINC, SNOMED, RxNorm, ICD-10 crosswalk tables |
| FHIR JSON flattening patterns | Inspired by [FHIR Inferno](https://github.com/tuva-health/FHIR_inferno) (Tuva, Apache 2.0) | Adapted for Snowflake VARIANT/LATERAL FLATTEN |
| OMOP CDM table definitions | [OHDSI CDM v5.4](https://ohdsi.github.io/CommonDataModel/) | 17 clinical + infrastructure tables |
| Tuva Input Layer definitions | [Tuva Health](https://thetuvaproject.com/) (Apache 2.0) | 13 input layer tables |
| FHIR/HL7v2 → OMOP mapping logic | **This project (original)** | 23 Python stored procs |
| FHIR/HL7v2 → Tuva mapping logic | **This project (original)** | 13 Python stored procs |
| HL7v2 → FHIR converter | **This project (original)** | 10 segment type converters |
| Streamlit configuration UI | **This project (original)** | Format detection, output toggle, per-mapper progress |
| Orchestration & audit trail | **This project (original)** | Run history, error tracking, FHIR quality validation |

## Cloud Support

| Cloud Provider | Status |
|---------------|--------|
| AWS           | Supported |
| Azure         | Supported |
| GCP           | Supported |

No cloud-specific features — pure Snowflake SQL + Python stored procedures.

## Application Roles

| Role        | Access Level                                     |
|-------------|--------------------------------------------------|
| `app_admin` | Full access: configure, run transformations, view |
| `app_user`  | Read-only: view terminology, explore output       |

## CoCo Skill

This project includes a [Cortex Code (CoCo)](https://docs.snowflake.com/en/user-guide/cortex-code/) skill at `skill/` that automates building, deploying, and extending the app. Install to `~/.snowflake/cortex/skills/snowflake-health-data-forge/`.

## Credits & Acknowledgments

- **[Tuva Health](https://thetuvaproject.com/)** (Apache 2.0) — Vocabulary/terminology seed table structure, FHIR parsing patterns, and Tuva Input Layer table definitions.
- **[OHDSI Athena](https://athena.ohdsi.org/)** — Source vocabulary data for concept crosswalks (LOINC, SNOMED, RxNorm, ICD-10, CPT, HCPCS → OMOP concept IDs).
- **[OHDSI CDM](https://ohdsi.github.io/CommonDataModel/)** — OMOP Common Data Model v5.4 specification.
- **[HL7 FHIR](https://www.hl7.org/fhir/)** — Fast Healthcare Interoperability Resources R4 standard.
- **[HL7v2](https://www.hl7.org/implement/standards/product_brief.cfm?product_id=185)** — Health Level Seven Version 2 messaging standard.

## License

Apache License 2.0 — See [LICENSE](LICENSE) for details.

## Project Structure

```
tuva-fhir-to-omop-app/
├── manifest.yml                    # Native App manifest
├── README.md                       # This file
├── CHANGELOG.md                    # Version history
├── LICENSE                         # Apache 2.0
├── scripts/
│   ├── setup.sql                   # Primary setup script
│   ├── setup_seeds.sql             # Vocabulary/terminology tables
│   ├── setup_procs.sql             # 39 procs (23 OMOP + 13 Tuva + 3 orchestration)
│   ├── setup_streamlit.sql         # Streamlit UI registration
│   ├── deploy_dev.sql              # Dev-mode deployment helper
│   ├── load_seeds.py               # Vocabulary data loader
│   └── seed_vocabulary_data.sql    # Vocabulary INSERT statements
├── streamlit/
│   ├── main.py                     # Streamlit UI (configure, run, explore)
│   └── environment.yml             # Python dependencies
├── seeds/                          # Vocabulary data files (populated at build)
├── tests/
│   ├── test_transformation.py      # Graduated pipeline test (levels 1-3)
│   └── test_vocabulary.py          # Terminology table validation
├── docs/
│   ├── install-guide.html          # Consumer installation guide
│   └── showcase.html               # Feature showcase
└── skill/                          # CoCo skill (snowflake-health-data-forge)
    ├── SKILL.md                    # Skill definition
    └── references/
        ├── mapping-patterns.md     # Mapper implementation patterns
        ├── deployment.md           # Native App deployment guide
        ├── omop-schema.md          # OMOP CDM schema reference
        └── sis-constraints.md      # Snowflake SiS constraints
```
