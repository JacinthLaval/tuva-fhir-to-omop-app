# Tuva FHIR-to-OMOP Transformer

A Snowflake Native App that transforms **FHIR R4** bundles into the **OMOP Common Data Model (CDM)**.

**Multi-cloud**: Runs on Snowflake across AWS, Azure, and GCP — your data never leaves your account.

## How It Works

This app has three distinct layers:

1. **FHIR Parsing** — Extracts individual resources (Patient, Condition, Observation, Encounter, MedicationRequest) from FHIR R4 Bundle JSON using Snowflake's native semi-structured data handling.

2. **Vocabulary / Terminology Crosswalks** — Bundled lookup tables that map source coding systems (LOINC, SNOMED CT, RxNorm, ICD-10-CM, ICD-10-PCS, CPT, HCPCS) to OMOP standard concept IDs. These vocabulary seeds are sourced from [OHDSI Athena](https://athena.ohdsi.org/) and structured following patterns established by the [Tuva Health](https://thetuvaproject.com/) open-source project (Apache 2.0).

3. **OMOP CDM Mapping** — Custom-built transformation logic (Python stored procedures) that maps parsed FHIR resources + vocabulary lookups into OMOP CDM v5.4 tables. This mapping layer is original to this project — it is not produced by Tuva.

### What Tuva Provides vs. What This App Builds

| Component | Source | Notes |
|-----------|--------|-------|
| Vocabulary/terminology seed tables | [Tuva Health](https://thetuvaproject.com/) (Apache 2.0) + [OHDSI Athena](https://athena.ohdsi.org/) | LOINC, SNOMED, RxNorm, ICD-10 crosswalk tables |
| FHIR JSON flattening patterns | Inspired by [FHIR Inferno](https://github.com/tuva-health/FHIR_inferno) (Tuva, Apache 2.0) | Adapted for Snowflake VARIANT/LATERAL FLATTEN |
| OMOP CDM table definitions | [OHDSI CDM v5.4](https://ohdsi.github.io/CommonDataModel/) | person, condition_occurrence, measurement, visit_occurrence, drug_exposure |
| FHIR → OMOP mapping logic | **This project (original)** | Python stored procs mapping FHIR resources to OMOP tables via vocabulary lookups |
| Streamlit configuration UI | **This project (original)** | Point-and-click setup, run, and explore |
| Orchestration & audit trail | **This project (original)** | Run history, error tracking, incremental refresh |

> **Important**: Tuva Health's own output is the [Tuva Core Data Model](https://thetuvaproject.com/core-data-model) — a distinct, non-OMOP data model with its own table structure (appointment, condition, encounter, lab_result, etc.). This app does **not** produce Tuva's core model. It produces OMOP CDM tables, using Tuva's vocabulary assets for code translation.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                      Consumer Account                         │
│                                                               │
│  ┌──────────────┐    ┌──────────────────────────────────┐    │
│  │  FHIR R4     │───▶│  Tuva FHIR-to-OMOP App           │    │
│  │  Bundles DB  │    │                                   │    │
│  └──────────────┘    │  ┌──────────────────────────────┐ │    │
│                      │  │ Streamlit Config UI           │ │    │
│                      │  ├──────────────────────────────┤ │    │
│  ┌──────────────┐    │  │ FHIR Parser (Snowflake SQL)  │ │    │
│  │  OMOP CDM    │◀───│  ├──────────────────────────────┤ │    │
│  │  Output      │    │  │ OMOP Mapper (Python Procs)   │ │    │
│  │  (v5.4)      │    │  ├──────────────────────────────┤ │    │
│  └──────────────┘    │  │ Vocabulary Seeds             │ │    │
│                      │  │ (Tuva/Athena: LOINC, SNOMED, │ │    │
│                      │  │  RxNorm, ICD-10, CPT, HCPCS) │ │    │
│                      │  └──────────────────────────────┘ │    │
│                      └──────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

## OMOP CDM Tables Generated

| OMOP Table               | FHIR Source         | Vocabulary Crosswalk |
|--------------------------|---------------------|----------------------|
| `person`                 | Patient             | Gender, Race, Ethnicity → OMOP concept |
| `condition_occurrence`   | Condition           | SNOMED CT → OMOP concept |
| `measurement`            | Observation         | LOINC → OMOP concept |
| `visit_occurrence`       | Encounter           | HL7 ActCode → OMOP visit concept |
| `drug_exposure`          | MedicationRequest   | RxNorm → OMOP concept |

## Installation

### From Snowflake Marketplace

1. Search for **"Tuva FHIR-to-OMOP"** in the Snowflake Marketplace
2. Click **Get** to install
3. Grant the requested privileges (CREATE DATABASE, EXECUTE TASK)
4. Open the Streamlit UI from the installed app

### Manual Deployment (Development)

```sql
CREATE APPLICATION PACKAGE tuva_fhir_to_omop_pkg;

CREATE STAGE tuva_fhir_to_omop_pkg.staging.app_stage;

-- Upload files (from SnowSQL or Snow CLI)
-- PUT file://manifest.yml @tuva_fhir_to_omop_pkg.staging.app_stage/;
-- PUT file://scripts/* @tuva_fhir_to_omop_pkg.staging.app_stage/scripts/;
-- PUT file://streamlit/* @tuva_fhir_to_omop_pkg.staging.app_stage/streamlit/;
-- PUT file://README.md @tuva_fhir_to_omop_pkg.staging.app_stage/;

CREATE APPLICATION tuva_fhir_to_omop
    FROM APPLICATION PACKAGE tuva_fhir_to_omop_pkg
    USING '@tuva_fhir_to_omop_pkg.staging.app_stage';
```

## Usage

### 1. Configure

Open the Streamlit UI → **Configure** tab:
- Select your FHIR source database, schema, and table
- Specify the JSON column name (default: `RAW_JSON`)
- Set the output schema name (default: `OMOP_CDM`)
- Click **Save Configuration**

### 2. Run Transformation

**Run Transformation** tab:
- Click **Run Full Transformation** for the complete pipeline
- Or run individual mappers (Parse, Persons, Conditions, Measurements, Visits, Drugs)

### 3. Explore Results

**Explore OMOP** tab to browse the generated OMOP CDM tables.

### 4. Programmatic Access

```sql
CALL core.run_full_transformation(
    'MY_DB.MY_SCHEMA.FHIR_BUNDLES',
    'RAW_JSON',
    'OMOP_CDM'
);

-- Individual mappers
CALL core.parse_fhir_bundles('MY_DB.MY_SCHEMA.FHIR_BUNDLES', 'RAW_JSON');
CALL core.map_persons('OMOP_CDM');
CALL core.map_conditions('OMOP_CDM');
CALL core.map_measurements('OMOP_CDM');
CALL core.map_visits('OMOP_CDM');
CALL core.map_drug_exposures('OMOP_CDM');
```

## FHIR Input Requirements

Source table with FHIR R4 Bundles as JSON:

```sql
CREATE TABLE my_fhir_data.staging.raw_bundles (
    RAW_JSON VARIANT  -- FHIR R4 Bundle JSON
);
```

Each row = one FHIR Bundle containing `entry` array with Patient, Condition, Observation, Encounter, and MedicationRequest resources.

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

## Cloud Support

| Cloud Provider | Status |
|---------------|--------|
| AWS           | ✅      |
| Azure         | ✅      |
| GCP           | ✅      |

No cloud-specific features — pure Snowflake SQL + Python stored procedures.

## Application Roles

| Role        | Access Level                                     |
|-------------|--------------------------------------------------|
| `app_admin` | Full access: configure, run transformations, view |
| `app_user`  | Read-only: view terminology, explore OMOP output  |

## Credits & Acknowledgments

- **[Tuva Health](https://thetuvaproject.com/)** (Apache 2.0) — Vocabulary/terminology seed table structure and FHIR parsing patterns. Tuva's own output is the Tuva Core Data Model (not OMOP); this project uses Tuva's terminology assets to power OMOP mapping.
- **[OHDSI Athena](https://athena.ohdsi.org/)** — Source vocabulary data for concept crosswalks (LOINC, SNOMED, RxNorm, ICD-10, CPT, HCPCS → OMOP concept IDs).
- **[OHDSI CDM](https://ohdsi.github.io/CommonDataModel/)** — OMOP Common Data Model v5.4 specification (target schema).
- **[HL7 FHIR](https://www.hl7.org/fhir/)** — Fast Healthcare Interoperability Resources R4 standard (source format).

## License

Apache License 2.0 — See [LICENSE](LICENSE) for details.

## Project Structure

```
tuva-fhir-to-omop-app/
├── manifest.yml              # Native App manifest
├── README.md                 # This file
├── scripts/
│   ├── setup.sql             # Primary setup script
│   ├── setup_seeds.sql       # Vocabulary/terminology tables
│   ├── setup_procs.sql       # FHIR parsing + OMOP mapping procs (original)
│   └── setup_streamlit.sql   # Streamlit UI registration
├── streamlit/
│   ├── main.py               # Streamlit configuration + run UI
│   └── environment.yml       # Python dependencies
└── seeds/                    # Vocabulary data files (populated at build)
```
