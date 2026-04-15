# FHIR-to-OMOP Native App — CoCo Skill

A [Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code) skill that guides you through building a customized Snowflake Native App to transform FHIR R4 bundles into OMOP CDM v5.4 tables.

## What This Skill Does

Instead of installing a pre-built app, this skill teaches CoCo how to build one tailored to your needs — your FHIR resource types, your vocabulary mappings, your UI preferences. CoCo walks you through a 7-step workflow with mandatory checkpoints so you stay in control.

## Installation

Copy the `skill/` directory to your CoCo skills folder:

```bash
# Clone the repo (or download just the skill/ directory)
git clone https://github.com/JacinthLaval/tuva-fhir-to-omop-app.git
cp -r tuva-fhir-to-omop-app/skill ~/.snowflake/cortex/skills/fhir-to-omop-native-app
```

Or if `$SNOWFLAKE_HOME` is set:
```bash
cp -r tuva-fhir-to-omop-app/skill $SNOWFLAKE_HOME/cortex/skills/fhir-to-omop-native-app
```

## Usage

Open Cortex Code and say any of these:

- "Build a FHIR to OMOP native app"
- "Create a healthcare data transformation app"
- "I need to convert FHIR R4 to OMOP CDM"

CoCo will load the skill and walk you through:

1. **Gather Requirements** — app name, source table, FHIR column, output schema
2. **Scaffold Project** — manifest, setup scripts, Streamlit UI, config
3. **Build Vocabulary Seeds** — LOINC, SNOMED, RxNorm, ICD-10, demographic crosswalks
4. **Build Mapper Procs** — 9 mappers + orchestrator + quality validator
5. **Build Streamlit UI** — 7-tab interface with safety guards
6. **Deploy and Test** — graduated 3-level testing (1 → 10 → 1,000 bundles)
7. **Version and Share** — release channels + cross-account direct share

## Skill Structure

```
skill/
├── SKILL.md                          # Main workflow (217 lines)
├── references/
│   ├── omop-schema.md                # OMOP CDM v5.4 table definitions
│   ├── mapping-patterns.md           # CTE-first LATERAL FLATTEN patterns
│   ├── sis-constraints.md            # Streamlit-in-Snowflake runtime gotchas
│   └── deployment.md                 # Versioning, release channels, sharing
└── assets/templates/
    ├── manifest.yml                  # Native App manifest template
    ├── setup.sql                     # Setup script skeleton
    ├── environment.yml               # Conda YAML (proper format)
    └── config.toml                   # Snowflake theme + fonts
```

## Key Knowledge Encoded

- **CTE-first pattern** — Snowflake doesn't support `LEFT JOIN LATERAL FLATTEN` with ON clause
- **SiS runtime limits** — no `icon=` params, no `:material/` icons, no version specifiers in environment.yml
- **OMOP_CDM protection** — default output to `omop_staging`, confirmation guard on production schema
- **Consumer grants** — reference binding alone is insufficient; must GRANT USAGE + SELECT explicitly
- **Release channel workflow** — REGISTER VERSION → ADD to channel → SET directive

## Prerequisites

- Snowflake account with ACCOUNTADMIN role
- A warehouse (X-Small handles 1,000 bundles in ~65 seconds)
- FHIR R4 data in a Snowflake table (VARIANT column with JSON bundles)

## License

Apache 2.0 — same as the parent project.
