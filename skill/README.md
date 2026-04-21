# Snowflake Health Data Forge — CoCo Skill

A [Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code) skill that guides you through building a Snowflake Native App to transform **HL7v2**, **FHIR R4 (JSON & XML)** clinical data into **OMOP CDM v5.4** or **Tuva Input Layer** tables.

## What This Skill Does

Instead of installing a pre-built app, this skill teaches CoCo how to build one tailored to your needs — your input formats, your vocabulary mappings, your output model, your UI preferences. CoCo walks you through a 7-step workflow with mandatory checkpoints so you stay in control.

## Installation

Copy the `skill/` directory to your CoCo skills folder:

```bash
git clone https://github.com/JacinthLaval/tuva-fhir-to-omop-app.git
cp -r tuva-fhir-to-omop-app/skill ~/.snowflake/cortex/skills/snowflake-health-data-forge
```

Or if `$SNOWFLAKE_HOME` is set:
```bash
cp -r tuva-fhir-to-omop-app/skill $SNOWFLAKE_HOME/cortex/skills/snowflake-health-data-forge
```

## Usage

Open Cortex Code and say any of these:

- "Build a FHIR to OMOP native app"
- "Convert HL7v2 messages to OMOP"
- "Create a Tuva Input Layer from FHIR data"
- "I need a health data forge"
- "Transform clinical data to OMOP CDM"

CoCo will load the skill and walk you through:

1. **Gather Requirements** — app name, source table, input format, output model (OMOP or Tuva)
2. **Scaffold Project** — manifest, setup scripts, Streamlit UI, config
3. **Build Vocabulary Seeds** — LOINC, SNOMED, RxNorm, ICD-10, CPT, HCPCS, demographic crosswalks
4. **Build Mapper Procs** — 23 OMOP mappers + 13 Tuva mappers + HL7v2 converter + orchestrator + quality validator (39 procs total)
5. **Build Streamlit UI** — format-aware configure/run/explore with output toggle and per-mapper progress
6. **Deploy and Test** — graduated 3-level testing (1 → 10 → 1,000 bundles), OMOP or Tuva output
7. **Version and Share** — release channels + cross-account direct share

## Skill Structure

```
skill/
├── SKILL.md                          # Main workflow
├── README.md                         # This file
├── references/
│   ├── omop-schema.md                # OMOP CDM v5.4 table definitions
│   ├── mapping-patterns.md           # 23 OMOP + 13 Tuva + HL7v2 converter patterns
│   ├── sis-constraints.md            # Streamlit-in-Snowflake runtime gotchas
│   └── deployment.md                 # Versioning, release channels, sharing
└── assets/templates/
    ├── manifest.yml                  # Native App manifest template
    ├── setup.sql                     # Setup script skeleton
    ├── environment.yml               # Conda YAML (proper format)
    └── config.toml                   # Snowflake theme + fonts
```

## Key Knowledge Encoded

- **Multi-format input** — auto-detects HL7v2 (MSH segment), FHIR JSON, FHIR XML
- **Dual output** — OMOP CDM v5.4 (concept_id mapped) or Tuva Input Layer (source codes, `data_source` label)
- **HL7v2 converter** — 10 segment types (PID, PV1, DG1, OBX, OBR, RXA, PR1, AL1, IN1, NK1) → FHIR resources
- **CTE-first pattern** — Snowflake doesn't support `LEFT JOIN LATERAL FLATTEN` with ON clause
- **SiS runtime limits** — no `icon=` params, no `:material/` icons, no version specifiers in environment.yml
- **Consumer grants** — reference binding alone is insufficient; must GRANT USAGE + SELECT explicitly
- **Release channel workflow** — REGISTER VERSION → ADD to channel → SET directive (max 2 versions per channel)

## Prerequisites

- Snowflake account with ACCOUNTADMIN role
- A warehouse (X-Small handles 1,000 FHIR bundles in ~65s, 4,013 HL7v2 messages in ~270s)
- Clinical data in a Snowflake table (HL7v2 messages, FHIR R4 JSON bundles, or FHIR R4 XML bundles)

## License

Apache 2.0 — same as the parent project.
