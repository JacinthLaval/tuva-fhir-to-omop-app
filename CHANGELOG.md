# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [V1.6] - 2026-04-21

### Added
- Tuva Input Layer output format (13 mappers): patient, encounter, condition, lab_result, observation, medication, immunization, procedure, location, practitioner, medical_claim, eligibility, appointment
- Output format toggle in Streamlit UI (OMOP CDM ↔ Tuva Input Layer)
- `data_source` label parameter on all Tuva mappers
- Tuva coverage mode in Explore tab

### Changed
- Skill renamed from `fhir-to-omop-native-app` to `snowflake-health-data-forge`
- Explore tab is now format-aware (shows OMOP or Tuva tables based on output choice)
- `run_full_transformation` accepts 4th parameter for output format ('omop' or 'tuva')

## [V1.5] - 2026-04-19

### Added
- HL7v2 message support with 10 segment converters (PID, PV1, DG1, OBX, OBR, RXA, PR1, AL1, IN1, NK1)
- `parse_hl7v2_to_fhir` procedure for HL7v2 → FHIR resource conversion
- Full OMOP CDM coverage: 23 mappers total
- Per-mapper progress bar in Streamlit Run tab
- Auto-detection of input format (HL7v2 MSH segment, JSON, XML)

### Changed
- Run Transformation UI shows per-step progress instead of single spinner

## [V1.4] - 2026-04-17

### Added
- 14 new OMOP mappers: immunizations, med_administrations, allergies, devices, diagnostic_reports, imaging_studies, care_plans, locations, organizations, practitioners, claims, care_teams, observation_periods, cdm_source
- `validate_fhir_quality` procedure for input data quality checks
- FHIR XML bundle support

### Changed
- `run_full_transformation` orchestrator expanded from 9 steps to 23 OMOP steps

## [V1.2] - 2026-04-15

### Added
- Procedure mapper (SNOMED/CPT/HCPCS → OMOP)
- Qualitative observation mapper (non-numeric Observations)
- Death mapper (Patient.deceased)
- Snowflake theme for Streamlit UI

### Fixed
- SiS compatibility issues with Streamlit deployment
- Deploy script stage path corrections

## [V1.0] - 2026-04-12

### Added
- Initial release: FHIR R4 JSON → OMOP CDM v5.4
- 5 core mappers: person, condition_occurrence, measurement, visit_occurrence, drug_exposure
- FHIR bundle parser with LATERAL FLATTEN
- Vocabulary crosswalk tables (LOINC, SNOMED, RxNorm, ICD-10, CPT, HCPCS, Demographics)
- Streamlit configuration UI
- Graduated test framework (levels 1-3)
- Vocabulary validation test
- CoCo skill for automated builds
- Apache 2.0 license
