-- =============================================================================
-- Tuva FHIR-to-OMOP — Streamlit UI Registration
-- =============================================================================

CREATE OR REPLACE STREAMLIT core.tuva_fhir_to_omop_ui
    FROM 'streamlit'
    MAIN_FILE = 'main.py';

GRANT USAGE ON STREAMLIT core.tuva_fhir_to_omop_ui TO APPLICATION ROLE app_user;
GRANT USAGE ON STREAMLIT core.tuva_fhir_to_omop_ui TO APPLICATION ROLE app_admin;
