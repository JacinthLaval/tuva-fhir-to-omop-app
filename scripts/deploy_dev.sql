-- =============================================================================
-- Tuva FHIR-to-OMOP — Dev Mode Deployment
-- Run with: snow sql -c HealthcareDemos -f scripts/deploy_dev.sql
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE SI_DEMO_WH;

-- ---------------------------------------------------------------------------
-- 1. Application Package
-- ---------------------------------------------------------------------------
CREATE APPLICATION PACKAGE IF NOT EXISTS TUVA_FHIR_TO_OMOP_PKG
    COMMENT = 'FHIR R4 → OMOP CDM v5.4 transformation (dev)';

-- ---------------------------------------------------------------------------
-- 2. Staging schema + named stage inside the package
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS TUVA_FHIR_TO_OMOP_PKG.STAGING;

CREATE STAGE IF NOT EXISTS TUVA_FHIR_TO_OMOP_PKG.STAGING.APP_CODE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for app code, scripts, vocabulary seeds, and Streamlit';

-- ---------------------------------------------------------------------------
-- 3. Upload files (run from SnowSQL / CLI)
-- ---------------------------------------------------------------------------
PUT file:///Users/toddcrosslin/Downloads/CoCoStuff/tuva-fhir-to-omop-app/manifest.yml @TUVA_FHIR_TO_OMOP_PKG.STAGING.APP_CODE/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
PUT file:///Users/toddcrosslin/Downloads/CoCoStuff/tuva-fhir-to-omop-app/README.md @TUVA_FHIR_TO_OMOP_PKG.STAGING.APP_CODE/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
PUT file:///Users/toddcrosslin/Downloads/CoCoStuff/tuva-fhir-to-omop-app/scripts/setup.sql @TUVA_FHIR_TO_OMOP_PKG.STAGING.APP_CODE/scripts/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
PUT file:///Users/toddcrosslin/Downloads/CoCoStuff/tuva-fhir-to-omop-app/scripts/setup_seeds.sql @TUVA_FHIR_TO_OMOP_PKG.STAGING.APP_CODE/scripts/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
PUT file:///Users/toddcrosslin/Downloads/CoCoStuff/tuva-fhir-to-omop-app/scripts/seed_vocabulary_data.sql @TUVA_FHIR_TO_OMOP_PKG.STAGING.APP_CODE/scripts/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
PUT file:///Users/toddcrosslin/Downloads/CoCoStuff/tuva-fhir-to-omop-app/scripts/setup_procs.sql @TUVA_FHIR_TO_OMOP_PKG.STAGING.APP_CODE/scripts/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
PUT file:///Users/toddcrosslin/Downloads/CoCoStuff/tuva-fhir-to-omop-app/scripts/setup_streamlit.sql @TUVA_FHIR_TO_OMOP_PKG.STAGING.APP_CODE/scripts/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
PUT file:///Users/toddcrosslin/Downloads/CoCoStuff/tuva-fhir-to-omop-app/scripts/load_seeds.py @TUVA_FHIR_TO_OMOP_PKG.STAGING.APP_CODE/scripts/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
PUT file:///Users/toddcrosslin/Downloads/CoCoStuff/tuva-fhir-to-omop-app/streamlit/main.py @TUVA_FHIR_TO_OMOP_PKG.STAGING.APP_CODE/streamlit/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
PUT file:///Users/toddcrosslin/Downloads/CoCoStuff/tuva-fhir-to-omop-app/streamlit/environment.yml @TUVA_FHIR_TO_OMOP_PKG.STAGING.APP_CODE/streamlit/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
PUT file:///Users/toddcrosslin/Downloads/CoCoStuff/tuva-fhir-to-omop-app/streamlit/.streamlit/config.toml @TUVA_FHIR_TO_OMOP_PKG.STAGING.APP_CODE/streamlit/.streamlit/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
PUT file:///Users/toddcrosslin/Downloads/CoCoStuff/tuva-fhir-to-omop-app/streamlit/static/Inter-Regular.ttf @TUVA_FHIR_TO_OMOP_PKG.STAGING.APP_CODE/streamlit/static/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
PUT file:///Users/toddcrosslin/Downloads/CoCoStuff/tuva-fhir-to-omop-app/streamlit/static/Inter-Medium.ttf @TUVA_FHIR_TO_OMOP_PKG.STAGING.APP_CODE/streamlit/static/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
PUT file:///Users/toddcrosslin/Downloads/CoCoStuff/tuva-fhir-to-omop-app/streamlit/static/Inter-SemiBold.ttf @TUVA_FHIR_TO_OMOP_PKG.STAGING.APP_CODE/streamlit/static/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
PUT file:///Users/toddcrosslin/Downloads/CoCoStuff/tuva-fhir-to-omop-app/streamlit/static/Inter-Bold.ttf @TUVA_FHIR_TO_OMOP_PKG.STAGING.APP_CODE/streamlit/static/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
PUT file:///Users/toddcrosslin/Downloads/CoCoStuff/tuva-fhir-to-omop-app/streamlit/static/JetBrainsMono-Regular.ttf @TUVA_FHIR_TO_OMOP_PKG.STAGING.APP_CODE/streamlit/static/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
PUT file:///Users/toddcrosslin/Downloads/CoCoStuff/tuva-fhir-to-omop-app/streamlit/static/JetBrainsMono-Medium.ttf @TUVA_FHIR_TO_OMOP_PKG.STAGING.APP_CODE/streamlit/static/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;

-- ---------------------------------------------------------------------------
-- 4. Create application in dev mode (uses stage directly, no version needed)
-- ---------------------------------------------------------------------------
DROP APPLICATION IF EXISTS TUVA_FHIR_TO_OMOP_APP CASCADE;

CREATE APPLICATION TUVA_FHIR_TO_OMOP_APP
    FROM APPLICATION PACKAGE TUVA_FHIR_TO_OMOP_PKG
    USING '@TUVA_FHIR_TO_OMOP_PKG.STAGING.APP_CODE'
    COMMENT = 'FHIR-to-OMOP dev instance';

-- ---------------------------------------------------------------------------
-- 5. Grant necessary privileges to the app
-- ---------------------------------------------------------------------------
GRANT CREATE DATABASE ON ACCOUNT TO APPLICATION TUVA_FHIR_TO_OMOP_APP;
GRANT EXECUTE TASK ON ACCOUNT TO APPLICATION TUVA_FHIR_TO_OMOP_APP;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO APPLICATION TUVA_FHIR_TO_OMOP_APP;
GRANT USAGE ON WAREHOUSE SI_DEMO_WH TO APPLICATION TUVA_FHIR_TO_OMOP_APP;

-- ---------------------------------------------------------------------------
-- 6. Bind references (FHIR source table + warehouse)
-- ---------------------------------------------------------------------------
CALL TUVA_FHIR_TO_OMOP_APP.CORE.REGISTER_REFERENCE(
    'fhir_source_database', 'SET', 'TRE_HEALTHCARE_DB.FHIR_STAGING.RAW_BUNDLES'
);
CALL TUVA_FHIR_TO_OMOP_APP.CORE.REGISTER_REFERENCE(
    'consumer_warehouse', 'SET', 'SI_DEMO_WH'
);

SELECT 'Deployment complete — TUVA_FHIR_TO_OMOP_APP ready in dev mode' AS status;
