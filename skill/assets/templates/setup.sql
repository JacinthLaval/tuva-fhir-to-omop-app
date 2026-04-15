-- Primary Setup Script — creates schemas, roles, tables, loads sub-scripts
-- Customize: app name, schema names, role names as needed

CREATE APPLICATION ROLE IF NOT EXISTS app_user;
CREATE APPLICATION ROLE IF NOT EXISTS app_admin;

CREATE OR ALTER VERSIONED SCHEMA core;
GRANT USAGE ON SCHEMA core TO APPLICATION ROLE app_user;
GRANT USAGE ON SCHEMA core TO APPLICATION ROLE app_admin;

CREATE SCHEMA IF NOT EXISTS app_state;
GRANT USAGE ON SCHEMA app_state TO APPLICATION ROLE app_admin;

CREATE TABLE IF NOT EXISTS app_state.configuration (
    key         VARCHAR(256)  NOT NULL,
    value       VARCHAR(4096) NOT NULL,
    updated_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (key)
);
GRANT SELECT, INSERT, UPDATE ON TABLE app_state.configuration TO APPLICATION ROLE app_admin;

CREATE TABLE IF NOT EXISTS app_state.run_history (
    run_id          VARCHAR(36)     DEFAULT UUID_STRING(),
    started_at      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    completed_at    TIMESTAMP_NTZ,
    status          VARCHAR(20)     DEFAULT 'RUNNING',
    fhir_bundles    INTEGER         DEFAULT 0,
    persons_mapped  INTEGER         DEFAULT 0,
    conditions_mapped INTEGER       DEFAULT 0,
    measurements_mapped INTEGER     DEFAULT 0,
    visits_mapped   INTEGER         DEFAULT 0,
    errors          INTEGER         DEFAULT 0,
    error_detail    VARCHAR(16777216),
    PRIMARY KEY (run_id)
);
GRANT SELECT, INSERT, UPDATE ON TABLE app_state.run_history TO APPLICATION ROLE app_admin;
GRANT SELECT ON TABLE app_state.run_history TO APPLICATION ROLE app_user;

CREATE OR REPLACE PROCEDURE core.register_reference(ref_name VARCHAR, operation VARCHAR, ref_or_alias VARCHAR)
    RETURNS VARCHAR
    LANGUAGE SQL
    EXECUTE AS OWNER
AS
BEGIN
    CASE (operation)
        WHEN 'ADD' THEN
            SELECT SYSTEM$SET_REFERENCE(:ref_name, :ref_or_alias);
        WHEN 'REMOVE' THEN
            SELECT SYSTEM$REMOVE_REFERENCE(:ref_name, :ref_or_alias);
        WHEN 'CLEAR' THEN
            SELECT SYSTEM$REMOVE_ALL_REFERENCES(:ref_name);
    END CASE;
    RETURN 'OK';
END;
GRANT USAGE ON PROCEDURE core.register_reference(VARCHAR, VARCHAR, VARCHAR)
    TO APPLICATION ROLE app_admin;

EXECUTE IMMEDIATE FROM 'setup_seeds.sql';
EXECUTE IMMEDIATE FROM 'seed_vocabulary_data.sql';
EXECUTE IMMEDIATE FROM 'setup_procs.sql';
EXECUTE IMMEDIATE FROM 'setup_streamlit.sql';

GRANT APPLICATION ROLE app_user TO APPLICATION ROLE app_admin;
