# Deployment & Sharing

## Local Development Deploy

```sql
CREATE APPLICATION PACKAGE IF NOT EXISTS <PKG_NAME>;
CREATE SCHEMA IF NOT EXISTS <PKG_NAME>.STAGING;
CREATE OR REPLACE STAGE <PKG_NAME>.STAGING.APP_CODE
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = NONE RECORD_DELIMITER = NONE);
```

PUT all project files (preserve directory structure):
```sql
PUT file:///path/to/manifest.yml @<PKG_NAME>.STAGING.APP_CODE/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
PUT file:///path/to/README.md @<PKG_NAME>.STAGING.APP_CODE/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
PUT file:///path/to/scripts/setup.sql @<PKG_NAME>.STAGING.APP_CODE/scripts/ OVERWRITE=TRUE AUTO_COMPRESS=FALSE;
-- ... all scripts, streamlit files, config.toml, fonts
```

Create dev application:
```sql
CREATE APPLICATION <APP_NAME> FROM APPLICATION PACKAGE <PKG_NAME> USING '@<PKG_NAME>.STAGING.APP_CODE';
```

## Versioned Release

When release channels exist (they're auto-created on some accounts):

```sql
-- Step 1: Register version
ALTER APPLICATION PACKAGE <PKG_NAME> REGISTER VERSION v1_0
    USING '@<PKG_NAME>.STAGING.APP_CODE';

-- Step 2: Add version to DEFAULT channel
ALTER APPLICATION PACKAGE <PKG_NAME>
    MODIFY RELEASE CHANNEL DEFAULT ADD VERSION v1_0;

-- Step 3: Set release directive
ALTER APPLICATION PACKAGE <PKG_NAME>
    MODIFY RELEASE CHANNEL DEFAULT
    SET DEFAULT RELEASE DIRECTIVE VERSION = v1_0 PATCH = 0;
```

**If release channels are NOT enabled**, use simpler syntax:
```sql
ALTER APPLICATION PACKAGE <PKG_NAME> ADD VERSION v1_0
    USING '@<PKG_NAME>.STAGING.APP_CODE';
ALTER APPLICATION PACKAGE <PKG_NAME>
    SET DEFAULT RELEASE DIRECTIVE VERSION = v1_0 PATCH = 0;
```

## Cross-Account Sharing (Direct Share)

Add specific account to the release channel:
```sql
ALTER APPLICATION PACKAGE <PKG_NAME>
    MODIFY RELEASE CHANNEL DEFAULT ADD ACCOUNT <ACCOUNT_LOCATOR>;
```

Consumer installs with:
```sql
CREATE APPLICATION <APP_NAME>
    FROM APPLICATION PACKAGE <PKG_NAME>;
```

**No `USING` clause** for versioned installs — it uses the release directive automatically.

## Consumer Setup (CRITICAL)

After installing, the consumer MUST:

1. **Grant privileges:**
```sql
GRANT CREATE DATABASE ON ACCOUNT TO APPLICATION <APP_NAME>;
GRANT USAGE ON WAREHOUSE <WH> TO APPLICATION <APP_NAME>;
```

2. **Grant source data access** (reference binding alone is NOT sufficient):
```sql
GRANT USAGE ON DATABASE <FHIR_DB> TO APPLICATION <APP_NAME>;
GRANT USAGE ON SCHEMA <FHIR_DB>.<SCHEMA> TO APPLICATION <APP_NAME>;
GRANT SELECT ON TABLE <FHIR_DB>.<SCHEMA>.<TABLE> TO APPLICATION <APP_NAME>;
```

3. **Bind references:**
```sql
CALL <APP_NAME>.core.register_reference('fhir_source_database', 'ADD', '<FHIR_DB>.<SCHEMA>.<TABLE>');
CALL <APP_NAME>.core.register_reference('consumer_warehouse', 'ADD', '<WH>');
```

## deploy_dev.sql Template

A convenience script that combines all PUT + CREATE commands. Key notes:
- Use `'SET'` (not `'ADD'`) for REGISTER_REFERENCE calls when re-binding
- Include PUT commands for `.streamlit/config.toml` and `static/` font files
- `AUTO_COMPRESS=FALSE` for all files

## Performance Benchmarks

| Bundles | Warehouse | Duration | Persons | Conditions | Measurements | Visits |
|---------|-----------|----------|---------|------------|--------------|--------|
| 1 | X-Small | ~17s | 2 | 5 | 23 | 1 |
| 10 | X-Small | ~27s | 20 | 38 | 231 | 10 |
| 1,000 | X-Small | ~65s | 2,000 | 3,520 | 22,725 | 1,000 |
