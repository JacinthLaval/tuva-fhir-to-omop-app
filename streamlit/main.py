import json

import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Tuva FHIR-to-OMOP", page_icon="🏥", layout="wide")

session = get_active_session()

st.title("Tuva FHIR-to-OMOP Transformer")
st.caption(
    "Vocabulary seeds from [Tuva Health](https://thetuvaproject.com/) (Apache 2.0) + "
    "[OHDSI Athena](https://athena.ohdsi.org/) · OMOP mapping: custom-built · "
    "Multi-cloud · Your data never leaves your account"
)


@st.cache_data(ttl=300)
def list_databases():
    try:
        return [r['name'] for r in session.sql("SHOW DATABASES").collect()]
    except:
        return []


@st.cache_data(ttl=120)
def list_schemas(db):
    try:
        return [r['name'] for r in session.sql(f"SHOW SCHEMAS IN DATABASE {db}").collect()]
    except:
        return []


@st.cache_data(ttl=60)
def list_tables(db, schema):
    try:
        rows = session.sql(f"SHOW TABLES IN {db}.{schema}").collect()
        return [r['name'] for r in rows]
    except:
        return []


@st.cache_data(ttl=60)
def list_columns(db, schema, table):
    try:
        rows = session.sql(f"DESCRIBE TABLE {db}.{schema}.{table}").collect()
        return [r['name'] for r in rows]
    except:
        return []


@st.cache_data(ttl=60)
def get_table_preview(db, schema, table, limit=5):
    try:
        return session.sql(f"SELECT * FROM {db}.{schema}.{table} LIMIT {limit}").to_pandas()
    except:
        return pd.DataFrame()


def load_config():
    try:
        rows = session.sql("SELECT key, value FROM app_state.configuration").collect()
        return {r['KEY']: r['VALUE'] for r in rows}
    except:
        return {}


PROTECTED_SCHEMAS = {'OMOP_CDM'}

tab_config, tab_run, tab_history, tab_vocab, tab_coverage, tab_quality, tab_explore = st.tabs([
    "Configure", "Run", "History",
    "Vocabulary", "Coverage", "Quality", "Explore",
])

with tab_config:

    st.subheader("FHIR source")

    databases = list_databases()

    src_col1, src_col2, src_col3 = st.columns(3)
    with src_col1:
        source_db = st.selectbox("Database", databases, index=None,
                                  placeholder="Select a database…", key="cfg_src_db")
    with src_col2:
        src_schemas = list_schemas(source_db) if source_db else []
        source_schema = st.selectbox("Schema", src_schemas, index=None,
                                      placeholder="Select a schema…", key="cfg_src_schema")
    with src_col3:
        src_tables = list_tables(source_db, source_schema) if source_db and source_schema else []
        source_table = st.selectbox("Table", src_tables, index=None,
                                     placeholder="Select a table…", key="cfg_src_table")

    if source_db and source_schema and source_table:
        cols = list_columns(source_db, source_schema, source_table)
        variant_cols = []
        try:
            desc_rows = session.sql(f"DESCRIBE TABLE {source_db}.{source_schema}.{source_table}").collect()
            variant_cols = [r['name'] for r in desc_rows if 'VARIANT' in str(r['type']).upper()]
        except:
            variant_cols = cols

        jc1, jc2 = st.columns(2)
        with jc1:
            json_column = st.selectbox("JSON Column (VARIANT)",
                                        variant_cols if variant_cols else cols,
                                        index=variant_cols.index('BUNDLE_DATA') if 'BUNDLE_DATA' in variant_cols else 0,
                                        key="cfg_json_col")
        with jc2:
            id_options = [c for c in cols if c != json_column]
            default_id = id_options.index('BUNDLE_ID') if 'BUNDLE_ID' in id_options else 0
            bundle_id_col = st.selectbox("Bundle ID Column",
                                          id_options,
                                          index=default_id,
                                          key="cfg_bid_col")

        with st.expander("Preview source data", expanded=False):
            preview_df = get_table_preview(source_db, source_schema, source_table)
            if len(preview_df) > 0:
                row_count = session.sql(
                    f"SELECT COUNT(*) AS cnt FROM {source_db}.{source_schema}.{source_table}"
                ).collect()[0]['CNT']
                st.caption(f"{row_count:,} total rows")
                st.dataframe(preview_df, use_container_width=True, hide_index=True)
            else:
                st.info("Table is empty or not accessible.")

    st.markdown("---")
    st.subheader("OMOP output destination")

    out_col1, out_col2 = st.columns(2)
    with out_col1:
        out_db = st.selectbox("Output Database", databases,
                               index=databases.index(source_db) if source_db and source_db in databases else 0,
                               key="cfg_out_db")
    with out_col2:
        out_schemas = list_schemas(out_db) if out_db else []
        new_schema_label = "➕ Create new schema…"
        schema_options = out_schemas + [new_schema_label]

        default_out_idx = None
        for i, s in enumerate(schema_options):
            if s == 'OMOP_STAGING':
                default_out_idx = i
                break
        if default_out_idx is None:
            default_out_idx = schema_options.index(new_schema_label)

        out_schema_choice = st.selectbox("Output Schema", schema_options,
                                          index=default_out_idx,
                                          key="cfg_out_schema")

    if out_schema_choice == new_schema_label:
        new_schema_name = st.text_input("New schema name", value="OMOP_STAGING",
                                         key="cfg_new_schema")
        output_schema = new_schema_name.upper().strip()
    else:
        output_schema = out_schema_choice

    if output_schema.upper() in PROTECTED_SCHEMAS:
        st.warning(
            f"⚠️ **{output_schema}** contains existing production data. "
            "Writing here will **replace all OMOP tables**."
        )
        try:
            prod_counts = []
            for tbl in ['person', 'condition_occurrence', 'measurement', 'visit_occurrence',
                        'drug_exposure', 'procedure_occurrence', 'observation', 'death']:
                try:
                    cnt = session.sql(f"SELECT COUNT(*) AS c FROM {out_db}.{output_schema}.{tbl}").collect()[0]['C']
                    if cnt > 0:
                        prod_counts.append(f"**{tbl}**: {cnt:,} rows")
                except:
                    pass
            if prod_counts:
                st.error("Existing data that will be **overwritten**:\n\n" + "  •  ".join(prod_counts))
        except:
            pass
        confirm_prod = st.checkbox(
            f"I understand — overwrite all tables in {output_schema}", value=False, key="cfg_confirm_prod"
        )
        if not confirm_prod:
            st.stop()

    st.markdown("---")

    if source_db and source_schema and source_table and output_schema:
        fq_table = f"{source_db}.{source_schema}.{source_table}"

        st.subheader("Review")
        rev1, rev2 = st.columns(2)
        with rev1:
            st.markdown(f"""
            **Source**
            - `{fq_table}`
            - JSON column: `{json_column if source_table else '—'}`
            - Bundle ID: `{bundle_id_col if source_table else '—'}`
            """)
        with rev2:
            st.markdown(f"""
            **Destination**
            - `{out_db}.{output_schema}`
            - Tables: person, condition_occurrence, measurement, visit_occurrence, drug_exposure, procedure_occurrence, observation, death
            """)

        if st.button("Save configuration", type="primary", use_container_width=True):
            try:
                session.sql(f"""
                    MERGE INTO app_state.configuration t
                    USING (
                        SELECT 'source_table' AS key, '{fq_table}' AS value
                        UNION ALL SELECT 'json_column', '{json_column}'
                        UNION ALL SELECT 'bundle_id_column', '{bundle_id_col}'
                        UNION ALL SELECT 'output_schema', '{output_schema}'
                        UNION ALL SELECT 'output_database', '{out_db}'
                    ) s ON t.key = s.key
                    WHEN MATCHED THEN UPDATE SET value = s.value, updated_at = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT (key, value) VALUES (s.key, s.value)
                """).collect()
                st.success("✅ Configuration saved!")
                st.cache_data.clear()
            except Exception as e:
                st.error(f"Error saving: {e}")
    else:
        st.info("Select a source table and output schema above to continue.")

with tab_run:
    st.subheader("Run FHIR-to-OMOP transformation")

    config = load_config()

    if config.get('source_table'):
        out_schema_display = config.get('output_schema', 'OMOP_STAGING')
        out_db_display = config.get('output_database', '')
        fq_out = f"{out_db_display}.{out_schema_display}" if out_db_display else out_schema_display

        c1, c2, c3 = st.columns([2, 1, 1])
        with c1:
            st.markdown(f"**Source:** `{config['source_table']}`")
        with c2:
            st.markdown(f"**→ Output:** `{fq_out}`")
        with c3:
            st.markdown(f"**JSON col:** `{config.get('json_column', 'BUNDLE_DATA')}`")

        if out_schema_display.upper() in PROTECTED_SCHEMAS:
            st.error(f"⚠️ Output targets **{out_schema_display}** — production schema. Change in Configure tab if unintended.")

        st.markdown("---")

        run_col1, run_col2 = st.columns([1, 2])
        with run_col1:
            run_btn = st.button("Run full pipeline", type="primary", use_container_width=True)
        with run_col2:
            st.caption("Parse → Person → Condition → Measurement → Visit → Drug → Procedure → Observation → Death")

        if run_btn:
            progress = st.progress(0, text="Starting pipeline…")
            status_container = st.container()

            steps = [
                ("Parsing FHIR bundles", None),
                ("Mapping persons", None),
                ("Mapping conditions", None),
                ("Mapping measurements", None),
                ("Mapping visits", None),
                ("Mapping drug exposures", None),
                ("Mapping procedures", None),
                ("Mapping observations", None),
                ("Mapping death records", None),
            ]
            total_steps = len(steps)

            try:
                result = session.call(
                    'core.run_full_transformation',
                    config['source_table'],
                    config.get('json_column', 'BUNDLE_DATA'),
                    config.get('output_schema', 'OMOP_STAGING')
                )
                progress.progress(100, text="Pipeline complete!")

                if 'FAILED' in str(result):
                    st.error(result)
                else:
                    st.success(result)
                    st.balloons()

                    with st.expander("Output summary", expanded=True):
                        summary_data = []
                        for tbl in ['person', 'condition_occurrence', 'measurement',
                                    'visit_occurrence', 'drug_exposure', 'procedure_occurrence',
                                    'observation', 'death']:
                            try:
                                cnt = session.sql(
                                    f"SELECT COUNT(*) AS c FROM {out_schema_display}.{tbl}"
                                ).collect()[0]['C']
                                summary_data.append({"Table": tbl, "Rows": cnt})
                            except:
                                summary_data.append({"Table": tbl, "Rows": "—"})
                        st.dataframe(pd.DataFrame(summary_data), use_container_width=True, hide_index=True)

            except Exception as e:
                progress.progress(100, text="Pipeline failed")
                st.error(f"Transformation failed: {e}")

        st.markdown("---")
        st.subheader("Individual mappers")
        st.caption("Run a single step of the pipeline for debugging or re-processing.")

        mappers = [
            ("Parse FHIR", "core.parse_fhir_bundles", "parse"),
            ("Persons", "core.map_persons", "map"),
            ("Conditions", "core.map_conditions", "map"),
            ("Measurements", "core.map_measurements", "map"),
            ("Visits", "core.map_visits", "map"),
            ("Drugs", "core.map_drug_exposures", "map"),
            ("Procedures", "core.map_procedures", "map"),
            ("Observations", "core.map_observations_qual", "map"),
            ("Death", "core.map_death", "map"),
        ]
        row1 = st.columns(5)
        row2 = st.columns(4)
        all_cols = row1 + row2
        for i, (label, proc, kind) in enumerate(mappers):
            with all_cols[i]:
                if st.button(label, use_container_width=True, key=f"mapper_{i}"):
                    with st.spinner(f"Running {label}…"):
                        try:
                            if kind == 'parse':
                                result = session.call(
                                    proc,
                                    config['source_table'],
                                    config.get('json_column', 'BUNDLE_DATA'),
                                    config.get('bundle_id_column', 'BUNDLE_ID')
                                )
                            else:
                                result = session.call(proc, config.get('output_schema', 'OMOP_STAGING'))
                            st.success(result)
                        except Exception as e:
                            st.error(str(e))
    else:
        st.info("Configure your FHIR source in the **Configure** tab first.")

with tab_history:
    st.subheader("Run history")
    try:
        history = session.sql("""
            SELECT run_id, started_at, completed_at, status,
                   fhir_bundles, persons_mapped, conditions_mapped,
                   measurements_mapped, visits_mapped, errors
            FROM app_state.run_history
            ORDER BY started_at DESC
            LIMIT 50
        """).to_pandas()

        if len(history) > 0:
            col1, col2, col3, col4 = st.columns(4)
            latest = history.iloc[0]
            col1.metric("Total Runs", len(history))
            col2.metric("Last Status", latest['STATUS'])
            col3.metric("Last Persons", f"{latest['PERSONS_MAPPED']:,}")
            col4.metric("Last Conditions", f"{latest['CONDITIONS_MAPPED']:,}")
            st.dataframe(history, use_container_width=True, hide_index=True)
        else:
            st.info("No transformation runs yet. Go to the **Run** tab to start.")
    except:
        st.info("Run history will appear here after your first transformation.")


VOCAB_MAP = {
    "LOINC": "terminology.loinc_to_omop",
    "SNOMED": "terminology.snomed_to_omop",
    "RxNorm": "terminology.rxnorm_to_omop",
    "ICD-10-CM": "terminology.icd10cm_to_omop",
    "CPT": "terminology.cpt_to_omop",
    "HCPCS": "terminology.hcpcs_to_omop",
    "Demographics": "terminology.demographic_to_omop",
}

with tab_vocab:
    st.subheader("Vocabulary browser")

    v_col1, v_col2 = st.columns([1, 2])
    with v_col1:
        selected_vocab = st.selectbox("Vocabulary", list(VOCAB_MAP.keys()))
    with v_col2:
        search_term = st.text_input("Search by code or description", key="vocab_search", placeholder="Type to filter…")

    if selected_vocab:
        vocab_table = VOCAB_MAP[selected_vocab]
        try:
            count_row = session.sql(f"SELECT COUNT(*) AS cnt FROM {vocab_table}").collect()
            total_count = count_row[0]['CNT']
            st.caption(f"**{selected_vocab}** — {total_count:,} total records")

            page_size = 50
            if f"vocab_page_{selected_vocab}" not in st.session_state:
                st.session_state[f"vocab_page_{selected_vocab}"] = 0
            page = st.session_state[f"vocab_page_{selected_vocab}"]

            cols = session.sql(f"SELECT * FROM {vocab_table} LIMIT 0").to_pandas().columns.tolist()

            where = ""
            if search_term:
                like = search_term.replace("'", "''")
                clauses = [f"CAST({c} AS VARCHAR) ILIKE '%{like}%'" for c in cols]
                where = "WHERE " + " OR ".join(clauses)

            filtered_count = session.sql(
                f"SELECT COUNT(*) AS cnt FROM {vocab_table} {where}"
            ).collect()[0]['CNT']
            total_pages = max(1, (filtered_count + page_size - 1) // page_size)
            page = min(page, total_pages - 1)

            offset = page * page_size
            df_vocab = session.sql(
                f"SELECT * FROM {vocab_table} {where} LIMIT {page_size} OFFSET {offset}"
            ).to_pandas()
            st.dataframe(df_vocab, use_container_width=True, hide_index=True)

            nav_col1, nav_col2, nav_col3 = st.columns([1, 2, 1])
            with nav_col1:
                if st.button("Previous", disabled=(page == 0), key="vocab_prev"):
                    st.session_state[f"vocab_page_{selected_vocab}"] = page - 1
                    st.rerun()
            with nav_col2:
                st.caption(f"Page {page + 1} of {total_pages} ({filtered_count:,} matching)")
            with nav_col3:
                if st.button("Next", disabled=(page >= total_pages - 1), key="vocab_next"):
                    st.session_state[f"vocab_page_{selected_vocab}"] = page + 1
                    st.rerun()

        except Exception as e:
            st.info(f"Vocabulary table `{vocab_table}` not available yet. Deploy the app to populate terminology seeds.")


COVERAGE_DOMAINS = {
    "Person": {"table": "person", "concept_col": "gender_concept_id"},
    "Condition": {"table": "condition_occurrence", "concept_col": "condition_concept_id"},
    "Measurement": {"table": "measurement", "concept_col": "measurement_concept_id"},
    "Visit": {"table": "visit_occurrence", "concept_col": "visit_concept_id"},
    "Drug Exposure": {"table": "drug_exposure", "concept_col": "drug_concept_id"},
    "Procedure": {"table": "procedure_occurrence", "concept_col": "procedure_concept_id"},
    "Observation": {"table": "observation", "concept_col": "observation_concept_id"},
}

with tab_coverage:
    st.subheader("Coverage report")

    config = load_config()
    cov_schema = config.get('output_schema', 'OMOP_STAGING')

    coverage_data = []
    for domain, info in COVERAGE_DOMAINS.items():
        tbl = f"{cov_schema}.{info['table']}"
        col = info['concept_col']
        try:
            row = session.sql(f"""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN {col} > 0 THEN 1 ELSE 0 END) AS mapped,
                    SUM(CASE WHEN {col} = 0 OR {col} IS NULL THEN 1 ELSE 0 END) AS unmapped
                FROM {tbl}
            """).collect()[0]
            total = row['TOTAL']
            mapped = row['MAPPED']
            unmapped = row['UNMAPPED']
            pct = round(mapped / total * 100, 1) if total > 0 else 0.0
            coverage_data.append({"Domain": domain, "Total": total, "Mapped": mapped,
                                  "Unmapped": unmapped, "Coverage %": pct})
        except:
            pass

    if coverage_data:
        total_all = sum(d['Total'] for d in coverage_data)
        mapped_all = sum(d['Mapped'] for d in coverage_data)
        unmapped_all = sum(d['Unmapped'] for d in coverage_data)
        overall_pct = round(mapped_all / total_all * 100, 1) if total_all > 0 else 0.0

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total Rows", f"{total_all:,}")
        m2.metric("Mapped", f"{mapped_all:,}")
        m3.metric("Unmapped", f"{unmapped_all:,}")
        m4.metric("Overall Coverage", f"{overall_pct}%")

        chart_df = pd.DataFrame(coverage_data).set_index("Domain")[["Mapped", "Unmapped"]]
        st.bar_chart(chart_df)

        st.dataframe(pd.DataFrame(coverage_data), use_container_width=True, hide_index=True)

        st.divider()
        st.subheader("Top 20 Unmapped Source Codes by Domain")

        SOURCE_CODE_MAP = {
            "Condition": {"table": "condition_occurrence", "concept_col": "condition_concept_id",
                          "source_col": "condition_source_value"},
            "Measurement": {"table": "measurement", "concept_col": "measurement_concept_id",
                            "source_col": "measurement_source_value"},
            "Drug Exposure": {"table": "drug_exposure", "concept_col": "drug_concept_id",
                              "source_col": "drug_source_value"},
            "Visit": {"table": "visit_occurrence", "concept_col": "visit_concept_id",
                      "source_col": "visit_source_value"},
            "Procedure": {"table": "procedure_occurrence", "concept_col": "procedure_concept_id",
                          "source_col": "procedure_source_value"},
            "Observation": {"table": "observation", "concept_col": "observation_concept_id",
                            "source_col": "observation_source_value"},
        }

        for domain, info in SOURCE_CODE_MAP.items():
            tbl = f"{cov_schema}.{info['table']}"
            col = info['concept_col']
            src = info['source_col']
            try:
                unmapped_df = session.sql(f"""
                    SELECT {src} AS source_code, COUNT(*) AS occurrences
                    FROM {tbl}
                    WHERE ({col} = 0 OR {col} IS NULL) AND {src} IS NOT NULL
                    GROUP BY {src}
                    ORDER BY occurrences DESC
                    LIMIT 20
                """).to_pandas()
                if len(unmapped_df) > 0:
                    with st.expander(f"{domain} — {len(unmapped_df)} unmapped codes"):
                        st.dataframe(unmapped_df, use_container_width=True, hide_index=True)
            except:
                pass
    else:
        st.info("No OMOP tables found. Run the transformation first to see coverage metrics.")


with tab_quality:
    st.subheader("Data quality")

    def run_quality_validation():
        try:
            result = session.call('core.validate_fhir_quality')
            return json.loads(result) if isinstance(result, str) else result
        except Exception as e:
            return None

    if st.button("Re-run validation", key="rerun_quality"):
        st.session_state['quality_result'] = run_quality_validation()

    if 'quality_result' not in st.session_state:
        st.session_state['quality_result'] = run_quality_validation()

    qr = st.session_state['quality_result']

    if qr:
        if isinstance(qr, dict):
            resource_dist = qr.get('resource_distribution', {})
            if resource_dist:
                st.subheader("Resource Type Distribution")
                dist_df = pd.DataFrame(
                    [{"Resource Type": k, "Count": v} for k, v in resource_dist.items()]
                ).sort_values("Count", ascending=False)
                st.bar_chart(dist_df.set_index("Resource Type"))

            issues = qr.get('issues', qr.get('quality_issues', []))
            if issues and isinstance(issues, list):
                st.subheader("Quality Issues")
                issues_df = pd.DataFrame(issues)
                severity_colors = {
                    'critical': '🔴', 'high': '🟠', 'medium': '🟡', 'low': '🟢', 'info': '🔵'
                }
                if 'severity' in issues_df.columns:
                    issues_df['severity'] = issues_df['severity'].apply(
                        lambda s: f"{severity_colors.get(str(s).lower(), '⚪')} {s}"
                    )
                st.dataframe(issues_df, use_container_width=True, hide_index=True)
            elif not issues:
                st.success("No quality issues detected.")

            summary = qr.get('summary', {})
            if summary:
                st.subheader("Summary")
                s_cols = st.columns(len(summary))
                for i, (k, v) in enumerate(summary.items()):
                    s_cols[i].metric(k.replace('_', ' ').title(), f"{v:,}" if isinstance(v, (int, float)) else str(v))

            other_keys = [k for k in qr.keys() if k not in ('resource_distribution', 'issues', 'quality_issues', 'summary')]
            if other_keys:
                with st.expander("Full Validation Result"):
                    st.json(qr)
        else:
            st.json(qr)
    else:
        st.info("Validation not available. Ensure `core.validate_fhir_quality` is deployed and FHIR data has been parsed.")


with tab_explore:
    st.subheader("Explore OMOP CDM output")

    config = load_config()
    out_schema = config.get('output_schema', 'OMOP_STAGING')

    omop_tables = ['person', 'condition_occurrence', 'measurement',
                   'visit_occurrence', 'drug_exposure', 'procedure_occurrence',
                   'observation', 'death']

    exp_col1, exp_col2 = st.columns([1, 2])
    with exp_col1:
        selected_table = st.selectbox("OMOP Table", omop_tables, key="explore_table")
    with exp_col2:
        row_limit = st.slider("Rows to display", min_value=10, max_value=500, value=100, step=10)

    if selected_table:
        try:
            count = session.sql(
                f"SELECT COUNT(*) AS cnt FROM {out_schema}.{selected_table}"
            ).collect()[0]['CNT']

            st.caption(f"**{selected_table}** — {count:,} total rows (showing up to {row_limit})")

            df = session.sql(
                f"SELECT * FROM {out_schema}.{selected_table} LIMIT {row_limit}"
            ).to_pandas()
            st.dataframe(df, use_container_width=True, hide_index=True)
        except Exception as e:
            st.info(f"Table `{out_schema}.{selected_table}` not yet created. Run the transformation first.")


st.divider()
st.caption(
    "Tuva FHIR-to-OMOP · "
    "Vocabulary: [Tuva Health](https://thetuvaproject.com/) (Apache 2.0) + "
    "[OHDSI Athena](https://athena.ohdsi.org/) · "
    "OMOP CDM mapping: custom-built · "
    "Built on [Snowflake](https://www.snowflake.com/) · "
    "Multi-cloud: AWS | Azure | GCP"
)
