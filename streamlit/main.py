import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Tuva FHIR-to-OMOP", page_icon="🏥", layout="wide")

session = get_active_session()

TUVA_BLUE = "#1B2A4A"
TUVA_ACCENT = "#29B5E8"

st.markdown(f"""
<style>
    .main-header {{
        background: linear-gradient(135deg, {TUVA_BLUE} 0%, #2C3E6B 100%);
        padding: 1.5rem 2rem;
        border-radius: 12px;
        margin-bottom: 1.5rem;
    }}
    .main-header h1 {{color: white; margin: 0; font-size: 1.8rem;}}
    .main-header p {{color: #A0B4D0; margin: 0.3rem 0 0 0; font-size: 0.95rem;}}
    .metric-card {{
        background: white;
        border: 1px solid #E8ECF1;
        border-radius: 10px;
        padding: 1.2rem;
        text-align: center;
    }}
    .metric-value {{font-size: 2rem; font-weight: 700; color: {TUVA_BLUE};}}
    .metric-label {{font-size: 0.8rem; color: #6B7B8D; text-transform: uppercase;}}
    .status-running {{color: #F39C12; font-weight: 600;}}
    .status-completed {{color: #27AE60; font-weight: 600;}}
    .status-failed {{color: #E74C3C; font-weight: 600;}}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="main-header">
    <h1>Tuva FHIR-to-OMOP Transformer</h1>
    <p>Vocabulary seeds from <a href="https://thetuvaproject.com/" style="color:#29B5E8;">Tuva Health</a> (Apache 2.0) + <a href="https://athena.ohdsi.org/" style="color:#29B5E8;">OHDSI Athena</a> &bull; OMOP mapping: custom-built &bull; Multi-cloud &bull; Your data never leaves your account</p>
</div>
""", unsafe_allow_html=True)

tab_config, tab_run, tab_history, tab_explore = st.tabs([
    "⚙️ Configure", "▶️ Run Transformation", "📊 Run History", "🔍 Explore OMOP"
])

with tab_config:
    st.subheader("FHIR Source Configuration")

    col1, col2 = st.columns(2)
    with col1:
        try:
            dbs = session.sql("SHOW DATABASES").collect()
            db_names = [row['name'] for row in dbs]
        except:
            db_names = []
        source_db = st.selectbox("Source Database", db_names, index=None,
                                  help="Database containing your raw FHIR R4 bundles")

    with col2:
        schema_names = []
        if source_db:
            try:
                schemas = session.sql(f"SHOW SCHEMAS IN DATABASE {source_db}").collect()
                schema_names = [row['name'] for row in schemas]
            except:
                pass
        source_schema = st.selectbox("Source Schema", schema_names, index=None)

    table_names = []
    if source_db and source_schema:
        try:
            tables = session.sql(
                f"SHOW TABLES IN {source_db}.{source_schema}"
            ).collect()
            table_names = [row['name'] for row in tables]
        except:
            pass
    source_table = st.selectbox("FHIR Bundles Table", table_names, index=None,
                                 help="Table with raw FHIR R4 JSON bundles")

    json_column = st.text_input("JSON Column Name", value="RAW_JSON",
                                 help="Column containing the FHIR Bundle JSON")

    st.divider()
    st.subheader("OMOP Output Configuration")
    output_schema = st.text_input("Output Schema", value="OMOP_CDM",
                                   help="Schema where OMOP CDM tables will be created")

    if source_db and source_schema and source_table:
        fq_table = f"{source_db}.{source_schema}.{source_table}"
        st.success(f"Ready to transform: `{fq_table}` → `{output_schema}`")

        if st.button("💾 Save Configuration"):
            try:
                session.sql(f"""
                    MERGE INTO app_state.configuration t
                    USING (
                        SELECT 'source_table' AS key, '{fq_table}' AS value
                        UNION ALL SELECT 'json_column', '{json_column}'
                        UNION ALL SELECT 'output_schema', '{output_schema}'
                    ) s ON t.key = s.key
                    WHEN MATCHED THEN UPDATE SET value = s.value, updated_at = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT (key, value) VALUES (s.key, s.value)
                """).collect()
                st.success("Configuration saved!")
            except Exception as e:
                st.error(f"Error saving: {e}")

with tab_run:
    st.subheader("Run FHIR-to-OMOP Transformation")

    try:
        config_rows = session.sql(
            "SELECT key, value FROM app_state.configuration"
        ).collect()
        config = {row['KEY']: row['VALUE'] for row in config_rows}
    except:
        config = {}

    if config.get('source_table'):
        st.info(f"**Source:** `{config['source_table']}` → **Output:** `{config.get('output_schema', 'OMOP_CDM')}`")

        col1, col2 = st.columns([1, 3])
        with col1:
            run_btn = st.button("🚀 Run Full Transformation", type="primary", use_container_width=True)
        with col2:
            st.caption("Parses FHIR bundles → Maps Person, Condition, Measurement, Visit, Drug Exposure")

        if run_btn:
            with st.spinner("Running transformation pipeline..."):
                try:
                    result = session.call(
                        'core.run_full_transformation',
                        config['source_table'],
                        config.get('json_column', 'RAW_JSON'),
                        config.get('output_schema', 'OMOP_CDM')
                    )
                    if 'FAILED' in str(result):
                        st.error(result)
                    else:
                        st.success(result)
                        st.balloons()
                except Exception as e:
                    st.error(f"Transformation failed: {e}")

        st.divider()
        st.subheader("Run Individual Mappers")
        mapper_cols = st.columns(5)
        mappers = [
            ("Parse FHIR", "core.parse_fhir_bundles"),
            ("Persons", "core.map_persons"),
            ("Conditions", "core.map_conditions"),
            ("Measurements", "core.map_measurements"),
            ("Visits", "core.map_visits"),
        ]
        for i, (label, proc) in enumerate(mappers):
            with mapper_cols[i % 5]:
                if st.button(label, use_container_width=True):
                    with st.spinner(f"Running {label}..."):
                        try:
                            if 'parse' in proc:
                                result = session.call(proc, config['source_table'],
                                                       config.get('json_column', 'RAW_JSON'))
                            else:
                                result = session.call(proc, config.get('output_schema', 'OMOP_CDM'))
                            st.success(result)
                        except Exception as e:
                            st.error(str(e))
    else:
        st.warning("Please configure your FHIR source in the **Configure** tab first.")

with tab_history:
    st.subheader("Transformation Run History")
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

with tab_explore:
    st.subheader("Explore OMOP CDM Output")

    try:
        config_rows = session.sql(
            "SELECT key, value FROM app_state.configuration"
        ).collect()
        config = {row['KEY']: row['VALUE'] for row in config_rows}
        out_schema = config.get('output_schema', 'OMOP_CDM')
    except:
        out_schema = 'OMOP_CDM'

    omop_tables = ['person', 'condition_occurrence', 'measurement',
                   'visit_occurrence', 'drug_exposure']

    selected_table = st.selectbox("OMOP Table", omop_tables)

    if selected_table:
        try:
            df = session.sql(
                f"SELECT * FROM {out_schema}.{selected_table} LIMIT 100"
            ).to_pandas()
            st.dataframe(df, use_container_width=True, hide_index=True)

            count = session.sql(
                f"SELECT COUNT(*) AS cnt FROM {out_schema}.{selected_table}"
            ).collect()[0]['CNT']
            st.caption(f"Showing first 100 of {count:,} rows")
        except Exception as e:
            st.info(f"Table `{out_schema}.{selected_table}` not yet created. Run the transformation first.")

st.divider()
st.markdown("""
<div style="text-align:center; color:#6B7B8D; font-size:0.8rem;">
    Tuva FHIR-to-OMOP &bull;
    Vocabulary: <a href="https://thetuvaproject.com/" target="_blank">Tuva Health</a> (Apache 2.0) +
    <a href="https://athena.ohdsi.org/" target="_blank">OHDSI Athena</a> &bull;
    OMOP CDM mapping: custom-built &bull;
    Built on <a href="https://www.snowflake.com/" target="_blank">Snowflake</a>
    &bull; Multi-cloud: AWS | Azure | GCP
</div>
""", unsafe_allow_html=True)
