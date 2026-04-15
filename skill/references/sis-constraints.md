# Streamlit in Snowflake (SiS) — Native App Constraints

## Runtime Limitations (confirmed)

The SiS Streamlit runtime in Native Apps is older than the latest open-source Streamlit. These features do NOT work:

| Feature | Error / Symptom | Workaround |
|---------|----------------|------------|
| `icon=` on st.button, st.info, etc. | `TypeError: got unexpected keyword argument 'icon'` | Use plain text labels |
| `:material/icon_name:` syntax | Renders as literal text "/icon_name:" | Use plain text in tabs, subheaders, title |
| `accept_new_options` on st.selectbox | `TypeError` | Omit parameter |
| Advanced config.toml keys | Silently ignored | Use only core theme keys |

## environment.yml Format (CRITICAL)

Must be proper conda YAML — NOT a flat package list.

```yaml
name: sf_env
channels:
  - snowflake
dependencies:
  - snowflake-snowpark-python
  - streamlit
```

**Rules:**
- Must have `name`, `channels`, `dependencies` keys
- Channel must be `snowflake`
- NO version specifiers like `>=1.22.0` — bare package names only
- Package names: lowercase, numbers, `.-_` only

## config.toml for Theming

Place at `streamlit/.streamlit/config.toml` in the app stage.

```toml
[server]
enableStaticServing = true

[theme]
primaryColor = "#29B5E8"
backgroundColor = "#ffffff"
secondaryBackgroundColor = "#f4f9fc"
textColor = "#11567F"
font = "Inter"
```

**Static fonts:** Place .ttf files in `streamlit/static/`, reference as `app/static/filename.ttf`:
```toml
[[theme.fontFaces]]
family = "Inter"
url = "app/static/Inter-Regular.ttf"
weight = 400
```

**Advanced keys** (may require SiS runtime 1.53+, use with caution):
`codeBackgroundColor`, `chartCategoricalColors`, `headingFontSizes`, `showWidgetBorder`

## Streamlit UI Best Practices for Native Apps

- Use `st.title()` + `st.caption()` for headers — no inline HTML/CSS
- Use plain text tab labels: `st.tabs(["Configure", "Run", "History", ...])`
- Use `st.dataframe()` for tables — no custom HTML tables
- Use `st.metric()` for KPIs — no custom HTML metric cards
- Use `st.bar_chart()` or `st.plotly_chart()` for visuals
- Pagination: use `st.caption(f"Page {n} of {total}")` — no HTML pagination
- Footer: use `st.caption()` with markdown links

## Native App Manifest (manifest_version 2)

- `DEBUG_MODE` is NOT supported — do not include
- `EXECUTE IMMEDIATE FROM` paths are RELATIVE to the setup script's directory
- References: use `register_callback` proc, object_type TABLE or WAREHOUSE
- Default streamlit: reference as `core.tuva_fhir_to_omop_ui` (schema.name)
