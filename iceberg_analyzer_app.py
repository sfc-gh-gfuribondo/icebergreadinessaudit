import streamlit as st
import json

try:
    from snowflake.snowpark.context import get_active_session
    session = get_active_session()
except:
    from snowflake.snowpark import Session
    session = Session.builder.config('connection_name', 'default').create()

st.title("🧊 Iceberg Readiness Audit")
st.caption("Analyze your Snowflake database for Apache Iceberg compatibility")



@st.cache_data(ttl=300)
def get_databases():
    result = session.sql("SHOW DATABASES").collect()
    return [row["name"] for row in result]

@st.cache_data(ttl=60)
def get_table_metadata(_session, database_name):
    query = f"""
    SELECT 
        c.TABLE_SCHEMA,
        c.TABLE_NAME,
        t.IS_TRANSIENT,
        t.CLUSTERING_KEY,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'column', c.COLUMN_NAME,
                'type', c.DATA_TYPE,
                'collation', IFNULL(c.COLLATION_NAME, 'none'),
                'precision', c.DATETIME_PRECISION
            )
        ) AS COLUMNS_INFO
    FROM {database_name}.INFORMATION_SCHEMA.COLUMNS c
    JOIN {database_name}.INFORMATION_SCHEMA.TABLES t
        ON c.TABLE_SCHEMA = t.TABLE_SCHEMA 
        AND c.TABLE_NAME = t.TABLE_NAME
    WHERE t.TABLE_TYPE = 'BASE TABLE'
    GROUP BY c.TABLE_SCHEMA, c.TABLE_NAME, t.IS_TRANSIENT, t.CLUSTERING_KEY
    """
    return _session.sql(query).to_pandas()

def analyze_table_with_ai(_session, schema_name, table_name, is_transient, clustering_key, columns_info):
    prompt = f"""You are a Snowflake data engineer. Analyze if this table is suitable for migration to Apache Iceberg format.

ICEBERG DATA TYPE LIMITATIONS:
- VARIANT columns NOT supported (must convert to structured OBJECT/ARRAY/MAP)
- Semi-structured ARRAY and OBJECT must be structured types with defined schemas
- GEOGRAPHY and GEOMETRY types NOT supported
- Collation on columns NOT supported
- UUID type NOT supported in Snowflake-managed Iceberg tables
- Timestamp precision limited to microseconds (6), nanoseconds will truncate
- Temporary and transient tables NOT supported

FEATURE COMPARISON (Native vs Iceberg Managed vs Iceberg External):
| Feature              | Native | Managed | External |
|----------------------|--------|---------|----------|
| Fail-safe            | Yes    | No      | No       |
| Collation            | Yes    | No      | No       |
| Snowpipe Streaming   | Yes    | No      | No       |
| Automatic Clustering | Yes    | Yes     | No       |
| Replication          | Yes    | No      | No       |
| Time Travel          | 90 days| Yes     | Limited  |

IMPORTANT: If table has clustering keys and needs clustering, External Iceberg is NOT suitable.

TABLE METADATA:
Schema: {schema_name}
Table: {table_name}
Is Transient: {is_transient}
Clustering Key: {clustering_key if clustering_key else 'none'}
Columns: {columns_info}

Respond with raw JSON only. Do NOT include ```json or ``` or any markdown. Start directly with curly brace:
{{"suitable": true/false, "target": "MANAGED or EXTERNAL", "blockers": ["list"], "feature_loss": ["list"], "warnings": ["list"], "recommendation": "brief recommendation"}}"""

    query = f"""SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '{prompt.replace("'", "''")}') AS RESULT"""
    result = _session.sql(query).collect()
    return result[0]["RESULT"]

def generate_summary_paragraph(_session, database_name, results):
    table_summaries = []
    for r in results:
        status = "suitable" if r["analysis"].get("suitable") else "not suitable"
        blockers = r["analysis"].get("blockers", [])
        blocker_text = f" (blockers: {', '.join(blockers)})" if blockers else ""
        table_summaries.append(f"{r['schema']}.{r['table']}: {status}{blocker_text}")
    
    all_summaries = "\n".join(table_summaries)
    
    prompt = f"""Summarize the Iceberg migration readiness assessment for the {database_name} database. 
Do not escape underscores or use backslashes in table names.
Write a brief section on each of these.  Be specific.
1. Overall readiness (how many tables are suitable vs not)
2. Common blockers or issues found across tables
3. Key recommendations for the migration

Table assessments:
{all_summaries}

Write in a professional tone suitable for a technical report."""

    # Change SUMMARIZE to COMPLETE
    query = f"""SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '{prompt.replace("'", "''")}') AS RESULT"""
    result = _session.sql(query).collect()
    summary = result[0]["RESULT"]
    return summary.replace("\\", "")
databases = get_databases()
selected_db = st.selectbox("Select Database to Analyze", databases, index=None, placeholder="Choose a database...")

if selected_db:
    with st.spinner(f"Loading table metadata from {selected_db}..."):
        try:
            metadata_df = get_table_metadata(session, selected_db)
        except Exception as e:
            st.error(f"Error loading metadata: {e}")
            st.stop()
    
    if metadata_df.empty:
        st.warning("No base tables found in this database.")
        st.stop()
    
    schemas = sorted(metadata_df["TABLE_SCHEMA"].unique())
    
    col1, col2 = st.columns([1, 3])
    with col1:
        st.metric("Total Tables", len(metadata_df))
    with col2:
        st.metric("Schemas", len(schemas))
    
    st.divider()
    
    selected_schema = st.selectbox("Filter by Schema (optional)", ["All Schemas"] + list(schemas))
    
    if selected_schema != "All Schemas":
        filtered_df = metadata_df[metadata_df["TABLE_SCHEMA"] == selected_schema]
    else:
        filtered_df = metadata_df
    
        if st.button("🔍 Analyze Tables for Iceberg Suitability", type="primary"):
            results = []
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for idx, (_, row) in enumerate(filtered_df.iterrows()):  # ← Use enumerate()
                progress = (idx + 1) / len(filtered_df)
                progress_bar.progress(progress)
                status_text.text(f"Analyzing {row['TABLE_SCHEMA']}.{row['TABLE_NAME']}...")
            
            try:
                ai_result = analyze_table_with_ai(
                    session,
                    row["TABLE_SCHEMA"],
                    row["TABLE_NAME"],
                    row["IS_TRANSIENT"],
                    row["CLUSTERING_KEY"],
                    str(row["COLUMNS_INFO"])
                )
                
                cleaned_result = ai_result.strip()
                if cleaned_result.startswith("```"):
                    cleaned_result = cleaned_result.replace("```json", "").replace("```", "").strip()
                
                try:
                    parsed = json.loads(cleaned_result)
                except:
                    parsed = {"suitable": None, "target": "UNKNOWN", "blockers": [], "feature_loss": [], "warnings": [], "recommendation": cleaned_result}
                
                results.append({
                    "schema": row["TABLE_SCHEMA"],
                    "table": row["TABLE_NAME"],
                    "clustering": row["CLUSTERING_KEY"] or "None",
                    "analysis": parsed
                })
            except Exception as e:
                results.append({
                    "schema": row["TABLE_SCHEMA"],
                    "table": row["TABLE_NAME"],
                    "clustering": row["CLUSTERING_KEY"] or "None",
                    "analysis": {"suitable": None, "error": str(e)}
                })
        
            progress_bar.empty()
            status_text.empty()
        
            st.session_state["analysis_results"] = results
            st.session_state["selected_db"] = selected_db
    
    if "analysis_results" in st.session_state:
        results = st.session_state["analysis_results"]
        
        suitable_count = sum(1 for r in results if r["analysis"].get("suitable") == True)
        unsuitable_count = sum(1 for r in results if r["analysis"].get("suitable") == False)
        unknown_count = len(results) - suitable_count - unsuitable_count
        
        st.subheader("📊 Summary")
        col1, col2, col3 = st.columns(3)
        col1.metric("✅ Suitable", suitable_count)
        col2.metric("❌ Not Suitable", unsuitable_count)
        col3.metric("⚠️ Unknown", unknown_count)
        
        schema_summary = {}
        for r in results:
            schema = r["schema"]
            if schema not in schema_summary:
                schema_summary[schema] = {"suitable": 0, "unsuitable": 0, "total": 0}
            schema_summary[schema]["total"] += 1
            if r["analysis"].get("suitable") == True:
                schema_summary[schema]["suitable"] += 1
            elif r["analysis"].get("suitable") == False:
                schema_summary[schema]["unsuitable"] += 1
        
        st.subheader("📁 Schema Overview")
        for schema, stats in schema_summary.items():
            pct = (stats["suitable"] / stats["total"] * 100) if stats["total"] > 0 else 0
            st.progress(pct / 100, text=f"**{schema}**: {stats['suitable']}/{stats['total']} tables suitable ({pct:.0f}%)")
        
        st.subheader("📝 Executive Summary")
        with st.spinner("Generating summary..."):
            try:
                db_name = st.session_state.get("selected_db", "the database")
                summary_text = generate_summary_paragraph(session, db_name, results)
                st.write(summary_text)
            except Exception as e:
                st.warning(f"Could not generate summary: {e}")
        
        st.subheader("📋 Detailed Results")
        for r in results:
            analysis = r["analysis"]
            suitable = analysis.get("suitable")
            
            if suitable == True:
                icon = "✅"
            elif suitable == False:
                icon = "❌"
            else:
                icon = "⚠️"
            
            with st.expander(f"{icon} {r['schema']}.{r['table']}"):
                if "error" in analysis:
                    st.error(f"Analysis error: {analysis['error']}")
                else:
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write(f"**Suitable:** {suitable}")
                        st.write(f"**Target:** {analysis.get('target', 'N/A')}")
                        st.write(f"**Clustering:** {r['clustering']}")
                    
                    with col2:
                        if analysis.get("blockers"):
                            st.write("**🚫 Blockers:**")
                            for b in analysis["blockers"]:
                                st.write(f"  - {b}")
                        
                        if analysis.get("feature_loss"):
                            st.write("**⚠️ Feature Loss:**")
                            for f in analysis["feature_loss"]:
                                st.write(f"  - {f}")
                    
                    if analysis.get("recommendation"):
                        st.info(f"**Recommendation:** {analysis['recommendation']}")
