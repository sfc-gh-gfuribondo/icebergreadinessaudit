# 🧊 The Iceberg Readiness Audit

The **Iceberg Readiness Audit** is a native Snowflake application designed to evaluate your existing databases and provide a strategic roadmap for migrating to **Apache Iceberg**.

By combining automated metadata inspection with AI-driven logic via **Snowflake Cortex**, the app identifies technical blockers and recommends the ideal target architecture for every table.

---

## 🚀 Key Features

- **Automated Metadata Audit**  
  Automatically extracts schema details, clustering keys, and column data types from the Snowflake `INFORMATION_SCHEMA`.

- **AI-Powered Suitability Engine**  
  Utilizes the `mistral-large2` model to analyze your specific table structures against known Iceberg technical limitations.

- **Target Recommendations**  
  Determines whether a table should be migrated to a **Snowflake-Managed Iceberg table** or an **External Iceberg table** based on feature requirements such as Automatic Clustering.

- **Executive Reporting**  
  Generates a professional summary of overall database readiness, common migration blockers, and recommended next steps.

- **Granular Drill-downs**  
  Provides a table-by-table breakdown of feature losses, warnings, and required schema changes.

---

## 🛠️ Installation (Streamlit in Snowflake)

This application runs **natively within your Snowflake account**. No external hosting or local Python setup is required.

1. **Create a Streamlit App**  
   In the Snowflake UI (Snowsight), navigate to **Projects → Streamlit** and click **+ Streamlit App**.

2. **Select Location**  
   Choose the database and schema where the app will reside, and select a warehouse to power the analysis.

3. **Paste the Code**  
   Copy the entire contents of `iceberg_analyzer_app.py` and paste it into the Streamlit code editor.

4. **Run**  
   Click **Run** to initialize the analyzer.

---

## 🔍 Migration Logic & Constraints

The app evaluates tables against known technical gaps between **Snowflake Native tables** and the **Iceberg format**.

### Technical Blockers Analyzed

- **Unsupported Data Types**  
  Identifies `GEOGRAPHY`, `GEOMETRY`, and `VARIANT` columns that must be converted to structured types.

- **Metadata Limitations**  
  Detects columns using **Collation** or **UUID** types, which are currently unsupported in Snowflake-managed Iceberg tables.

- **Table Lifecycle**  
  Flags **Transient** and **Temporary** tables as unsuitable for direct migration.

- **Precision Limits**  
  Identifies timestamp columns with nanosecond precision that will be truncated to microseconds.

---

## 📊 Feature Comparison Matrix

| Feature               | Native | Managed Iceberg | External Iceberg |
|-----------------------|--------|-----------------|------------------|
| Fail-safe             | Yes    | No              | No               |
| Collation             | Yes    | No              | No               |
| Snowpipe Streaming    | Yes    | No              | No               |
| Automatic Clustering  | Yes    | Yes             | No               |
| Time Travel           | 90 days| Yes             | Limited          |

> **Note:** If a table has clustering keys and requires automatic maintenance, the app will flag **External Iceberg** as an unsuitable target.

---

## 📋 Requirements

- **Role Privileges**  
  The executing role must have the `SNOWFLAKE.CORTEX_USER` database role to access AI functions.

- **Warehouse**  
  An active warehouse is required to run the Streamlit app and execute Cortex AI queries.

- **Metadata Access**  
  The role must have permission to `SHOW DATABASES` and query the `INFORMATION_SCHEMA` of the selected database.
