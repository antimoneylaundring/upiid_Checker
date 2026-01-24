import streamlit as st
import pandas as pd
import time
import re
import datetime
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2 import extras, OperationalError, InterfaceError

# Load environment variables
load_dotenv()

# Database Configuration from .env
DB_URL = os.getenv("DB_URL")

if not DB_URL:
    st.error("❌ DB_URL not found in .env file. Please add your database connection string.")
    st.stop()

# Convert SQLAlchemy format to psycopg2 format if needed
if DB_URL and 'postgresql+psycopg2://' in DB_URL:
    DB_URL = DB_URL.replace('postgresql+psycopg2://', 'postgresql://')

st.set_page_config(
    page_title="UPI/Bank Import & Check",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ============================================================================
# CUSTOM CSS STYLING
# ============================================================================
st.markdown("""
    <style>
    /* Main container styling */
    .main {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
    }
    
    /* Card-like containers */
    .stApp > div > div {
        background-color: rgba(255, 255, 255, 0.95);
        border-radius: 15px;
        padding: 2rem;
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
    }
    
    /* Headers styling */
    h1 {
        color: #2c3e50;
        font-weight: 700;
        text-align: center;
        margin-bottom: 1rem;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
    }
    
    h2 {
        color: #34495e;
        font-weight: 600;
        border-bottom: 3px solid #667eea;
        padding-bottom: 0.5rem;
        margin-bottom: 1.5rem;
    }
    
    /* Metric cards */
    [data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: 700;
        color: #667eea;
    }
    
    [data-testid="stMetricLabel"] {
        font-size: 1rem;
        font-weight: 500;
        color: #5a6c7d;
    }
    
    /* Buttons */
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 0.75rem 2rem;
        font-weight: 600;
        font-size: 1rem;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6);
    }
    
    /* File uploader */
    [data-testid="stFileUploader"] {
        background-color: #f8f9fa;
        border: 2px dashed #667eea;
        border-radius: 10px;
        padding: 1rem;
    }
    
    /* Text inputs and text areas */
    .stTextInput > div > div > input,
    .stTextArea > div > div > textarea {
        border-radius: 8px;
        border: 2px solid #e1e8ed;
        padding: 0.75rem;
        transition: all 0.3s ease;
    }
    
    .stTextInput > div > div > input:focus,
    .stTextArea > div > div > textarea:focus {
        border-color: #667eea;
        box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
    }
    
    /* Select boxes */
    .stSelectbox > div > div {
        border-radius: 8px;
        border: 2px solid #e1e8ed;
    }
    
    /* Info boxes */
    .stInfo {
        background-color: #e3f2fd;
        border-left: 4px solid #2196f3;
        border-radius: 8px;
        padding: 1rem;
    }
    
    .stSuccess {
        background-color: #e8f5e9;
        border-left: 4px solid #4caf50;
        border-radius: 8px;
        padding: 1rem;
    }
    
    .stWarning {
        background-color: #fff3e0;
        border-left: 4px solid #ff9800;
        border-radius: 8px;
        padding: 1rem;
    }
    
    .stError {
        background-color: #ffebee;
        border-left: 4px solid #f44336;
        border-radius: 8px;
        padding: 1rem;
    }
    
    /* Dataframe styling */
    .dataframe {
        border-radius: 8px;
        overflow: hidden;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    }
    
    /* Expander styling */
    .streamlit-expanderHeader {
        background-color: #f8f9fa;
        border-radius: 8px;
        font-weight: 600;
        color: #2c3e50;
    }
    
    /* Radio buttons */
    .stRadio > label {
        font-weight: 600;
        color: #2c3e50;
    }
    
    /* Divider */
    hr {
        border: none;
        height: 2px;
        background: linear-gradient(90deg, transparent, #667eea, transparent);
        margin: 2rem 0;
    }
    
    /* Column gaps */
    [data-testid="column"] {
        background-color: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.05);
    }
    
    /* Spinner */
    .stSpinner > div {
        border-top-color: #667eea !important;
    }
    
    /* Download button */
    .stDownloadButton > button {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 0.75rem 2rem;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    
    .stDownloadButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(17, 153, 142, 0.4);
    }
    </style>
""", unsafe_allow_html=True)

# Table options and their required columns + conflict column + filter value
TABLE_OPTIONS = {
    "UPI": {
        "table_name": "all_upiiD",
        "required": ["Upi_vpa", "Inserted_date"],
        "conflict_col": "Upi_vpa",
        "filter_value": "UPI"
    },
    "Bank Account": {
        "table_name": "all_bank_acc",
        "required": ["Bank_account_number", "Inserted_date"],
        "conflict_col": "Bank_account_number",
        "filter_value": "Bank Account"
    }
}

def get_db_connection():
    """Create database connection with keepalive settings"""
    return psycopg2.connect(
        DB_URL,
        connect_timeout=10,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
    )

def normalize_colname(name: str) -> str:
    return re.sub(r'[^a-z0-9]', '', str(name).lower())


def map_columns(df_columns):
    mapping = {}
    for col in df_columns:
        mapping[normalize_colname(col)] = col
    return mapping


def find_required_columns(df_cols, required_list):
    mapping = map_columns(df_cols)
    found = {}
    missing = []
    for req in required_list:
        norm_req = normalize_colname(req)
        if norm_req in mapping:
            found[req] = mapping[norm_req]
        else:
            parts = re.split(r'[^a-z0-9]+', req.lower())
            matched = None
            for dfcol in df_cols:
                n = normalize_colname(dfcol)
                if all(p for p in parts if p and p in n):
                    matched = dfcol
                    break
            if matched:
                found[req] = matched
            else:
                missing.append(req)
    return found, missing


def import_with_retries(records,
                        table_name,
                        on_conflict,
                        initial_chunk_size=1000,
                        max_retries=3,
                        backoff_seconds=2):
    """Insert all data in ONE transaction for maximum speed"""
    total = len(records)
    if total == 0:
        return {"inserted": 0, "errors": []}

    conn = None
    
    for attempt in range(max_retries):
        try:
            # Get column names from first record
            columns = list(records[0].keys())
            
            # Prepare ALL values as list of tuples - NO CHUNKING
            values = [tuple(row[col] for col in columns) for row in records]
            
            # Connect to database
            st.info(f"🔌 Connecting to database... (Attempt {attempt + 1}/{max_retries})")
            conn = get_db_connection()
            cur = conn.cursor()
            
            # Build INSERT query with ON CONFLICT
            cols_str = ', '.join([f'"{col}"' for col in columns])
            insert_query = f"""
                INSERT INTO "{table_name}" ({cols_str})
                VALUES %s
                ON CONFLICT ("{on_conflict}") DO UPDATE SET
                "Inserted_date" = EXCLUDED."Inserted_date"
            """
            
            # Execute SINGLE batch insert for ALL records
            st.info(f"⚡ Inserting {len(values)} records in ONE transaction...")
            start_time = time.time()
            
            # Use larger page_size for better performance
            extras.execute_values(cur, insert_query, values, page_size=5000)
            
            conn.commit()
            elapsed = time.time() - start_time
            
            st.success(f"✅ Successfully inserted {len(values)} records in {elapsed:.2f} seconds!")
            
            cur.close()
            conn.close()
            
            return {"inserted": len(values), "errors": []}
            
        except (OperationalError, InterfaceError) as e:
            msg = str(e)
            if conn:
                try:
                    conn.close()
                except:
                    pass
            
            if attempt < max_retries - 1:
                wait_time = backoff_seconds * (2 ** attempt)
                st.warning(f"⚠️ Connection error: {msg}")
                st.info(f"🔄 Retrying in {wait_time} seconds... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                st.error(f"❌ Failed after {max_retries} attempts: {msg}")
                return {"inserted": 0, "errors": [{"error": msg}]}
                
        except Exception as e:
            msg = str(e)
            if conn:
                try:
                    if not conn.closed:
                        conn.rollback()
                    conn.close()
                except:
                    pass
            
            st.error(f"❌ Error during import: {msg}")
            return {"inserted": 0, "errors": [{"error": msg}]}
    
    return {"inserted": 0, "errors": [{"error": "Max retries exceeded"}]}


# ============================================================================
# CHECK FUNCTIONS
# ============================================================================

def check_id_in_db(id_value: str, table_name: str, search_column: str) -> dict:
    """Check if ID exists in database using direct SQL query"""
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        query = f'SELECT * FROM "{table_name}" WHERE "{search_column}" = %s LIMIT 1'
        cur.execute(query, (id_value.strip(),))
        
        result = cur.fetchone()
        
        cur.close()
        conn.close()
        
        if result:
            return {
                "exists": True,
                "record": dict(result),
                "error": None
            }
        else:
            return {
                "exists": False,
                "record": None,
                "error": None
            }
    except Exception as e:
        if conn:
            try:
                conn.close()
            except:
                pass
        return {
            "exists": False,
            "record": None,
            "error": str(e)
        }


def check_ids_batch(ids_list: list, table_name: str, search_column: str) -> pd.DataFrame:
    """Check multiple IDs in batch"""
    results = []
    
    # Use batch query for better performance
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Query all IDs at once
        placeholders = ','.join(['%s'] * len(ids_list))
        query = f'SELECT "{search_column}" FROM "{table_name}" WHERE "{search_column}" IN ({placeholders})'
        cur.execute(query, ids_list)
        
        found_ids = set(row[search_column] for row in cur.fetchall())
        
        cur.close()
        conn.close()
        
        # Build results
        for id_val in ids_list:
            exists = id_val in found_ids
            results.append({
                "ID": id_val,
                "Exists": "✅ Yes" if exists else "❌ No",
                "Status": "Found" if exists else "Not Found"
            })
            
    except Exception as e:
        if conn:
            try:
                conn.close()
            except:
                pass
        # Fallback to individual checks
        st.warning(f"⚠️ Batch check failed: {e}. Falling back to individual checks...")
        for id_val in ids_list:
            check_result = check_id_in_db(id_val, table_name, search_column)
            results.append({
                "ID": id_val,
                "Exists": "✅ Yes" if check_result["exists"] else "❌ No",
                "Status": check_result["error"] if check_result["error"] else "Found" if check_result["exists"] else "Not Found"
            })
    
    return pd.DataFrame(results)


# ============================================================================
# HEADER
# ============================================================================
st.title("🏦 Total Database Summary")
st.markdown("<p style='text-align: center; color: #5a6c7d; font-size: 1.1rem;'>UPI & Bank Account Management System</p>", unsafe_allow_html=True)
st.markdown("---")

try:
    conn = get_db_connection()
    cur = conn.cursor()

    # Total UPI IDs
    cur.execute('SELECT COUNT(*) FROM "all_upiiD"')
    upi_count = cur.fetchone()[0]

    # Total Bank Accounts
    cur.execute('SELECT COUNT(*) FROM "all_bank_acc"')
    bank_count = cur.fetchone()[0]

    cur.close()
    conn.close()

    # ===== DISPLAY =====
    col1, col2 = st.columns(2)

    with col1:
        st.metric("💳 Total UPI IDs", f"{upi_count:,}")

    with col2:
        st.metric("🏦 Total Bank Accounts", f"{bank_count:,}")

except Exception as e:
    st.error("❌ Failed to fetch data from database")
    st.exception(e)

st.markdown("---")

# ============================================================================
# MAIN UI: TWO-COLUMN LAYOUT
# ============================================================================

col1, col2 = st.columns(2, gap="large")

# ============================================================================
# COLUMN 1: IMPORT FUNCTIONALITY
# ============================================================================
with col1:
    st.header("📥 Import IDs")
    st.markdown("Upload CSV file and import data into database")
    
    target_label = st.selectbox("Select target to import", list(TABLE_OPTIONS.keys()), key="import_target")
    target_cfg = TABLE_OPTIONS[target_label]
    TABLE_NAME = target_cfg["table_name"]
    REQUIRED_COLS = target_cfg["required"]
    CONFLICT_COL = target_cfg["conflict_col"]
    FILTER_VALUE = target_cfg["filter_value"]
    
    st.markdown(f"**Target table:** `{TABLE_NAME}`")
    
    uploaded_file = st.file_uploader("Upload CSV file", type=["csv", "txt"], key="import_file")
    
    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file, dtype=str)
            st.info(f"📄 Loaded {len(df)} rows with {len(df.columns)} columns")
            
        except Exception as e:
            st.error(f"Could not read CSV: {e}")
            st.stop()
        
        # Dynamic filtering based on selected import type
        st.write(f"**Applying filters for {target_label}...**")
        
        mask = (
            (df["Feature_type"].astype(str).str.strip() == "BS Money Laundering") &
            (df["Upi_bank_account_wallet"].astype(str).str.strip().isin([FILTER_VALUE])) &
            (df["Search_for"].astype(str).str.strip().isin(["App", "Web"]))
        )

        filtered_df = df[mask].copy()
        st.write(f"✅ After filtering: {len(filtered_df)} rows (removed {len(df) - len(filtered_df)} rows)")
        
        if len(filtered_df) == 0:
            st.warning("⚠️ No rows matched the filter criteria. Please check your CSV data.")
            st.stop()
        
        # Find required columns
        found_cols_map, missing = find_required_columns(filtered_df.columns.tolist(), REQUIRED_COLS)
        
        if missing:
            st.error(f"❌ CSV must contain columns: {', '.join(missing)}")
            st.error(f"Available columns: {', '.join(filtered_df.columns.tolist())}")
            st.stop()
        
        # Select and rename columns
        actual_cols = [found_cols_map[c] for c in REQUIRED_COLS]
        df_clean = filtered_df[actual_cols].copy()
        rename_map = {found_cols_map[c]: c for c in REQUIRED_COLS}
        df_clean = df_clean.rename(columns=rename_map)
        
        # Validate key column exists
        key_col = CONFLICT_COL
        if key_col not in df_clean.columns:
            st.error(f"Internal error: expected key column '{key_col}' not found.")
            st.stop()
        
        # Clean data
        df_clean = df_clean.dropna(subset=[key_col])
        df_clean[key_col] = df_clean[key_col].astype(str).str.strip()
        
        # Apply specific transformations based on type
        if key_col.lower() == "upi_vpa":
            df_clean[key_col] = df_clean[key_col].str.lower()
        
        # Remove empty strings after strip
        df_clean = df_clean[df_clean[key_col] != ""]
        df_clean = df_clean.drop_duplicates(subset=[key_col])
        
        st.write(f"🧹 After cleaning: {len(df_clean)} unique records")
        
        # --------------------------
        # INSERTED_DATE NORMALIZE & TODAY CHECK
        # --------------------------
        if "Inserted_date" in df_clean.columns:
            try:
                # Convert to YYYY-MM-DD (string)
                df_clean["Inserted_date"] = pd.to_datetime(df_clean["Inserted_date"], errors="coerce").dt.strftime("%Y-%m-%d")
            except Exception:
                # If conversion fails, leave as-is
                pass

            # Today's date in ISO format (YYYY-MM-DD)
            today_str = datetime.date.today().isoformat()
            today_mask = df_clean["Inserted_date"] == today_str
            today_count = int(today_mask.sum())

            if today_count > 0:
                st.warning(f"⚠️ {today_count} row(s) contain today's date ({today_str}).")
                st.write("If you don't want rows with today's date to be imported, select the checkbox below to remove them and continue.")
                remove_today = st.checkbox("Remove rows with today's date and continue import", value=False, key="remove_today")

                if remove_today:
                    df_clean = df_clean[~today_mask].copy()
                    st.info(f"✅ Removed {today_count} row(s) with today's date. Remaining rows: {len(df_clean)}")
                else:
                    st.error("⛔ Import stopped because file contains rows with today's date. Select the checkbox to remove them and continue.")
                    st.stop()
        # --------------------------
        # END OF DATE CHECK
        # --------------------------
        
        records = df_clean.to_dict(orient="records")
        
        if len(records) == 0:
            st.warning("⚠️ No records to import after all filters and cleaning.")
            st.stop()
        
        st.success(f"✅ Prepared {len(records)} unique records to import into `{TABLE_NAME}`")
        
        # Show preview
        with st.expander("📋 Preview data (first 10 rows)", expanded=False):
            st.dataframe(df_clean.head(10), use_container_width=True)
        
        # Configuration options (removed - using single transaction)
        st.info("💡 Import will execute in ONE transaction for maximum speed")
        
        btn = st.button("🚀 Start Import", use_container_width=True, type="primary")
        
        if btn:
            with st.spinner("Importing all records in one transaction..."):
                result = import_with_retries(
                    records,
                    TABLE_NAME,
                    on_conflict=CONFLICT_COL,
                    initial_chunk_size=len(records),
                    max_retries=3,
                    backoff_seconds=2
                )
                
                st.divider()
                st.success(f"✅ Done! Processed: {result['inserted']} records")
                
                if result["errors"]:
                    st.error(f"⚠️ {len(result['errors'])} chunk errors")
                    with st.expander("View errors"):
                        st.json(result["errors"])
                else:
                    st.info("✅ No errors reported")

# ============================================================================
# COLUMN 2: CHECK FUNCTIONALITY
# ============================================================================
with col2:
    st.header("🔍 Check IDs")
    st.markdown("Search for IDs in database")
    
    check_target = st.selectbox("Select target to check", list(TABLE_OPTIONS.keys()), key="check_target")
    check_cfg = TABLE_OPTIONS[check_target]
    CHECK_TABLE = check_cfg["table_name"]
    SEARCH_COLUMN = check_cfg["conflict_col"]
    
    st.markdown(f"**Searching in:** `{CHECK_TABLE}`")
    
    check_method = st.radio("Check method", ["Single/Multiple IDs", "Batch Upload"], horizontal=True)
    
    if check_method == "Single/Multiple IDs":
        st.markdown("**Enter IDs (one per line or comma-separated)**")
        id_input = st.text_area("Enter IDs to search", 
                                placeholder="user1@upi\nuser2@upi\nuser3@upi\n\nOr: user1@upi, user2@upi, user3@upi",
                                height=100)
        
        if id_input:
            # Parse IDs - handle both newline and comma separated
            if ',' in id_input:
                ids_list = [id.strip() for id in id_input.split(',') if id.strip()]
            else:
                ids_list = [id.strip() for id in id_input.split('\n') if id.strip()]
            
            st.info(f"📊 Found {len(ids_list)} ID(s) to check")
            
            if st.button("🔎 Search All", use_container_width=True, type="primary"):
                with st.spinner("Searching..."):
                    results_df = check_ids_batch(ids_list, CHECK_TABLE, SEARCH_COLUMN)
                    
                    # Show summary
                    col_exists, col_not_exists = st.columns(2)
                    with col_exists:
                        exists_count = (results_df["Exists"] == "✅ Yes").sum()
                        st.metric("Found", exists_count, f"{(exists_count/len(results_df)*100):.1f}%")
                    with col_not_exists:
                        not_exists_count = (results_df["Exists"] == "❌ No").sum()
                        st.metric("Not Found", not_exists_count, f"{(not_exists_count/len(results_df)*100):.1f}%")
                    
                    # Show results table
                    st.dataframe(results_df, use_container_width=True, height=300)
                    
                    # Show details for found IDs
                    found_ids = results_df[results_df["Exists"] == "✅ Yes"]["ID"].tolist()
                    if found_ids:
                        st.subheader("📋 Details of Found IDs")
                        for id_val in found_ids:
                            result = check_id_in_db(id_val, CHECK_TABLE, SEARCH_COLUMN)
                            with st.expander(f"🔍 {id_val}"):
                                st.json(result["record"])
                    
                    # Download option
                    csv = results_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Results",
                        data=csv,
                        file_name=f"check_results_{check_target.lower().replace(' ', '_')}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
    
    else:
        batch_file = st.file_uploader("Upload CSV with IDs to check", type=["csv", "txt"], key="check_file")
        
        if batch_file:
            try:
                batch_df = pd.read_csv(batch_file, dtype=str)
                st.info(f"📄 Loaded {len(batch_df)} rows")
            except Exception as e:
                st.error(f"Could not read CSV: {e}")
                st.stop()
            
            found_cols_map, _ = find_required_columns(batch_df.columns.tolist(), [SEARCH_COLUMN])
            
            if SEARCH_COLUMN not in found_cols_map:
                id_column = batch_df.columns[0]
                st.warning(f"⚠️ Column '{SEARCH_COLUMN}' not found. Using first column '{id_column}' as ID column")
            else:
                id_column = found_cols_map[SEARCH_COLUMN]
                st.info(f"✅ Using column '{id_column}' for IDs")
            
            batch_ids = batch_df[id_column].astype(str).str.strip().tolist()
            batch_ids = [id for id in batch_ids if id]
            
            st.info(f"📊 Found {len(batch_ids)} IDs to check")
            
            if st.button("🔎 Check All", use_container_width=True, type="primary"):
                with st.spinner("Checking all IDs..."):
                    results_df = check_ids_batch(batch_ids, CHECK_TABLE, SEARCH_COLUMN)
                    
                    col_exists, col_not_exists = st.columns(2)
                    with col_exists:
                        exists_count = (results_df["Exists"] == "✅ Yes").sum()
                        st.metric("Found", exists_count, f"{(exists_count/len(results_df)*100):.1f}%")
                    with col_not_exists:
                        not_exists_count = (results_df["Exists"] == "❌ No").sum()
                        st.metric("Not Found", not_exists_count, f"{(not_exists_count/len(results_df)*100):.1f}%")
                    
                    st.dataframe(results_df, use_container_width=True, height=400)
                    
                    csv = results_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Results",
                        data=csv,
                        file_name=f"check_results_{check_target.lower().replace(' ', '_')}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )