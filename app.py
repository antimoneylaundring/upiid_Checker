import streamlit as st
import pandas as pd
import time
import re
from supabase import create_client, Client
import datetime

SUPABASE_URL = "https://zekvwyaaefjtjqjolsrm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inpla3Z3eWFhZWZqdGpxam9sc3JtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjIyNDA4NTksImV4cCI6MjA3NzgxNjg1OX0.wXT_VnXuEZ2wtHSJMR9VJAIv_mtXGQdu0jy0m9V2Gno"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


st.set_page_config(page_title="UPI/Bank Import & Check", layout="wide")

# Table options and their required columns + conflict column
TABLE_OPTIONS = {
    "UPI": {
        "table_name": "all_upiiD",
        "required": ["Upi_vpa", "Inserted_date"],
        "conflict_col": "Upi_vpa"
    },
    "Bank Account": {
        "table_name": "all_bank_acc",
        "required": ["Bank_account_number", "Inserted_date"],
        "conflict_col": "Bank_account_number"
    }
}

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

def upsert_chunk(data_chunk, table_name, on_conflict):
    return supabase.table(table_name).upsert(data_chunk, on_conflict=on_conflict).execute()


def import_with_retries(records,
                        table_name,
                        on_conflict,
                        initial_chunk_size=1000,
                        max_retries=3,
                        backoff_seconds=2):
    total = len(records)
    if total == 0:
        return {"inserted": 0, "errors": []}

    chunk_size = initial_chunk_size
    inserted = 0
    errors = []

    idx = 0
    while idx < total:
        chunk = records[idx: idx + chunk_size]
        attempt = 0
        success = False

        while attempt <= max_retries and not success:
            try:
                res = upsert_chunk(chunk, table_name, on_conflict=on_conflict)
                if hasattr(res, "status_code") and getattr(res, "status_code") >= 400:
                    raise Exception(f"HTTP {res.status_code} - {getattr(res, 'data', '')}")
                inserted += len(chunk)
                success = True
            except Exception as e:
                msg = str(e)
                if "57014" in msg or "canceling statement due to statement timeout" in msg.lower():
                    attempt += 1
                    st.warning(f"Chunk size {chunk_size} timed out. Attempt {attempt}/{max_retries}. Reducing chunk size.")
                    new_chunk_size = max(1, chunk_size // 2)
                    if new_chunk_size < chunk_size:
                        chunk_size = new_chunk_size
                        st.info(f"New chunk size: {chunk_size}")
                    else:
                        time.sleep(backoff_seconds * attempt)
                else:
                    errors.append({"index": idx, "error": msg})
                    st.error(f"Error inserting chunk at index {idx}: {msg}")
                    success = True
            if not success:
                time.sleep(backoff_seconds * (2 ** (attempt - 1)) if attempt > 0 else backoff_seconds)

        idx += chunk_size

    return {"inserted": inserted, "errors": errors}


# ============================================================================
# CHECK FUNCTIONS (NEW)
# ============================================================================

def check_id_in_db(id_value: str, table_name: str, search_column: str) -> dict:
    try:
        response = supabase.table(table_name).select("*").eq(search_column, id_value.strip()).execute()
        
        if response.data and len(response.data) > 0:
            return {
                "exists": True,
                "record": response.data[0],
                "error": None
            }
        else:
            return {
                "exists": False,
                "record": None,
                "error": None
            }
    except Exception as e:
        return {
            "exists": False,
            "record": None,
            "error": str(e)
        }


def check_ids_batch(ids_list: list, table_name: str, search_column: str) -> pd.DataFrame:
    results = []
    for id_val in ids_list:
        check_result = check_id_in_db(id_val, table_name, search_column)
        results.append({
            "ID": id_val,
            "Exists": "✅ Yes" if check_result["exists"] else "❌ No",
            "Status": check_result["error"] if check_result["error"] else "Found" if check_result["exists"] else "Not Found"
        })
    return pd.DataFrame(results)


# ============================================================================
# MAIN UI: TWO-COLUMN LAYOUT
# ============================================================================

col1, col2 = st.columns(2, gap="large")

# ============================================================================
# COLUMN 1: IMPORT FUNCTIONALITY
# ============================================================================
with col1:
    st.header("📥 Import IDs")
    st.markdown("**Upload CSV file and import data into database**")
    
    target_label = st.selectbox("Select target to import", list(TABLE_OPTIONS.keys()), key="import_target")
    target_cfg = TABLE_OPTIONS[target_label]
    TABLE_NAME = target_cfg["table_name"]
    REQUIRED_COLS = target_cfg["required"]
    CONFLICT_COL = target_cfg["conflict_col"]
    
    st.markdown(f"**Target table:** `{TABLE_NAME}`")
    
    uploaded_file = st.file_uploader("Upload CSV file", type=["csv", "txt"], key="import_file")
    
    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file, dtype=str)
        except Exception as e:
            st.error(f"Could not read CSV: {e}")
            st.stop()
        
        found_cols_map, missing = find_required_columns(df.columns.tolist(), REQUIRED_COLS)
        if missing:
            st.error(f"CSV must contain columns: {', '.join(missing)}")
            st.stop()
        
        actual_cols = [found_cols_map[c] for c in REQUIRED_COLS]
        df_clean = df[actual_cols].copy()
        rename_map = {found_cols_map[c]: c for c in REQUIRED_COLS}
        df_clean = df_clean.rename(columns=rename_map)
        
        key_col = CONFLICT_COL
        if key_col not in df_clean.columns:
            st.error(f"Internal error: expected key column '{key_col}' not found.")
            st.stop()
        
        df_clean = df_clean.dropna(subset=[key_col])
        df_clean[key_col] = df_clean[key_col].astype(str).str.strip()
        df_clean = df_clean.drop_duplicates(subset=[key_col])
        
        # --------------------------
        # INSERTED_DATE NORMALIZE & TODAY CHECK (ADDED)
        # --------------------------
        if "Inserted_date" in df_clean.columns:
            try:
                # convert to YYYY-MM-DD (string)
                df_clean["Inserted_date"] = pd.to_datetime(df_clean["Inserted_date"], errors="coerce").dt.strftime("%Y-%m-%d")
            except Exception:
                # if conversion fails, leave as-is
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
        
        st.info(f"✅ Prepared {len(records)} unique records to import")
        st.dataframe(df_clean.head(10), use_container_width=True)
        
        col_chunk, col_retries = st.columns(2)
        with col_chunk:
            chunk_default = st.number_input("Chunk size", min_value=1, max_value=5000, value=1000, step=100)
        with col_retries:
            retries = st.number_input("Max retries", min_value=0, max_value=100, value=3)
        
        btn = st.button("🚀 Start Import", use_container_width=True)
        
        if btn:
            with st.spinner("Importing..."):
                result = import_with_retries(records,
                                             TABLE_NAME,
                                             on_conflict=CONFLICT_COL,
                                             initial_chunk_size=int(chunk_default),
                                             max_retries=int(retries),
                                             backoff_seconds=2)
                st.success(f"✅ Done! Processed: {result['inserted']} records")
                if result["errors"]:
                    st.error(f"⚠️ {len(result['errors'])} chunk errors")
                    st.json(result["errors"])
                else:
                    st.info("✅ No errors reported")


# ============================================================================
# COLUMN 2: CHECK FUNCTIONALITY (NEW)
# ============================================================================
with col2:
    st.header("🔍 Check IDs")
    st.markdown("**Search for IDs in database**")
    
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
            
            if st.button("🔎 Search All", use_container_width=True):
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
                    st.dataframe(results_df, use_container_width=True)
                    
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
                        file_name=f"check_results_{check_target.lower()}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
    
    else:
        batch_file = st.file_uploader("Upload CSV with IDs to check", type=["csv", "txt"], key="check_file")
        
        if batch_file:
            try:
                batch_df = pd.read_csv(batch_file, dtype=str)
            except Exception as e:
                st.error(f"Could not read CSV: {e}")
                st.stop()
            
            found_cols_map, _ = find_required_columns(batch_df.columns.tolist(), [SEARCH_COLUMN])
            
            if SEARCH_COLUMN not in found_cols_map:
                id_column = batch_df.columns[0]
                st.warning(f"Using column '{id_column}' as ID column")
            else:
                id_column = found_cols_map[SEARCH_COLUMN]
            
            batch_ids = batch_df[id_column].astype(str).str.strip().tolist()
            
            st.info(f"📊 Found {len(batch_ids)} IDs to check")
            
            if st.button("🔎 Check All", use_container_width=True):
                with st.spinner("Checking all IDs..."):
                    results_df = check_ids_batch(batch_ids, CHECK_TABLE, SEARCH_COLUMN)
                    
                    col_exists, col_not_exists = st.columns(2)
                    with col_exists:
                        exists_count = (results_df["Exists"] == "✅ Yes").sum()
                        st.metric("Found", exists_count, f"{(exists_count/len(results_df)*100):.1f}%")
                    with col_not_exists:
                        not_exists_count = (results_df["Exists"] == "❌ No").sum()
                        st.metric("Not Found", not_exists_count, f"{(not_exists_count/len(results_df)*100):.1f}%")
                    
                    st.dataframe(results_df, use_container_width=True)
                    
                    csv = results_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Results",
                        data=csv,
                        file_name=f"check_results_{check_target.lower()}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
