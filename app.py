import streamlit as st
import pandas as pd
import re
from supabase import create_client, Client
from io import BytesIO

def clean_upi(value):
    value = str(value).lower().strip()
    value = re.sub(r'[\u200b\u200c\u200d\u2060]', '', value)  # remove invisible characters
    value = re.sub(r'\s+', '', value)  # remove spaces
    return value

SUPABASE_URL = 'https://zekvwyaaefjtjqjolsrm.supabase.co'
SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inpla3Z3eWFhZWZqdGpxam9sc3JtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjIyNDA4NTksImV4cCI6MjA3NzgxNjg1OX0.wXT_VnXuEZ2wtHSJMR9VJAIv_mtXGQdu0jy0m9V2Gno'

TABLE_NAME = 'all_upiiD'
DB_COLUMN = 'Upi_vpa'
EXCEL_COLUMN = 'Upi_vpa'

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

st.title("UPI ID Database Checker (Fast Mode)")

uploaded_file = st.file_uploader("Upload Excel File (.xlsx)", type=["xlsx"])

if uploaded_file:

    df = pd.read_excel(uploaded_file)

    if EXCEL_COLUMN not in df.columns:
        st.error(f"Column '{EXCEL_COLUMN}' not found in uploaded file.")
    else:
        input_upi_ids = set(df[EXCEL_COLUMN].astype(str).apply(clean_upi))

        with st.spinner("Fetching UPI list from database (optimized SQL)..."):
            sql_query = f"SELECT {DB_COLUMN} FROM {TABLE_NAME};"
            db_data = supabase.rpc("execute_sql", {"query": sql_query}).execute()

        if not db_data.data:
            st.error("Database returned no data. Check table name/permissions.")
        else:
            db_upi_ids = set(clean_upi(row[DB_COLUMN]) for row in db_data.data)

            not_matched = input_upi_ids - db_upi_ids

            st.subheader("UPI IDs NOT Found in Database:")

            if not not_matched:
                st.success("All UPI IDs are already present in the database.")
            else:
                st.warning(f"Total Missing: {len(not_matched)}")
                st.dataframe(pd.DataFrame(list(not_matched), columns=["Not_Matched_UPI"]))

                # Download Excel
                result_df = pd.DataFrame(list(not_matched), columns=["Not_Matched_UPI"])
                output = BytesIO()
                result_df.to_excel(output, index=False)
                output.seek(0)

                st.download_button(
                    label="⬇ Download Missing UPI List",
                    data=output,
                    file_name="notMatch_upi_ids.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

