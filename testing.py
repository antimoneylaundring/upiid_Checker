import streamlit as st
import pandas as pd
import re
from supabase import create_client, Client
from io import BytesIO

# -------------------------
# Function to clean & normalize UPI Strings
# -------------------------
def clean_upi(value):
    value = str(value).lower().strip()
    value = re.sub(r'[\u200b\u200c\u200d\u2060]', '', value)  # remove invisible characters
    value = re.sub(r'\s+', '', value)  # remove spaces
    return value

# -------------------------
# Supabase Config
# -------------------------
SUPABASE_URL = 'https://zekvwyaaefjtjqjolsrm.supabase.co'
SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inpla3Z3eWFhZWZqdGpxam9sc3JtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjIyNDA4NTksImV4cCI6MjA3NzgxNjg1OX0.wXT_VnXuEZ2wtHSJMR9VJAIv_mtXGQdu0jy0m9V2Gno'

TABLE_NAME = 'all_upiiD'
DB_COLUMN = 'Upi_vpa'
EXCEL_COLUMN = 'Upi_vpa'

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -------------------------
# Streamlit UI
# -------------------------
st.title("UPI ID Database Checker")

uploaded_file = st.file_uploader("Upload Excel File (.xlsx)", type=["xlsx"])

if uploaded_file:
    df = pd.read_excel(uploaded_file)

    if EXCEL_COLUMN not in df.columns:
        st.error(f"Column '{EXCEL_COLUMN}' not found in uploaded file.")
    else:
        # Clean input UPI values
        input_upi_ids = set(df[EXCEL_COLUMN].astype(str).apply(clean_upi))

        # Fetch DB UPI data
        all_rows = []
        page_size = 1000
        start = 0

        with st.spinner("Fetching data from Database..."):
            while True:
                response = supabase.table(TABLE_NAME).select(DB_COLUMN).range(start, start + page_size - 1).execute()
                data = response.data
                if not data:
                    break
                all_rows.extend(data)
                if len(data) < page_size:
                    break
                start += page_size

        db_upi_ids = set(clean_upi(row[DB_COLUMN]) for row in all_rows)

        # Compare
        not_matched = input_upi_ids - db_upi_ids

        st.subheader("UPI IDs NOT Found in Database:")
        if not not_matched:
            st.success("All UPI IDs are already present in the database.")
        else:
            st.warning(f"Total Not Found: {len(not_matched)}")
            st.write(not_matched)

            # Convert to downloadable Excel
            result_df = pd.DataFrame(list(not_matched), columns=["Not_Matched_UPI"])
            output = BytesIO()
            result_df.to_excel(output, index=False)
            output.seek(0)

            st.download_button(
                label="Download Not Matched UPI List",
                data=output,
                file_name="notMatch_upi_ids.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
