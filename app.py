import streamlit as st
import pandas as pd
import re
from supabase import create_client, Client
from io import BytesIO

# Function to clean & normalize UPI Strings
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

st.title("UPI ID Database Checker")

uploaded_file = st.file_uploader("Upload Excel File (.xlsx)", type=["xlsx"])

if uploaded_file:
    df = pd.read_excel(uploaded_file)

    if EXCEL_COLUMN not in df.columns:
        st.error(f"Column '{EXCEL_COLUMN}' not found in uploaded file.")
    else:
        # Clean input UPI list
        input_upi_ids = set(df[EXCEL_COLUMN].astype(str).apply(clean_upi))

        st.write(f"Total UPI in File: **{len(input_upi_ids)}**")

        # Query DB only for input values (chunked fast lookup)
        db_upi_ids = set()
        input_list = list(input_upi_ids)
        chunk_size = 8000

        st.info("Checking UPI IDs in Database... please wait")

        for i in st.progress(range(0, len(input_list), chunk_size)):
            pass
        
        with st.spinner("Comparing..."):
            for i in range(0, len(input_list), chunk_size):
                chunk = input_list[i:i + chunk_size]
                response = (
                    supabase.table(TABLE_NAME)
                    .select(DB_COLUMN)
                    .in_(DB_COLUMN, chunk)
                    .execute()
                )
                if response.data:
                    for row in response.data:
                        db_upi_ids.add(clean_upi(row[DB_COLUMN]))

        # Compute NOT MATCHED
        not_matched = input_upi_ids - db_upi_ids

        st.subheader("UPI IDs NOT Found in Database:")
        st.write(f"**Total Not Found:** {len(not_matched)}")

        if len(not_matched) == 0:
            st.success("All UPI IDs already exist in the database.")
        else:
            st.warning("Some UPI IDs are missing in DB.")
            st.write(not_matched)

            # Download file
            result_df = pd.DataFrame(list(not_matched), columns=["Not_Matched_UPI"])
            output = BytesIO()
            result_df.to_excel(output, index=False)
            output.seek(0)

            st.download_button(
                label="Download Not Matched List",
                data=output,
                file_name="notMatch_upi_ids.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
