import streamlit as st
import pandas as pd
import re
from supabase import create_client, Client
from io import BytesIO

# ------------------- CLEAN UPI FUNCTION -------------------
def clean_upi(value):
    value = str(value).lower().strip()
    value = re.sub(r'[\u200b\u200c\u200d\u2060]', '', value)  # remove invisible characters
    value = re.sub(r'\s+', '', value)  # remove spaces
    return value

SUPABASE_URL = "https://zekvwyaaefjtjqjolsrm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inpla3Z3eWFhZWZqdGpxam9sc3JtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjIyNDA4NTksImV4cCI6MjA3NzgxNjg1OX0.wXT_VnXuEZ2wtHSJMR9VJAIv_mtXGQdu0jy0m9V2Gno"

st.header("Import UPI Data into Supabase (Bulk Insert)")
uploaded_file = st.file_uploader("Upload Excel File", type=["xlsx", "xls"])

if uploaded_file:
    df = pd.read_excel(uploaded_file)

    # Ensure required columns exist
    if not {"InsertDate", "Upi_vpa"}.issubset(df.columns):
        st.error("Excel must contain columns: InsertDate, Upi_vpa")
    else:
        # Clean UPI values
        df["Upi_vpa"] = df["Upi_vpa"].apply(clean_upi)

        st.write("### Preview Data")
        st.dataframe(df)

        # ---------- BULK INSERT BUTTON ----------
        if st.button("Bulk Insert into Supabase"):
            try:
                data_to_insert = df.to_dict(orient="records")

                #BULK INSERT (super fast)
                supabase.table("upi_table").insert(data_to_insert).execute()

                st.success(f"Successfully inserted {len(data_to_insert)} records into Supabase!")
            except Exception as e:
                st.error(f"Error inserting data: {str(e)}")

TABLE_NAME = "all_upiiD"
DB_COLUMN = "Upi_vpa"
EXCEL_COLUMN = "Upi_vpa"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

st.title("UPI ID Database Checker")

uploaded_file = st.file_uploader("Upload Excel File (.xlsx)", type=["xlsx"])

if uploaded_file:
    df = pd.read_excel(uploaded_file)

    if EXCEL_COLUMN not in df.columns:
        st.error(f"Column '{EXCEL_COLUMN}' not found in uploaded file.")
    else:
        # Clean input UPI values
        input_upi_ids = set(df[EXCEL_COLUMN].astype(str).apply(clean_upi))
        input_list = list(input_upi_ids)

        with st.spinner("Checking UPI IDs in database (This is fast)..."):
            response = supabase.rpc("get_missing_upi", {"input_array": input_list}).execute()

        not_matched = {row["missing_upi"] for row in response.data if row["missing_upi"]}

        st.subheader("UPI IDs NOT Found in Database:")

        if not not_matched:
            st.success("All UPI IDs are already present in the database.")
        else:
            st.warning(f"Total Not Found: {len(not_matched)}")
            st.write(not_matched)

            # Create downloadable Excel
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
