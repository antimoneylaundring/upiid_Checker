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

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "all_upiiD"
DB_COLUMN = "Upi_vpa"
EXCEL_COLUMN = "Upi_vpa"

st.set_page_config(page_title="UPI Checker", layout="wide")
st.markdown("""
<style>
    body {
        background-color: #f5f7fa !important;
    }
    .main {
        background: rgba(255,255,255,0.55) !important;
        backdrop-filter: blur(10px) !important;
        border-radius: 18px;
        padding: 20px !important;
        margin-top: 18px;
        border: 1px solid rgba(255,255,255,0.2) !important;
        box-shadow: 0 8px 28px rgba(0,0,0,0.1);
    }

    h1, h2, h3, h4 {
        font-size: 20px;
        font-family: 'Segoe UI', sans-serif !important;
        font-weight: 600 !important;
        color: #1d3557 !important;
        font-size: 33px !important;
        padding: 0px !important;
    }

    .stButton>button {
        font-size: 15px;
        font-weight: 600;
        border-radius: 8px;
        padding: 10px 20px;
        border: 0px;
        background: linear-gradient(135deg, #457b9d, #1d3557);
        color: white;
        box-shadow: 0px 4px 8px rgba(0,0,0,0.15);
        transition: 0.3s;
    }
    .stButton>button:hover {
        transform: scale(1.04);
        background: linear-gradient(135deg, #1d3557, #457b9d);
    }

    .stFileUploader label div {
        background:#edf2f7 !important;
        padding: 12px;
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)

st.title("UPI Database Tool")

left, right = st.columns([1, 1.1])

with left:
    # st.markdown("<div class='main'>", unsafe_allow_html=True)
    st.write("-----")
    st.header("Import UPI Data into Database")
    uploaded_insert_file = st.file_uploader("Upload Excel File for Insert", type=["xlsx", "xls"])

    if uploaded_insert_file:
        df_insert = pd.read_excel(uploaded_insert_file)

        # Ensure required columns exist
        if not {"Inserted_date", "Upi_vpa"}.issubset(df_insert.columns):
            st.error("Excel must contain both columns: InsertDate, Upi_vpa")
        else:
            # Clean UPI values
            df_insert["Upi_vpa"] = df_insert["Upi_vpa"].apply(clean_upi)

            # Ensure InsertDate format is only date (no time)
            df_insert["Inserted_date"] = pd.to_datetime(df_insert["Inserted_date"]).dt.strftime("%Y-%m-%d")

            st.write("### Preview Data")
            st.dataframe(df_insert)

            if st.button("Bulk Insert into Supabase"):
                try:
                    data_to_insert = df_insert.to_dict(orient="records")
                    supabase.table(TABLE_NAME).insert(data_to_insert).execute()
                    st.success(f"Successfully inserted {len(data_to_insert)} records into Supabase!")
                except Exception as e:
                    st.error(f"Error inserting data: {str(e)}")              

with right:
    st.write("-----")
    st.title("UPI ID Checker")

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