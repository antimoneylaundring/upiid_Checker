import streamlit as st
import pandas as pd
import re
from supabase import create_client, Client

SUPABASE_URL = "https://zekvwyaaefjtjqjolsrm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inpla3Z3eWFhZWZqdGpxam9sc3JtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjIyNDA4NTksImV4cCI6MjA3NzgxNjg1OX0.wXT_VnXuEZ2wtHSJMR9VJAIv_mtXGQdu0jy0m9V2Gno"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ====== PAGE TITLE ======
st.title("Total Database Summary")

st.markdown("UPI & Bank Data")

# ====== Fetch counts from Supabase ======
try:
    # --- Fetch total UPI count ---
    upi_response = supabase.table("all_upiiD").select("Upi_vpa", count="exact").execute()
    upi_count = upi_response.count if hasattr(upi_response, "count") else len(upi_response.data)

    # --- Fetch total Bank count ---
    bank_response = supabase.table("all_bank_acc").select("Bank_account_number", count="exact").execute()
    bank_count = bank_response.count if hasattr(bank_response, "count") else len(bank_response.data)

    # ====== Display totals ======
    col1, col2 = st.columns(2)
    with col1:
        st.metric(label="Total UPI IDs", value=f"{upi_count:,}")
    with col2:
        st.metric(label="Total Bank Accounts", value=f"{bank_count:,}")

    st.markdown("---")

    # ====== Optional: show small summary ======
    st.info(f"There are currently **{upi_count:,}** unique UPI IDs and **{bank_count:,}** bank accounts stored in Supabase.")

except Exception as e:
    st.error(f"Failed to fetch data: {e}")