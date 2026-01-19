import streamlit as st
import psycopg2
import os
from dotenv import load_dotenv

# ===== LOAD ENV =====
load_dotenv()
DB_URL = os.getenv("DB_URL")

if DB_URL and 'postgresql+psycopg2://' in DB_URL:
    DB_URL = DB_URL.replace('postgresql+psycopg2://', 'postgresql://')

if not DB_URL:
    st.error("DB_URL not found in .env file")
    st.stop()

# ===== PAGE TITLE =====
st.title("Total Database Summary")
st.markdown("UPI & Bank Data")

# ===== DB CONNECTION FUNCTION =====
def get_connection():
    return psycopg2.connect(
        DB_URL,
        connect_timeout=10,
        sslmode="require"
    )

# ===== FETCH COUNTS =====
try:
    conn = get_connection()
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
        st.metric("Total UPI IDs", f"{upi_count:,}")

    with col2:
        st.metric("Total Bank Accounts", f"{bank_count:,}")

    st.markdown("---")

    st.info(
        f"There are currently **{upi_count:,}** unique UPI IDs and "
        f"**{bank_count:,}** bank accounts stored in Nhost database."
    )

except Exception as e:
    st.error("Failed to fetch data from Nhost DB")
    st.exception(e)
