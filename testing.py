import streamlit as st
import pandas as pd
from datetime import timedelta
from io import BytesIO
from supabase import create_client, Client
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.utils import get_column_letter
import streamlit.components.v1 as components

st.markdown("""
<style>
/* REMOVE default Streamlit page centering */
.block-container {
    padding-top: 1rem !important;
    padding-left: 1rem !important;
    padding-right: 1rem !important;
    max-width: 100% !important;
}

/* FIX iframe shrinking issue */
iframe {
    width: 100% !important;
    display: block !important;
    margin: 0 !important;
}

/* TABLE container should be full width */
.st-emotion-cache-1kyxreq, .st-emotion-cache-1r6slbn {
    width: 100% !important;
}

/* REMOVE center alignment inside iframe */
html, body {
    margin: 0;
    padding: 0;
    width: 100% !important;
    overflow-x: hidden !important;
}

/* Table itself must be 100% width */
.excel-table {
    width: 100% !important;
    table-layout: fixed !important;
}
</style>
""", unsafe_allow_html=True)


# ====== SUPABASE CONFIG ======
SUPABASE_URL = "https://zekvwyaaefjtjqjolsrm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inpla3Z3eWFhZWZqdGpxam9sc3JtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjIyNDA4NTksImV4cCI6MjA3NzgxNjg1OX0.wXT_VnXuEZ2wtHSJMR9VJAIv_mtXGQdu0jy0m9V2Gno"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ====== STREAMLIT UI ======
st.title("UPI, Bank & Website Summary")

uploaded_file = st.file_uploader("Upload Excel or CSV File", type=["xlsx", "xls", "csv"])

if uploaded_file:
    # --- Load file ---
    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_excel(uploaded_file)

    st.success(f"File Loaded: {uploaded_file.name}")
    df.columns = df.columns.str.strip()

    # --- Validate required columns ---
    required_cols = ["Feature_type", "Approvd_status", "Input_user", "Inserted_date", "Website_url", "Upi_vpa", "Bank_account_number"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        st.error(f"Missing columns: {missing}")
        st.stop()

    # --- Apply filter conditions ---
    filtered_df = df[
        (df["Feature_type"].astype(str).str.strip() == "BS Money Laundering") &
        (df["Approvd_status"] == 1) &
        (df["Input_user"].astype(str).str.strip().str.lower() != "automated") &
        (df["Search_for"].astype(str).str.strip().isin(["App", "Web"])) & 
        (df["Upi_bank_account_wallet"].astype(str).str.strip().isin(["UPI", "Bank Account"]))
    ].copy()

    if filtered_df.empty:
        st.warning("No records found after applying filter conditions.")
        st.stop()

    st.info(f"{len(filtered_df)} rows matched filter conditions")

    # --- Clean UPI values ---
    def clean_val(x):
        if pd.isna(x):
            return None
        return str(x).strip().lower().replace(" ", "")

    filtered_df["Upi_vpa_clean"] = filtered_df["Upi_vpa"].apply(clean_val)
    filtered_df["Bank_acc_clean"] = filtered_df["Bank_account_number"].apply(clean_val)
    filtered_df["Inserted_date"] = pd.to_datetime(filtered_df["Inserted_date"], errors="coerce").dt.date
    filtered_df['Website_url'] = filtered_df['Website_url'].apply(clean_val)

    # --- Fetch all UPIs from Supabase ---
    all_upis = list(filtered_df["Upi_vpa_clean"].dropna().unique())

    if all_upis:
        # Fetch all existing UPIs from Supabase
        existing_upis = set()
        for i in range(0, len(all_upis), 1000):  # batching
            batch = all_upis[i:i+1000]
            data = supabase.table("all_upiiD").select("Upi_vpa").in_("Upi_vpa", batch).execute()
            if data.data:
                existing_upis.update({d["Upi_vpa"].strip().lower() for d in data.data if d.get("Upi_vpa")})
        
        # Now find UPIs NOT found in Supabase (i.e. new)
        not_found_upis = set(all_upis) - existing_upis
    else:
        not_found_upis = set()
    
    # --- Fetch existing Banks from Supabase ---
    all_banks = list(filtered_df["Bank_acc_clean"].dropna().unique())
    not_found_banks = set()
    if all_banks:
        existing_banks = set()
        for i in range(0, len(all_banks), 1000):
            batch = all_banks[i:i+1000]
            data = supabase.table("all_bank_acc").select("Bank_account_number").in_("Bank_account_number", batch).execute()
            if data.data:
                existing_banks.update({d["Bank_account_number"].strip().lower() for d in data.data if d.get("Bank_account_number")})
        not_found_banks = set(all_banks) - existing_banks

    # --- Group by date ---
    grouped = filtered_df.groupby("Inserted_date").agg(
        website_total =('Id', 'count'),
        Total_UPI=("Upi_vpa_clean", "count"),
        Unique_UPI=("Upi_vpa_clean", pd.Series.nunique),
        Bank_Total=("Bank_acc_clean", "count"),
        Bank_Unique=("Bank_acc_clean", pd.Series.nunique),
        unique_website = ('Website_url', pd.Series.nunique)
    ).reset_index()

    # --- Build summary ---
    summary_data = []

    for _, row in grouped.iterrows():
        date = row["Inserted_date"]
        website_total = row['website_total']
        total_upi = row["Total_UPI"]
        unique_upi = row["Unique_UPI"]
        unique_website = row['unique_website']

        # Count how many UPIs from this date are new
        date_upis = set(filtered_df.loc[filtered_df["Inserted_date"] == date, "Upi_vpa_clean"])
        new_upi_today = len(date_upis & not_found_upis)

        total_bank = row["Bank_Total"]
        unique_bank = row["Bank_Unique"]
        bank_new_today = len(set(filtered_df.loc[filtered_df["Inserted_date"] == date, "Bank_acc_clean"]) & not_found_banks)

        summary_data.append({
            "Date": date,
            "Total": website_total,
            "UPI_Total": total_upi,
            "UPI_Unique": unique_upi,
            "UPI_%": f"{(unique_upi / total_upi * 100):.0f}%" if total_upi else "0%",
            "UPI_New": new_upi_today,
            "UPI_New_%": f"{(new_upi_today / unique_upi * 100):.0f}%" if unique_upi else "0%",
            "Bank_Total": total_bank,
            "Bank_Unique": unique_bank,
            "Bank_%": f"{(unique_bank / total_bank * 100):.0f}%" if total_bank else "0%",
            "Bank_New": bank_new_today,
            "Bank_New_%": f"{(bank_new_today / unique_bank * 100):.0f}%" if unique_bank else "0%",
            "unique_website": unique_website
        })

    summary_df = pd.DataFrame(summary_data)
    
    st.subheader("📊 Summary Report")

    # Create styled HTML table
    html_table = """
        <style>
        .table-container {
            width: 100%;
            overflow-x: auto;
            margin: 0;
            padding: 0;
        }

        .excel-table {
            border-collapse: collapse;
            font-family: 'Segoe UI', sans-serif;
            font-size: 13px;  /* Reduced from 15px */
            width: 100% !important;
            table-layout: fixed !important;
            min-width: unset;  /* Remove min-width constraint */
        }

        .excel-table th, .excel-table td {
            border: 1px solid #ccc;
            text-align: center;
            padding: 6px 4px !important;  /* Reduced padding */
            white-space: normal !important;
            word-wrap: break-word !important;
            overflow: hidden;
        }

        /* Specific column widths for better control */
        .excel-table th:nth-child(1), .excel-table td:nth-child(1) { width: 8%; }   /* Date */
        .excel-table th:nth-child(2), .excel-table td:nth-child(2) { width: 5%; }   /* Total */
        .excel-table th:nth-child(3), .excel-table td:nth-child(3) { width: 5%; }   /* UPI Total */
        .excel-table th:nth-child(4), .excel-table td:nth-child(4) { width: 6%; }   /* UPI Unique */
        .excel-table th:nth-child(5), .excel-table td:nth-child(5) { width: 4%; }   /* UPI % */
        .excel-table th:nth-child(6), .excel-table td:nth-child(6) { width: 5%; }   /* UPI New */
        .excel-table th:nth-child(7), .excel-table td:nth-child(7) { width: 4%; }   /* UPI New % */
        .excel-table th:nth-child(8), .excel-table td:nth-child(8) { width: 7%; }   /* Bank Total */
        .excel-table th:nth-child(9), .excel-table td:nth-child(9) { width: 6%; }   /* Bank Unique */
        .excel-table th:nth-child(10), .excel-table td:nth-child(10) { width: 4%; } /* Bank % */
        .excel-table th:nth-child(11), .excel-table td:nth-child(11) { width: 5%; } /* Bank New */
        .excel-table th:nth-child(12), .excel-table td:nth-child(12) { width: 4%; } /* Bank New % */
        .excel-table th:nth-child(13), .excel-table td:nth-child(13) { width: 8%; } /* Unique Website */

        .excel-table thead tr:first-child th {
            background-color: #cbd5e1;
            font-size: 16px;  /* Reduced from 18px */
            font-weight: 700;
            padding: 8px;
        }

        .excel-table thead tr:nth-child(2) th {
            background-color: #cbd5e1;
            font-size: 14px;  /* Reduced from 16px */
            font-weight: 600;
        }

        .excel-table thead tr:nth-child(3) th {
            background-color: #e2e8f0;
            font-weight: 500;
            font-size: 12px;  /* Added smaller font for headers */
        }

        .excel-table td {
            background-color: #f8fafc;
        }
        </style>

        <div class="table-container">
        <table class="excel-table">
            <thead>
                <tr>
                    <th colspan="13">UPI, Bank & Website Report</th>
                </tr>

                <tr>
                    <th rowspan="2">Date</th>
                    <th rowspan="2">Total</th>
                    <th colspan="5">UPI</th>
                    <th colspan="5">Bank</th>
                    <th rowspan="2">Unique Website</th>
                </tr>

                <tr>
                    <th>Total</th><th>Unique</th><th>%</th><th>New</th><th>%</th>
                    <th>Total</th><th>Unique</th><th>%</th><th>New</th><th>%</th>
                </tr>
            </thead>

            <tbody>
    """

    for _, row in summary_df.iterrows():
        html_table += f"""
            <tr>
                <td>{row['Date']}</td>
                <td>{row['Total']}</td>
                <td>{row['UPI_Total']}</td>
                <td>{row['UPI_Unique']}</td>
                <td>{row['UPI_%']}</td>
                <td>{row['UPI_New']}</td>
                <td>{row['UPI_New_%']}</td>
                <td>{row['Bank_Total']}</td>
                <td>{row['Bank_Unique']}</td>
                <td>{row['Bank_%']}</td>
                <td>{row['Bank_New']}</td>
                <td>{row['Bank_New_%']}</td>
                <td>{row['unique_website']}</td>
            </tr>
        """

    html_table += "</tbody></table></div>"

    # Render the HTML
    components.html(
        html_table,
        height=450,
        scrolling=True
    )

    # --- Create formatted Excel output ---
    output = BytesIO()
    wb = Workbook()
    ws = wb.active
    ws.title = "Summary"

    # Header rows
    ws.append(["Date", "Total", "UPI", "", "", "", "", "Bank", "", "", "", "", "Unique Webiste"])
    ws.merge_cells("C1:G1")
    ws.merge_cells("H1:L1")
    ws["C1"].alignment = ws["H1"].alignment = Alignment(horizontal="center", vertical="center")
    ws["C1"].font = ws["H1"].font = Font(bold=True)

    ws.append(["Date", "Total", "Total", "Unique", "%", "New", "%", "Total bank", "Unique", "%", "New", "%", "Unique Webiste"])

    for r in dataframe_to_rows(summary_df, index=False, header=False):
        ws.append(r)

    for cell in ws[1] + ws[2]:
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.font = Font(bold=True)

    for i, col_cells in enumerate(ws.columns, 1):
        length = max(len(str(c.value)) if c.value else 0 for c in col_cells)
        ws.column_dimensions[get_column_letter(i)].width = length + 2

    wb.save(output)
    output.seek(0)

    st.download_button(
        label="Download Summary Excel",
        data=output,
        file_name="upi_summary_report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

else:
    st.info("📤 Please upload a file to generate the report.")
