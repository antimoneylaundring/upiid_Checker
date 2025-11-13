import pandas as pd
from datetime import timedelta
import streamlit as st

# ===== CONFIG =====
INPUT_FILE = r"C:\Users\Acer\Downloads\table__v3_scraper_merchantlaundering_data_table customer__Mystery Shopping date__20251030 (1).csv"
MAPPING_FILE = r"C:\Users\Acer\Downloads\Unique_UPI&Bank_Repo_29-10-2025.xlsx"
OUTPUT_FILE = r"C:\Users\Acer\Downloads\report_output.xlsx"

# ===== READ INPUT FILE =====
df = pd.read_csv(INPUT_FILE, low_memory=False)
df.columns = df.columns.str.strip()

# ===== READ MAPPING FILES =====
mapping_upi = pd.read_excel(MAPPING_FILE, sheet_name="UPI")
mapping_bank = pd.read_excel(MAPPING_FILE, sheet_name="Bank")
rules_df = pd.read_excel(MAPPING_FILE, sheet_name="Rules")

mapping_upi.columns = mapping_upi.columns.str.strip()
mapping_bank.columns = mapping_bank.columns.str.strip()
rules_df.columns = rules_df.columns.str.strip()

# ===== CLEANUP FUNCTION =====
def clean_value(x):
    if pd.isna(x):
        return None
    return str(x).strip().lower().replace(" ", "").replace(".", "").replace(",", "")

# ===== APPLY RULES DYNAMICALLY =====
def apply_rules(df, rules_df):
    for _, rule in rules_df.iterrows():
        col = rule['Column_name']
        op = str(rule['Operator']).strip().lower()
        vals = [v.strip().lower() for v in str(rule['Values']).split(',') if v.strip()]

        if col not in df.columns:
            print(f"Warning: Column '{col}' not found in dataframe. Skipping rule.")
            continue

        df[col] = df[col].astype(str).str.lower().str.strip()

        if op == 'equals':
            df = df[df[col].isin(vals)]
        elif op == 'not equals':
            df = df[~df[col].isin(vals)]
        elif op == 'in':
            df = df[df[col].isin(vals)]
        elif op == 'not in':
            df = df[~df[col].isin(vals)]
        elif op == 'contains':
            pattern = '|'.join(vals)
            df = df[df[col].str.contains(pattern, case=False, na=False)]
        elif op == 'not contains':
            pattern = '|'.join(vals)
            df = df[~df[col].str.contains(pattern, case=False, na=False)]
        else:
            print(f"Unknown operator '{op}' for column '{col}'. Skipping.")
    return df

# ===== CLEAN & NORMALIZE DATA =====
df['Upi_bank_account_wallet'] = df['Upi_bank_account_wallet'].astype(str).str.strip().str.lower()
df['Upi_vpa_clean'] = df['Upi_vpa'].apply(clean_value)
df['Bank_account_number_clean'] = df['Bank_account_number'].apply(clean_value)
df['Website_url_clean'] = df['Website_url'].apply(clean_value)

mapping_upi['Upi_vpa_clean'] = mapping_upi['Upi_vpa'].apply(clean_value)
mapping_bank['Bank_account_number_clean'] = mapping_bank['Bank_account_number'].apply(clean_value)

df['Inserted_date'] = pd.to_datetime(df['Inserted_date'], errors='coerce').dt.date
mapping_upi['Inserted_date'] = pd.to_datetime(mapping_upi['Inserted_date'], errors='coerce').dt.date
mapping_bank['Inserted_date'] = pd.to_datetime(mapping_bank['Inserted_date'], errors='coerce').dt.date

# ===== APPLY RULES FROM EXCEL =====
df = apply_rules(df, rules_df)

# ===== SPLIT DATA =====
df_upi = df[df['Upi_bank_account_wallet'] == 'upi'].copy()
df_bank = df[df['Upi_bank_account_wallet'] == 'bank account'].copy()

# ===== BASE COUNTS =====
total_group = df.groupby('Inserted_date').size().reset_index(name='Total Count')

upi_group = df_upi.groupby('Inserted_date').agg(
    Total_UPI=('Upi_vpa_clean', 'count'),
    Unique_UPI=('Upi_vpa_clean', pd.Series.nunique)
).reset_index()

bank_group = df_bank.groupby('Inserted_date').agg(
    Total_Bank=('Bank_account_number_clean', 'count'),
    Unique_Bank=('Bank_account_number_clean', pd.Series.nunique)
).reset_index()

website_group = df.groupby('Inserted_date').agg(
    Unique_Website=('Website_url_clean', pd.Series.nunique)
).reset_index()

# ===== FIND NEW UPI COUNT =====
new_upi_list, new_upi_details = [], []
for current_date, group in df_upi.groupby('Inserted_date'):
    prev_date = current_date - timedelta(days=1)
    known_upis = set(mapping_upi.loc[mapping_upi['Inserted_date'] <= prev_date, 'Upi_vpa_clean'].dropna())
    new_upis_today = set(group['Upi_vpa_clean'].dropna()) - known_upis
    new_upi_list.append({'Inserted_date': current_date, 'New_UPI': len(new_upis_today)})
    for u in new_upis_today:
        new_upi_details.append({'Inserted_date': current_date, 'New_Upi_vpa_clean': u})

new_upi_group = pd.DataFrame(new_upi_list)
new_upi_df = pd.DataFrame(new_upi_details)

# ===== FIND NEW BANK ACCOUNT COUNT =====
new_bank_list, new_bank_details = [], []
for current_date, group in df_bank.groupby('Inserted_date'):
    prev_date = current_date - timedelta(days=1)
    known_banks = set(mapping_bank.loc[mapping_bank['Inserted_date'] <= prev_date, 'Bank_account_number_clean'].dropna())
    new_banks_today = set(group['Bank_account_number_clean'].dropna()) - known_banks
    new_bank_list.append({'Inserted_date': current_date, 'New_Bank': len(new_banks_today)})
    for b in new_banks_today:
        new_bank_details.append({'Inserted_date': current_date, 'New_Bank_account_number_clean': b})

new_bank_group = pd.DataFrame(new_bank_list)
new_bank_df = pd.DataFrame(new_bank_details)

# ===== MERGE ALL =====
report = total_group.merge(upi_group, on='Inserted_date', how='left')
report = report.merge(bank_group, on='Inserted_date', how='left')
report = report.merge(new_upi_group, on='Inserted_date', how='left')
report = report.merge(new_bank_group, on='Inserted_date', how='left')
report = report.merge(website_group, on='Inserted_date', how='left')

# ===== FILL & CONVERT =====
report = report.fillna(0)
for col in ['Total Count', 'Total_UPI', 'Unique_UPI', 'New_UPI', 'Total_Bank', 'Unique_Bank', 'New_Bank', 'Unique_Website']:
    report[col] = report[col].astype(int)

# ===== PERCENTAGE CALCULATIONS =====
report['Unique UPI %']  = (report['Unique_UPI'] / report['Total_UPI'] * 100).round(0).astype(str) + '%'
report['New UPI %']     = (report['New_UPI'] / report['Total_UPI'] * 100).round(0).astype(str) + '%'
report['Unique Bank %'] = (report['Unique_Bank'] / report['Total_Bank'] * 100).round(0).astype(str) + '%'
report['New Bank %']    = (report['New_Bank'] / report['Total_Bank'] * 100).round(0).astype(str) + '%'

# ===== FINAL REPORT =====
report = report.rename(columns={'Inserted_date': 'Date'})
report = report[
    ['Date', 'Total Count', 'Total_UPI', 'Unique_UPI', 'Unique UPI %',
     'New_UPI', 'New UPI %', 'Total_Bank', 'Unique_Bank', 'Unique Bank %',
     'New_Bank', 'New Bank %', 'Unique_Website']
]

# ===== DETAIL SHEETS =====
upi_details = df_upi[['Inserted_date', 'Upi_vpa', 'Upi_vpa_clean']].drop_duplicates().reset_index(drop=True)
bank_details = df_bank[['Inserted_date', 'Bank_account_number', 'Bank_account_number_clean']].drop_duplicates().reset_index(drop=True)

# ===== WRITE TO EXCEL =====
with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
    report.to_excel(writer, index=False, sheet_name='Summary_Report')
    upi_details.to_excel(writer, index=False, sheet_name='UPI_Details')
    bank_details.to_excel(writer, index=False, sheet_name='Bank_Details')
    mapping_upi.to_excel(writer, index=False, sheet_name='Mapping_UPI_Data')
    mapping_bank.to_excel(writer, index=False, sheet_name='Mapping_Bank_Data')
    new_upi_df.to_excel(writer, index=False, sheet_name='New_UPIs')
    new_bank_df.to_excel(writer, index=False, sheet_name='New_Bank_Accounts')
    rules_df.to_excel(writer, index=False, sheet_name='Rules_Used')

print(f"Report generated successfully: {OUTPUT_FILE}")
