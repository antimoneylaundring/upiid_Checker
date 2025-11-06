import pandas as pd
import re
from supabase import create_client, Client

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

# 1. Read Excel file
df = pd.read_excel("input_upi_id.xlsx")

# Clean input UPI values
input_upi_ids = set(df[EXCEL_COLUMN].astype(str).apply(clean_upi))

# 2. Connect Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# 3. Fetch ALL DB UPI values using pagination
all_rows = []
page_size = 1000
start = 0

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

# 4. Compute NOT MATCHED UPI ids (Excel → DB missing)
not_matched = input_upi_ids - db_upi_ids

print("\nUPI IDs NOT found in Database:\n")

if not not_matched:
    print("All UPI IDs are already present in database.")
else:
    for upi in not_matched:
        print(upi)

# 5. Save to Excel
pd.DataFrame(list(not_matched), columns=["Not_Matched_UPI"]).to_excel("notMatch_upi_ids.xlsx", index=False)

print("\nFile Saved: notMatch_upi_ids.xlsx")
