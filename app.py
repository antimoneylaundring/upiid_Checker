import pandas as pd
import re
from supabase import create_client, Client

def clean_upi(value):
    value = str(value).lower().strip()
    value = re.sub(r'[\u200b\u200c\u200d\u2060]', '', value)
    value = re.sub(r'\s+', '', value)
    return value

SUPABASE_URL = 'https://zekvwyaaefjtjqjolsrm.supabase.co'
SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inpla3Z3eWFhZWZqdGpxam9sc3JtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjIyNDA4NTksImV4cCI6MjA3NzgxNjg1OX0.wXT_VnXuEZ2wtHSJMR9VJAIv_mtXGQdu0jy0m9V2Gno'

TABLE_NAME = 'all_upiiD'
DB_COLUMN = 'Upi_vpa'
EXCEL_COLUMN = 'Upi_vpa'

# Read Excel file
df = pd.read_excel("input_upi_id.xlsx")
input_upi_ids = set(df[EXCEL_COLUMN].astype(str).apply(clean_upi))

# Connect Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Query DB only for input UPI IDs in chunks
input_list = list(input_upi_ids)
chunk_size = 8000  # safe chunk size
db_upi_ids = set()

for i in range(0, len(input_list), chunk_size):
    chunk = input_list[i:i + chunk_size]
    response = supabase.table(TABLE_NAME).select(DB_COLUMN).in_(DB_COLUMN, chunk).execute()
    
    if response.data:
        for row in response.data:
            db_upi_ids.add(clean_upi(row[DB_COLUMN]))

# Find UPI not present in DB
not_matched = input_upi_ids - db_upi_ids

print("\nUPI IDs NOT found in Database:\n")
if not not_matched:
    print("All UPI IDs are present in the database.")
else:
    for upi in not_matched:
        print(upi)

# Save to Excel
pd.DataFrame(list(not_matched), columns=["Not_Matched_UPI"]).to_excel("notMatch_upi_ids.xlsx", index=False)
print("\nFile Saved: notMatch_upi_ids.xlsx")
