from flask import Flask, render_template, request, send_file
import pandas as pd
import re
from supabase import create_client, Client
import io

# Function to clean & normalize UPI Strings
def clean_upi(value):
    value = str(value).lower().strip()
    value = re.sub(r'[\u200b\u200c\u200d\u2060]', '', value)  # remove invisible characters
    value = re.sub(r'\s+', '', value)  # remove spaces
    return value

SUPABASE_URL = 'https://zekvwyaaefjtjqjolsrm.supabase.co'
SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inpla3Z3eWFhZWZqdGpxam9sc3JtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjIyNDA4NTksImV4cCI6MjA3NzgxNjg1OX0.wXT_VnXuEZ2wtHSJMR9VJAIv_mtXGQdu0jy0m9V2Gno'  # ← keep same key
TABLE_NAME = 'all_upiiD'
DB_COLUMN = 'Upi_vpa'
EXCEL_COLUMN = 'Upi_vpa'

# Flask App
app = Flask(__name__)

# Connect Supabase
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        # Step 1: Read uploaded Excel
        file = request.files["file"]
        df = pd.read_excel(file)
        input_upi_ids = set(df[EXCEL_COLUMN].astype(str).apply(clean_upi))

        # Step 2: Fetch DB data with pagination
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

        # Step 3: Compute Not Matched UPI
        not_matched = input_upi_ids - db_upi_ids

        # Step 4: Save to Excel in memory
        output = pd.DataFrame(list(not_matched), columns=["Not_Matched_UPI"])
        buffer = io.BytesIO()
        output.to_excel(buffer, index=False)
        buffer.seek(0)

        return send_file(buffer, as_attachment=True, download_name="notMatch_upi_ids.xlsx")

    return render_template("index.html")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
