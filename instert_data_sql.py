import re
import os
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_NAME = "all_bank_acc"  # your target table

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def extract_data_from_sql(sql_file):
    """Extract only inserted_date and bank_account_number from SQL"""
    with open(sql_file, 'r', encoding='utf-8', errors='ignore') as f:
        text = f.read()

    pattern = re.compile(
        r"INSERT INTO v3_scraper_merchantlaundering_data_table \([^)]*\) VALUES\s*(.*?);",
        re.DOTALL
    )

    all_rows = []
    for match in pattern.finditer(text):
        values_block = match.group(1)
        tuples = re.findall(r"\((.*?)\)", values_block, re.DOTALL)
        for tup in tuples:
            parts = re.findall(r"'((?:''|[^'])*)'", tup)
            if len(parts) >= 7:
                bank_account = parts[0]
                if bank_account != '' and bank_account != 'NA':
                    all_rows.append({
                        'Inserted_date': parts[4],
                        'Bank_account_number': bank_account
                    })
    return all_rows

def dedupe_rows_keep_latest(rows):
    """
    If input rows contain duplicate Bank_account_number, keep the row with the latest Inserted_date.
    Assumes Inserted_date is parseable lexicographically (ISO-like) or uses last-seen fallback.
    """
    # If dates have non-uniform formats, you can parse with datetime; for now keep last lexicographically greatest
    by_acc = {}
    for r in rows:
        acc = r['Bank_account_number']
        date = r.get('Inserted_date', '')
        existing = by_acc.get(acc)
        if existing is None or date > existing.get('Inserted_date', ''):
            by_acc[acc] = r
    return list(by_acc.values())

def fetch_existing_accounts(table_name):
    """Fetch existing bank account numbers from the table (returns a set)."""
    try:
        res = supabase.table(table_name).select("Bank_account_number").execute()
        # supabase-py returns a result with .data and .error in many versions; handle both
        data = None
        if hasattr(res, "data"):
            data = res.data
        elif isinstance(res, dict):
            data = res.get("data")
        else:
            data = res
        existing = set()
        if data:
            for row in data:
                # row might be a dict like {'Bank_account_number': '...'}
                # adapt if column name is different/case-sensitive in your DB
                val = row.get("Bank_account_number") or row.get("bank_account_number")
                if val:
                    existing.add(val)
        return existing
    except Exception as e:
        print("[WARN] Failed to fetch existing accounts (continuing without pre-filter):", e)
        return set()

def insert_or_update_in_batches(sql_file, table_name, batch_size=5000, prefetch_existing=True):
    """UPSERT: Insert new, Update if duplicate using on_conflict param"""
    print("Extracting data from SQL file...")
    rows = extract_data_from_sql(sql_file)
    print(f"[OK] Total raw rows extracted: {len(rows)}")

    # dedupe incoming rows so batches don't contain duplicates
    rows = dedupe_rows_keep_latest(rows)
    total_rows = len(rows)
    print(f"[OK] After dedupe (keep latest per account): {total_rows}")
    print(f"[INFO] Columns: Inserted_date, Bank_account_number")
    print(f"[INFO] Mode: UPSERT via on_conflict (insert new, update if duplicate)\n")

    if total_rows == 0:
        print("[ERROR] No valid rows found")
        return False

    existing_accounts = set()
    if prefetch_existing:
        existing_accounts = fetch_existing_accounts(table_name)
        print(f"[INFO] Existing accounts in DB fetched: {len(existing_accounts)}")
    else:
        print("[INFO] Skipping prefetch of existing accounts (may cause more upsert attempts)")

    # Option A (recommended): filter out accounts we already know exist,
    # so we only upsert new rows or rely on on_conflict to update existing.
    # If you want to update existing rows with new Inserted_date, keep them and rely on on_conflict.
    # Here I'll keep rows that are new OR if you want to update existing, comment out the filter.
    filtered_rows = []
    for r in rows:
        if r['Bank_account_number'] not in existing_accounts:
            filtered_rows.append(r)
        else:
            # If you want to update existing rows as well, uncomment this line:
            # filtered_rows.append(r)
            pass

    print(f"[INFO] Rows to attempt upsert after prefilter: {len(filtered_rows)}")

    batch_num = 1
    successful_operations = 0
    failed_batches = []

    print(f"[START] Beginning UPSERT batch operation (batch size: {batch_size})...\n")
    for i in range(0, len(filtered_rows), batch_size):
        batch = filtered_rows[i:i + batch_size]

        try:
            # IMPORTANT: supply on_conflict so Postgres knows which column to use for ON CONFLICT
            res = supabase.table(table_name).upsert(
                batch,
                on_conflict="Bank_account_number"   # <-- change if DB column is different (e.g. bank_account_number)
            ).execute()

            # Check result for errors in different client versions
            err = None
            data = None
            if hasattr(res, "error"):
                err = res.error
                data = res.data if hasattr(res, "data") else None
            elif isinstance(res, dict):
                err = res.get("error")
                data = res.get("data")
            else:
                # fallback
                err = None
                data = res

            if err:
                raise Exception(err)

            successful_operations += len(batch)
            print(f"[OK] Batch {batch_num}: Processed {len(batch)} rows | Total success: {successful_operations}/{len(filtered_rows)}")
            # update existing_accounts set so next batches skip duplicates if you filtered new ones only
            for r in batch:
                existing_accounts.add(r['Bank_account_number'])

            batch_num += 1

        except Exception as e:
            failed_batches.append((batch_num, str(e)))
            print(f"[ERROR] Batch {batch_num}: {str(e)}")
            batch_num += 1

    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total rows extracted (after dedupe): {total_rows}")
    print(f"Attempted upsert rows: {len(filtered_rows)}")
    print(f"Successfully upserted (counted locally): {successful_operations}")
    print(f"Failed batches: {len(failed_batches)}")
    print(f"Mode: UPSERT via on_conflict (New inserted, Existing updated if you choose to upsert them)")

    if failed_batches:
        print(f"\nFailed batch details:")
        for batch_no, error in failed_batches:
            print(f"  Batch {batch_no}: {error}")
        return False
    else:
        print("\n[SUCCESS] All attempted data processed successfully!")
        return True

if __name__ == "__main__":
    sql_file = "v3_scraper_merchantlaundering_data_table_202512241240.sql"
    if os.path.exists(sql_file):
        insert_or_update_in_batches(sql_file, TABLE_NAME, batch_size=5000, prefetch_existing=True)
    else:
        print(f"[ERROR] SQL file not found: {sql_file}")
