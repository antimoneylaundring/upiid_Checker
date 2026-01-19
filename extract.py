import pandas as pd
import psycopg2
from psycopg2 import extras, OperationalError, InterfaceError
import os
from dotenv import load_dotenv
import time

# Load env
load_dotenv()
DB_URL = os.getenv("DB_URL")

if DB_URL and 'postgresql+psycopg2://' in DB_URL:
    DB_URL = DB_URL.replace('postgresql+psycopg2://', 'postgresql://')

CSV_FILE = "upi_data.csv"
TABLE_NAME = "all_upiiD"

def get_db_connection():
    """Create database connection with keepalive settings"""
    return psycopg2.connect(
        DB_URL,
        connect_timeout=10,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5
    )

def apply_filters(df):
    """Apply business logic filters to dataframe"""
    print("\nApplying filters...")
    print(f"  Initial rows: {len(df)}")
    
    # Apply filter
    mask = (
        (df["Feature_type"].astype(str).str.strip() == "BS Money Laundering") &
        (df["Input_user"].astype(str).str.strip().str.lower() != "automated") &
        (df["Upi_bank_account_wallet"].astype(str).str.strip().isin(["UPI"])) &
        (df["Search_for"].astype(str).str.strip().isin(["App", "Web"]))
    )
    
    df_filtered = df[mask].copy()
    
    return df_filtered

def bulk_insert_with_retry(csv_file, max_retries=3):
    """Insert filtered data in one transaction with retry logic"""
    for attempt in range(max_retries):
        conn = None
        try:
            # Read CSV (all 30 columns)
            print(f"Reading CSV: {csv_file}")
            df = pd.read_csv(csv_file, dtype=str)
            print(f"Read {len(df)} rows with {len(df.columns)} columns from CSV")
            
            # Apply filters (uses all columns for filtering)
            df_filtered = apply_filters(df)
            
            if len(df_filtered) == 0:
                print("\nNo rows to insert after filtering!")
                return 0, 0
            
            # Remove duplicates based on Bank_account_number
            initial_count = len(df_filtered)
            df_filtered = df_filtered.drop_duplicates(subset=['Upi_vpa'])
            if len(df_filtered) < initial_count:
                print(f"  Removed {initial_count - len(df_filtered)} duplicate bank accounts")
            
            # Prepare data for bulk insert - ONLY Inserted_date and Bank_account_number
            values = [
                (row["Inserted_date"], row["Upi_vpa"]) 
                for _, row in df_filtered.iterrows()
            ]
            
            # Connect and insert
            print(f"\nConnecting to database...")
            conn = get_db_connection()
            cur = conn.cursor()
            
            # Insert only the 2 columns we need
            insert_query = f"""
                INSERT INTO "{TABLE_NAME}" ("Inserted_date", "Upi_vpa")
                VALUES %s
                ON CONFLICT DO NOTHING
            """
            
            print(f"Inserting {len(values)} rows (Inserted_date, Upi_vpa only)...")
            start_time = time.time()
            
            extras.execute_values(
                cur, 
                insert_query, 
                values,
                page_size=1000
            )
            
            conn.commit()
            elapsed = time.time() - start_time
            
            cur.close()
            conn.close()
            
            print(f"Successfully inserted {len(values)} rows in {elapsed:.2f}s!")
            return len(values), 0
            
        except (OperationalError, InterfaceError) as e:
            print(f"Connection error: {e}")
            if conn:
                try:
                    conn.close()
                except:
                    pass
            
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"Retrying in {wait_time}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                print(f"Failed after {max_retries} attempts")
                raise
        
        except Exception as e:
            print(f"Error: {e}")
            if conn:
                try:
                    if not conn.closed:
                        conn.rollback()
                    conn.close()
                except:
                    pass
            raise
    
    return 0, 0

# Execute bulk insert
if __name__ == "__main__":
    print("="*60)
    print("BULK INSERT SCRIPT - FILTERED DATA")
    print("="*60)
    
    try:
        success, failed = bulk_insert_with_retry(CSV_FILE)
        print(f"\n{'='*60}")
        print(f"SUMMARY")
        print(f"{'='*60}")
        print(f"Total Inserted: {success}")
        print(f"Total Failed: {failed}")
        print(f"{'='*60}")
    except Exception as e:
        print(f"\n{'='*60}")
        print(f"FATAL ERROR: {e}")
        print(f"{'='*60}")
        import traceback
        traceback.print_exc()