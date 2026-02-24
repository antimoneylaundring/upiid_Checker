import os
import sys
import time
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ================= LOAD ENV =================
load_dotenv()

DB_URL = os.getenv("DB_URL")
if not DB_URL:
    print("Error: DB_URL not found in .env file.")
    sys.exit(1)

# ================= CONFIG =================
EXCEL_FILE = "data.xlsx"
TABLE_NAME = "all_upiiD"
CHUNK_SIZE = 10_000

# ================= MAIN =================
def main():
    start = time.time()

    print(f"Reading Excel file: {EXCEL_FILE} (this may take time for large files...)")

    df = pd.read_excel(
        EXCEL_FILE,
        usecols=["Inserted_date", "Upi_vpa"],
        engine="openpyxl",
        parse_dates=["Inserted_date"],
    )

    # ---------- CLEANING ----------
    df["Inserted_date"] = pd.to_datetime(
        df["Inserted_date"], errors="coerce"
    ).dt.date

    df["Upi_vpa"] = df["Upi_vpa"].astype(str).str.strip()
    df.replace("nan", None, inplace=True)
    df.dropna(subset=["Upi_vpa"], inplace=True)

    # Deduplicate before DB insert
    df.drop_duplicates(subset=["Upi_vpa"], inplace=True)

    total_rows = len(df)
    print(f" Total rows to import (after dedup): {total_rows:,}")

    # ---------- DB ENGINE ----------
    engine = create_engine(
        DB_URL,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 30},
    )

    print(f" Importing in chunks of {CHUNK_SIZE:,} rows (skipping duplicates)...")

    inserted_total = 0
    skipped_total = 0

    # ================= INSERT LOOP =================
    for i in range(0, total_rows, CHUNK_SIZE):
        chunk = df.iloc[i:i + CHUNK_SIZE]

        dates = chunk["Inserted_date"].tolist()
        upis  = chunk["Upi_vpa"].tolist()

        with engine.begin() as conn:
            result = conn.execute(
                text(f"""
                    INSERT INTO "{TABLE_NAME}" ("Inserted_date", "Upi_vpa")
                    SELECT d, u
                    FROM unnest(
                        CAST(:dates AS date[]),
                        CAST(:upis  AS text[])
                    ) AS t(d, u)
                    ON CONFLICT DO NOTHING
                """),
                {
                    "dates": dates,
                    "upis": upis
                }
            )

            inserted = result.rowcount
            skipped  = len(chunk) - inserted
            inserted_total += inserted
            skipped_total  += skipped

        # ---------- PROGRESS ----------
        processed = min(i + CHUNK_SIZE, total_rows)
        pct = (processed / total_rows) * 100
        elapsed = time.time() - start

        print(
            f"   Processed {processed:,}  {total_rows:,} "
            f"({pct:.1f}%) inserted={inserted_total:,} "
            f" skipped={skipped_total:,} {elapsed:.1f}s"
        )

    elapsed = time.time() - start
    print(
        f"\n DONE in {elapsed:.1f}s "
        f" Inserted: {inserted_total:,} "
        f" Skipped (duplicates): {skipped_total:,}"
    )


# ================= ENTRY =================
if __name__ == "__main__":
    main()