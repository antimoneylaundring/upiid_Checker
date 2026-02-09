import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox
import os, sys


# ---------------- HELPERS ----------------
def find_column(cols, keys):
    for c in cols:
        cc = c.lower().replace(" ", "").replace("_", "")
        for k in keys:
            if k in cc:
                return c
    return None


def app_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


# ---------------- MAIN ----------------
def process_file():
    try:
        path = filedialog.askopenfilename(
            title="Select Excel / CSV",
            filetypes=[("Excel / CSV", "*.xlsx *.xls *.csv")]
        )
        if not path:
            return

        df = pd.read_csv(path) if path.endswith(".csv") else pd.read_excel(path)
        df.columns = df.columns.str.strip()

        # -------- REQUIRED COLUMNS --------
        input_col = "Input_user"
        search_col = "Search_for"
        wallet_col = "Upi_bank_account_wallet"
        status_col = "Approvd_status"

        qc_user_col = find_column(df.columns, ["approvedby", "qcby", "qcuser"])
        video_col = find_column(df.columns, ["videourl", "video"])
        url_col = video_col

        if not qc_user_col or not video_col:
            messagebox.showerror("Error", "QC User / Video URL column missing")
            return

        # -------- USER LIST --------
        users = [
            "Emp Manoj Kumar", "Emp Muskan Verma", "Emp Shashank Sharma",
            "Emp Sheetal Dubey", "Emp Shubhankar Shukla", "Emp Sunena Yadav",
            "Emp Vidhi Satsangi", "INT Bhavna Mathur",
            "INT Chandrakanta Vishwakarma", "INT Gunjan Baghel",
            "INT Laxmi Kumari", "INT Neha Baghel",
            "INT Riya Kaushik", "INT Shikha Gautam"
        ]

        multi_users = {
            "Emp Sunena Yadav",
            "Emp Shubhankar Shukla",
            "Emp Sheetal Dubey"
        }

        summary = pd.DataFrame({"Name": users})

        # ================= INSERTION =================
        approved = df[
            (df[input_col].isin(users)) &
            (df[status_col] == 1)
        ].copy()

        approved["_used"] = False

        # ---- NOT FOUND (HIGHEST PRIORITY) ----
        not_found = approved[
            (approved[search_col] == "Web") &
            (approved[input_col].str.contains("nfuser", case=False, na=False))
        ]
        approved.loc[not_found.index, "_used"] = True

        # ---- MULTIPLE CASES ----
        dup_urls = approved[url_col].astype(str).str.strip()
        dup_urls = dup_urls[dup_urls != ""]
        dup_urls = dup_urls[dup_urls.duplicated(keep=False)]

        multiple = approved[
            (~approved["_used"]) &
            (approved[input_col].isin(multi_users)) &
            (approved[search_col] == "Web") &
            (approved[wallet_col] == "UPI") &
            (approved[url_col].astype(str).str.strip().isin(dup_urls))
        ]
        approved.loc[multiple.index, "_used"] = True

        # ---- DAILY CASES ----
        daily = approved[
            (~approved["_used"]) &
            (approved[search_col] == "Web") &
            (approved[wallet_col].isin(["UPI", "Bank Account"]))
        ]
        approved.loc[daily.index, "_used"] = True

        # ---- APP ----
        app = approved[
            (~approved["_used"]) &
            (approved[search_col] == "App")
        ]
        approved.loc[app.index, "_used"] = True

        # ---- CRYPTO ----
        crypto = approved[
            (~approved["_used"]) &
            (approved[search_col] == "Web") &
            (approved[wallet_col] == "Crypto")
        ]

        # ---- WA / TG ----
        watg = approved[
            approved[search_col] == "Messaging Channel Platforms"
        ]

        def m(df_):
            return df_.groupby(input_col).size()

        summary["Daily Cases"] = summary["Name"].map(m(daily)).fillna("NA")
        summary["Multiple Cases"] = summary["Name"].map(m(multiple)).fillna("NA")
        summary["Not Found"] = summary["Name"].map(m(not_found)).fillna("NA")
        summary["App"] = summary["Name"].map(m(app)).fillna("NA")
        summary["WA/TG Case"] = summary["Name"].map(m(watg)).fillna("NA")
        summary["Crypto Cases"] = summary["Name"].map(m(crypto)).fillna("NA")

        num_cols = [
            "Daily Cases", "Multiple Cases", "Not Found",
            "App", "WA/TG Case", "Crypto Cases"
        ]
        summary["Total Case"] = summary[num_cols].replace("NA", 0).astype(int).sum(axis=1)

        # ================= ERROR =================
        error_df = df[
            (df[status_col] == 2) &
            (df[input_col] != df[qc_user_col])
        ]
        summary["Error"] = summary["Name"].map(
            error_df.groupby(input_col).size()
        ).fillna("NA")

        # ================= QC SUMMARY =================
        total_qc = df.groupby(qc_user_col).size()
        video_qc = df[
            df[video_col].notna() &
            (df[video_col].astype(str).str.strip() != "")
        ].groupby(qc_user_col).size()

        qc_df = pd.DataFrame({
            "Non Video QC": total_qc - video_qc,
            "Video QC": video_qc,
            "Total QC": total_qc
        }).fillna(0).astype(int)

        qc_df["Home QC"] = "NA"
        qc_df.reset_index(inplace=True)
        qc_df.rename(columns={qc_user_col: "Name"}, inplace=True)

        final_summary = summary.merge(qc_df, on="Name", how="left").fillna("NA")

        # ================= TOTAL ROW =================
        total_row = {}
        for col in final_summary.columns:
            if col == "Name":
                total_row[col] = "Total"
            else:
                col_data = final_summary[col].replace("NA", 0)
                total_row[col] = int(col_data.sum()) if pd.api.types.is_numeric_dtype(col_data) else "NA"

        final_summary = pd.concat(
            [final_summary, pd.DataFrame([total_row])],
            ignore_index=True
        )

        # ================= SAVE =================
        out = os.path.join(app_dir(), "FINAL_DAILY_QC_INSERTION_SUMMARY.xlsx")
        final_summary.to_excel(out, index=False)

        messagebox.showinfo("Success", f"Report Generated:\n{out}")

    except Exception as e:
        messagebox.showerror("Error", str(e))


# ================= GUI =================
root = tk.Tk()
root.title("Final QC & Insertion Summary")
root.geometry("650x340")
root.resizable(False, False)

tk.Label(
    root,
    text="Final Insertion & QC Summary",
    font=("Arial", 12, "bold")
).pack(pady=20)

tk.Button(
    root,
    text="Select File & Generate Report",
    width=50,
    height=2,
    command=process_file
).pack(pady=20)

tk.Label(root, text="Pixeltruth Automation", fg="gray").pack(side="bottom", pady=10)

root.mainloop()
