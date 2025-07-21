
# pbl_parser.py

import pandas as pd
import re

PBL_ACCOUNT_NUMBERS = {
    "2126117010855",
    # Add other valid PBL account numbers here
}

def to_hex_date(date_str):
    try:
        dt = pd.to_datetime(date_str, errors="coerce")
        if pd.isna(dt):
            return "0"
        ymd = dt.strftime("%Y%m%d")
        return format(int(ymd), "x")
    except Exception:
        return "0"

def to_hex_balance(balance):
    try:
        bal = float(balance)
        bal_int = int(round(bal))
        return format(bal_int, "x")
    except Exception:
        return "0"

def parse_pbl_statement(input_file):
    # Read metadata (first 6 rows)
    metadata_raw = pd.read_excel(input_file, header=None, nrows=6)
    
    # Extract account number (row 2, col 3)
    raw_acc = metadata_raw.iloc[2, 3]
    m_acc = re.search(r'([0-9\-]+)', str(raw_acc))
    account_number = m_acc.group(1) if m_acc else "UnknownAcc"
    account_number = account_number.replace("-", "")
    
    # Validate account number
    if account_number not in PBL_ACCOUNT_NUMBERS:
        raise ValueError(f"Account {account_number} is not authorized for PBL statements.")
    
    # Extract statement period (row 4, col 0)
    raw_period = metadata_raw.iloc[4, 0]
    m_period = re.search(r'FROM\s+(.+?)\s+TO\s+(.+)', str(raw_period), re.IGNORECASE)
    if m_period:
        start_date_str = m_period.group(1)
        end_date_str = m_period.group(2)
    else:
        start_date_str = ""
        end_date_str = ""
    
    try:
        start_date = pd.to_datetime(start_date_str, format="%d %b %Y")
        statement_month = start_date.strftime("%B")
        statement_year = str(start_date.year)
    except Exception:
        statement_month = ""
        statement_year = ""
    
    # Read transaction data (header row at index 5)
    df_data = pd.read_excel(input_file, header=5, dtype=str)
    
    # Select exact columns by name
    cols_required = ["Tran Date", "Transaction Ref.", "Description", "Debit", "Credit", "Balance"]
    df_data = df_data[cols_required]
    
    # Drop rows where Tran Date starts with 'BALANCE AT PERIOD START :'
    df_data = df_data[~df_data["Tran Date"].str.startswith("BALANCE AT PERIOD START :", na=False)].reset_index(drop=True)
    
    # Remove repeated header rows
    def is_duplicate_header(row):
        return all(str(row[col]).strip().lower() == col.lower() for col in cols_required)
    df_data = df_data[~df_data.apply(is_duplicate_header, axis=1)].reset_index(drop=True)
    
    # Drop rows where all key columns are empty or whitespace or NaN
    def is_row_empty(row):
        return all((pd.isna(row[col]) or str(row[col]).strip() == "") for col in cols_required)
    df_data = df_data[~df_data.apply(is_row_empty, axis=1)].reset_index(drop=True)
    
    # Drop rows where Description is exactly "Balance B/F"
    df_data = df_data[df_data["Description"].str.strip().str.lower() != "balance b/f"].reset_index(drop=True)
    
    # Convert date column
    df_data["Tran Date"] = pd.to_datetime(df_data["Tran Date"], errors="coerce").dt.strftime("%Y-%m-%d")
    
    # Clean numeric columns
    for col in ["Debit", "Credit", "Balance"]:
        df_data[col] = pd.to_numeric(df_data[col].str.replace(",", ""), errors='coerce')
    
    # Rename columns to DB schema
    df_data = df_data.rename(columns={
        "Tran Date": "B_Date",
        "Transaction Ref.": "B_Ref_Cheque",
        "Description": "B_Particulars",
        "Debit": "B_Withdrawal",
        "Credit": "B_Deposit",
        "Balance": "B_Balance"
    })
    
    # Generate bank_uid for each row
    uid_list = []
    for idx, row in df_data.iterrows():
        row_num = str(idx + 1).zfill(6)
        hex_date = to_hex_date(row.get("B_Date", ""))
        hex_balance = to_hex_balance(row.get("B_Balance", 0))
        uid = f"PBL_{hex_date}_{hex_balance}_{row_num}"
        uid_list.append(uid)
    df_data.insert(0, "bank_uid", uid_list)
    
    # Add metadata columns
    df_data["acct_no"] = account_number
    df_data["statement_month"] = statement_month
    df_data["statement_year"] = statement_year
    df_data["bank_code"] = "PBL"
    df_data["bank_ven"] = ""  # blank for now
    
    return df_data
