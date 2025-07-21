# logics/bank_tally_match_logic_pbl.py

import re

# --- Config for PBL bank extraction ---
PBL_BANK_CONFIG = {
    "narration_column": "B_Particulars",      # Not used for code, but included for completeness
    "cheque_column": "B_Ref_Cheque",          # Main reference/cheque code column in PBL
    "withdrawal_column": "B_Withdrawal",
    "deposit_column": "B_Deposit",
}

# --- Config for Tally extraction ---
PBL_TALLY_CONFIG = {
    "narration_column": "T_Particulars",
    "debit_column": "T_Debit",
    "credit_column": "T_Credit",
}

def extract_pbl_cheque_ref(text):
    """
    Extracts long reference codes from text.
    Example matches: LD2503320442, FT25032KBLVY, PDLD2404274823, etc.
    Pattern: 2+ uppercase letters, 6+ digits, then optional trailing letters/digits.
    """
    text = str(text)
    match = re.search(r'\b([A-Z]{2,}[0-9]{6,}[A-Z0-9]*)\b', text)
    if match:
        return match.group(1)
    return None

def match_cheques_pbl(bank_df, tally_df, start_id=1, run_tag=""):
    """
    Matches bank and tally cheques for PBL using reference code and exact amount.
    - Extracts cheque refs from bank 'B_Ref_Cheque' and tally 'T_Particulars'
    - For withdrawals: matches abs(B_Withdrawal) == T_Credit
    - For deposits: matches B_Deposit == T_Debit
    - Both code and amount must match for a valid pair.
    Returns only matched pairs.
    """
    bank_df = bank_df[bank_df['bf_is_matched'] == 0].copy()
    tally_df = tally_df[tally_df['bft_is_matched'] == 0].copy()

    bank_df['cheque_ref'] = bank_df[PBL_BANK_CONFIG['cheque_column']].apply(extract_pbl_cheque_ref)
    tally_df['cheque_ref'] = tally_df[PBL_TALLY_CONFIG['narration_column']].apply(extract_pbl_cheque_ref)

    matched = []
    used_bank = set()
    used_tally = set()
    match_id = start_id

    # Build map for quick lookup by cheque ref
    tally_cheque_map = {}
    for idx, row in tally_df.iterrows():
        ref = row['cheque_ref']
        if ref:
            tally_cheque_map.setdefault(ref, []).append(idx)

    for i, b_row in bank_df.iterrows():
        ref = b_row['cheque_ref']
        if not ref or i in used_bank:
            continue
        withdrawal = float(b_row.get(PBL_BANK_CONFIG['withdrawal_column'], 0) or 0)
        deposit = float(b_row.get(PBL_BANK_CONFIG['deposit_column'], 0) or 0)
        for j in tally_cheque_map.get(ref, []):
            if j in used_tally:
                continue
            t_row = tally_df.loc[j]
            tally_credit = float(t_row.get(PBL_TALLY_CONFIG['credit_column'], 0) or 0)
            tally_debit = float(t_row.get(PBL_TALLY_CONFIG['debit_column'], 0) or 0)
            # Exact amount match, following direction
            if (withdrawal and abs(withdrawal) == tally_credit) or (deposit and deposit == tally_debit):
                match_id_str = f"PTM_{run_tag}_{match_id:04d}" if run_tag else f"PTM_{match_id:04d}"
                matched.append({**b_row.to_dict(), 'bt_match_id': match_id_str, 'bt_source': 'Bank'})
                matched.append({**t_row.to_dict(), 'bt_match_id': match_id_str, 'bt_source': 'Tally'})
                used_bank.add(i)
                used_tally.add(j)
                match_id += 1
                break

    # Return only matched pairs (pt_matched table)
    return matched
