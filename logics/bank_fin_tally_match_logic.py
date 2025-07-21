# logics/bank_fin_tally_match_logic.py

import pandas as pd
import re

def _extract_numeric(val):
    if pd.isnull(val):
        return ''
    return ''.join(re.findall(r'\d+', str(val)))

def _get_bank_amount(bank_row):
    if 'B_Withdrawal' not in bank_row:
        raise ValueError("Missing 'B_Withdrawal' column in bank_row.")
    return float(bank_row['B_Withdrawal'])

def bank_fin_tally_match(bf_df, tally_df, bank_code, run_tag=""):

    tally_df['tally_uid'] = tally_df['tally_uid'].astype(str)
    tally_df = tally_df.set_index('tally_uid', drop=False)
    used_tally_uids = set()

    grouped = bf_df.groupby('bf_match_id')
    bft_matched_rows = []
    bft_id_counter = 1

    for _, group in grouped:
        bank_rows = group[group['bf_source'].str.lower() == 'bank']
        finance_rows = group[group['bf_source'].str.lower() == 'finance']

        if bank_rows.shape[0] != 1 or finance_rows.shape[0] < 1:
            continue

        bank_row = bank_rows.iloc[0]
        
        
        finance_vouchers = finance_rows['F_Voucher_No'].apply(_extract_numeric)
        finance_amounts = finance_rows['F_Credit_Amount'].astype(float)
        bank_amount = _get_bank_amount(bank_row)

        matched_tally_uids = []
        tally_candidates = tally_df[~tally_df.index.isin(used_tally_uids)].copy()
        tally_candidates['vch_suffix'] = tally_candidates['T_Vch_No'].apply(_extract_numeric)
        tally_candidates['T_Credit'] = tally_candidates['T_Credit'].astype(float)

        for vch, amt in zip(finance_vouchers, finance_amounts):
            tally_match = tally_candidates[
                (tally_candidates['vch_suffix'] == vch) &
                (tally_candidates['T_Credit'] == amt)
            ]
            if not tally_match.empty:
                uid = str(tally_match['tally_uid'].iloc[0])
                matched_tally_uids.append(uid)
            else:
                break

        n_fin = len(finance_rows)
        n_tally = len(matched_tally_uids)
        group_sum_fin = finance_amounts.sum()
        group_sum_tally = tally_df.loc[matched_tally_uids]['T_Credit'].sum() if matched_tally_uids else 0

        if n_fin == n_tally and abs(group_sum_fin - bank_amount) < 1e-4 and abs(group_sum_tally - bank_amount) < 1e-4:
            used_tally_uids.update(matched_tally_uids)
            bft_match_id = f"BFTM_{run_tag}_{bft_id_counter:04d}" if run_tag else f"BFTM_{bft_id_counter:04d}"

            bft_match_type = f"1 to {n_fin} to {n_tally}"

            bank_out = bank_row.copy()
            bank_out['bft_match_id'] = bft_match_id
            bank_out['bft_match_type'] = bft_match_type
            bank_out['bft_source'] = 'Bank'
            bft_matched_rows.append(bank_out)

            for _, fin_row in finance_rows.iterrows():

                fin_out = fin_row.copy()
                fin_out['bft_match_id'] = bft_match_id
                fin_out['bft_match_type'] = bft_match_type
                fin_out['bft_source'] = 'Finance'
                bft_matched_rows.append(fin_out)

            for uid in matched_tally_uids:
                tally_out = tally_df.loc[uid].copy()

                tally_out['bft_match_id'] = bft_match_id
                tally_out['bft_match_type'] = bft_match_type
                tally_out['bft_source'] = 'Tally'
                bft_matched_rows.append(tally_out)

            bft_id_counter += 1

    bft_matched_df = pd.DataFrame(bft_matched_rows)
    return bft_matched_df
