# bank_tally_reconcile_routes.py

"""Bank Tally Reconciliation Routes Module.

This module defines Flask routes for reconciling bank and tally records.
The main endpoint performs reconciliation based on the bank code and account number,
calls the relevant matching logic, updates the matched records, and stores the results.
"""

from flask import Blueprint, request, jsonify
import pandas as pd
from sqlalchemy import text
from datetime import datetime

from utils.db import engine, ensure_table_exists
from utils.help_texts import HelpTexts

from logics.bank_tally_match_logic_mdb import match_cheques as match_cheques_mdb
from logics.bank_tally_match_logic_mtb import match_cheques as match_cheques_mtb
from logics.bank_tally_match_logic_pbl import match_cheques_pbl

# Create a Flask Blueprint for bank tally reconciliation routes
bank_tally_bp = Blueprint('bank_tally_bp', __name__, url_prefix='/bank_tally')


@bank_tally_bp.route('/reconcile', methods=['POST'])
def reconcile_bank_tally():
    """Reconcile unmatched bank and tally records for the provided bank and account.

    Retrieves unmatched records, applies bank-specific matching logic,
    updates the relevant tables, and returns the matching results.
    """
    bank_code = request.form.get('bank_code')
    account_number = request.form.get('account_number')
    if not bank_code or not account_number:
        return jsonify({'success': False, 'msg': 'bank_code and account_number are required.'})

    try:
        # Retrieve unmatched bank records for the given account number
        bank_df = pd.read_sql(
            text("SELECT * FROM bank_data WHERE bf_is_matched = 0 AND acct_no=:acct_no"),
            engine, params={"acct_no": account_number}
        )

        # Retrieve unmatched tally records for the given bank code and account number
        tally_df = pd.read_sql(
            text("SELECT * FROM tally_data WHERE bft_is_matched = 0 AND bank_code=:bank_code AND acct_no=:acct_no"),
            engine, params={"bank_code": bank_code, "acct_no": account_number}
        )

    except Exception as e:
        return jsonify({'success': False, 'msg': f'Error loading data: {e}'})

    if bank_df.empty or tally_df.empty:
        return jsonify({'success': False, 'msg': 'No unmatched data for this bank/account.'})

    # Generate a unique run tag for this reconciliation process
    run_tag = f"{bank_code}_{account_number}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    # Execute matching logic based on the bank code
    if bank_code == "MDB":
        bt_matched = match_cheques_mdb(
            bank_df, tally_df, start_id=1, run_tag=run_tag)
    elif bank_code == "MTB":
        bt_matched = match_cheques_mtb(
            bank_df, tally_df, start_id=1, run_tag=run_tag)
    elif bank_code == "PBL":
        bt_matched = match_cheques_pbl(
            bank_df, tally_df, start_id=1, run_tag=run_tag)
    else:
        return jsonify({'success': False, 'msg': f'Bank code {bank_code} not supported for cheque reconciliation.'})

    # Convert matched results to DataFrame
    bt_matched_df = pd.DataFrame(bt_matched)

    # Ensure the results table exists in the database
    ensure_table_exists(engine, 'bt_matched')
    now_dt = datetime.now()
    bt_matched_df['input_date'] = now_dt
    bt_matched_df['bt_is_matched'] = 1
    bt_matched_df['bt_date_matched'] = now_dt

    # Rename columns to match MySQL table schema
    bt_matched_df = bt_matched_df.rename(columns={
        "Vch_Type": "Vch Type",
        "Vch_No_": "Vch No."
    })

    # Insert matched records and update source tables
    if not bt_matched_df.empty:
        # Identify matched bank and tally unique identifiers
        matched_bank_uids = bt_matched_df.loc[bt_matched_df['bt_source']
                                              == 'Bank', 'bank_uid'].dropna().unique().tolist()
        matched_tally_uids = bt_matched_df.loc[bt_matched_df['bt_source']
                                               == 'Tally', 'tally_uid'].dropna().unique().tolist()
        now_str = now_dt.strftime('%Y-%m-%d %H:%M:%S')

        with engine.begin() as conn:
            bt_matched_df.to_sql('bt_matched', conn,
                                 if_exists='append', index=False)
            # Update matched records in bank_data
            if matched_bank_uids:
                bank_uids = ",".join(f"'{x}'" for x in matched_bank_uids)
                conn.execute(
                    text(
                        f"UPDATE bank_data SET bt_is_matched=1, bt_date_matched=:dt WHERE bank_uid IN ({bank_uids})"),
                    {"dt": now_str}
                )
            # Update matched records in tally_data
            if matched_tally_uids:
                tally_uids = ",".join(f"'{x}'" for x in matched_tally_uids)
                conn.execute(
                    text(
                        f"UPDATE tally_data SET bt_is_matched=1, bt_date_matched=:dt WHERE tally_uid IN ({tally_uids})"),
                    {"dt": now_str}
                )

    return jsonify({
        'success': True,
        'matched_count': len(bt_matched),
        'msg': f'Matched records inserted: {len(bt_matched)}'
    })
