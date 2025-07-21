# routes/reports_routes.py

from flask import Blueprint, request, jsonify, send_file
from utils.db import engine
from sqlalchemy import text
import pandas as pd
import io
import datetime

reports_bp = Blueprint('reports', __name__)

@reports_bp.route('/reports/unmatched_bank', methods=['POST'])
def get_unmatched_bank_report():
    """
    Get unmatched bank records filtered by bank_code, acct_no, statement_month, and statement_year.
    Expects JSON: { "bank_code": "...", "acct_no": "...", "statement_month": "...", "statement_year": "..." }
    """
    data = request.get_json()
    bank_code = data.get('bank_code')
    acct_no = data.get('acct_no')
    statement_month = data.get('statement_month')
    statement_year = data.get('statement_year')

    # Validate all filters are provided
    if not all([bank_code, acct_no, statement_month, statement_year]):
        return jsonify({'success': False, 'msg': 'Missing one or more required filters'}), 400

    try:
        with engine.connect() as conn:
            query = text(
                "SELECT * FROM bank_data "
                "WHERE bt_is_matched = 0 AND bft_is_matched = 0 "
                "AND bank_code = :bank_code AND acct_no = :acct_no AND statement_month = :statement_month AND statement_year = :statement_year"
            )
            result = conn.execute(query, {
                "bank_code": bank_code,
                "acct_no": acct_no,
                "statement_month": statement_month,
                "statement_year": statement_year
            })
            rows = [dict(row) for row in result.mappings()]
        # Format date fields as YYYY-MM-DD using strftime
        date_fields = ['B_Date', 'bf_date_matched', 'bft_date_matched', 'bt_date_matched', 'input_date']
        for row in rows:
            for field in date_fields:
                if field in row and row[field]:
                    val = row[field]
                    if isinstance(val, (datetime.date, datetime.datetime)):
                        row[field] = val.strftime('%Y-%m-%d')
                    else:
                        row[field] = str(val)[:10]
        # Add serial number
        for idx, row in enumerate(rows):
            row['S/N'] = idx + 1
        # Only keep specified columns (custom subset for Unmatched Bank)
        column_order = [
            'S/N', 'bank_uid', 'bank_code', 'acct_no', 'statement_month', 'statement_year',
            'B_Date', 'B_Particulars', 'B_Ref_Cheque', 'B_Withdrawal', 'B_Deposit', 'bank_ven'
        ]
        filtered = [
            {col: row.get(col, '') for col in column_order}
            for row in rows
        ]
        return jsonify({'success': True, 'data': filtered})
    except Exception as e:
        return jsonify({'success': False, 'msg': str(e)}), 500

@reports_bp.route('/reports/unmatched_bank_excel', methods=['POST'])
def download_unmatched_bank_excel():
    """
    Download unmatched bank records as Excel, filtered by bank_code, acct_no, statement_month, and statement_year.
    Expects JSON: { "bank_code": "...", "acct_no": "...", "statement_month": "...", "statement_year": "..." }
    """
    data = request.get_json()
    bank_code = data.get('bank_code')
    acct_no = data.get('acct_no')
    statement_month = data.get('statement_month')
    statement_year = data.get('statement_year')

    # Only these columns, in this order (custom subset for Unmatched Bank)
    column_order = [
        'S/N', 'bank_uid', 'bank_code', 'acct_no', 'statement_month', 'statement_year',
        'B_Date', 'B_Particulars', 'B_Ref_Cheque', 'B_Withdrawal', 'B_Deposit', 'bank_ven'
    ]

    try:
        with engine.connect() as conn:
            query = text(
                "SELECT * FROM bank_data "
                "WHERE bt_is_matched = 0 AND bft_is_matched = 0 "
                "AND bank_code = :bank_code AND acct_no = :acct_no AND statement_month = :statement_month AND statement_year = :statement_year"
            )
            result = conn.execute(query, {
                "bank_code": bank_code,
                "acct_no": acct_no,
                "statement_month": statement_month,
                "statement_year": statement_year
            })
            rows = [dict(row) for row in result.mappings()]
        if not rows:
            return jsonify({'success': False, 'msg': 'No data to export'}), 404
        # Add serial number column as first column
        df = pd.DataFrame(rows)
        df.insert(0, 'S/N', list(range(1, len(df) + 1)))
        # Format date columns as YYYY-MM-DD
        date_cols = ['B_Date', 'bf_date_matched', 'bft_date_matched', 'bt_date_matched', 'input_date']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
        # Keep only the specified columns, in order
        df = df[[col for col in column_order if col in df.columns]]
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        output.seek(0)
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='unmatched_bank_report.xlsx'
        )
    except Exception as e:
        return jsonify({'success': False, 'msg': str(e)}), 500

@reports_bp.route('/get_bank_codes', methods=['GET'])
def get_bank_codes():
    try:
        with engine.connect() as conn:
            query = text("SELECT DISTINCT bank_code FROM bank_data WHERE bank_code IS NOT NULL AND bank_code != ''")
            result = conn.execute(query)
            codes = [row[0] for row in result]
        return jsonify({'success': True, 'bank_codes': codes})
    except Exception as e:
        return jsonify({'success': False, 'msg': str(e)}), 500

@reports_bp.route('/get_acct_nos', methods=['GET'])
def get_acct_nos():
    bank_code = request.args.get('bank_code')
    if not bank_code:
        return jsonify({'success': False, 'msg': 'Missing bank_code'}), 400
    try:
        with engine.connect() as conn:
            query = text("SELECT DISTINCT acct_no FROM bank_data WHERE bank_code = :bank_code AND acct_no IS NOT NULL AND acct_no != ''")
            result = conn.execute(query, {"bank_code": bank_code})
            accts = [row[0] for row in result]
        return jsonify({'success': True, 'acct_nos': accts})
    except Exception as e:
        return jsonify({'success': False, 'msg': str(e)}), 500

@reports_bp.route('/get_statement_years', methods=['GET'])
def get_statement_years():
    try:
        with engine.connect() as conn:
            query = text("SELECT DISTINCT statement_year FROM bank_data WHERE statement_year IS NOT NULL AND statement_year != ''")
            result = conn.execute(query)
            years = [row[0] for row in result]
        return jsonify({'success': True, 'years': years})
    except Exception as e:
        return jsonify({'success': False, 'msg': str(e)}), 500

@reports_bp.route('/get_statement_months', methods=['GET'])
def get_statement_months():
    try:
        with engine.connect() as conn:
            query = text("SELECT DISTINCT statement_month FROM bank_data WHERE statement_month IS NOT NULL AND statement_month != ''")
            result = conn.execute(query)
            months = [row[0] for row in result]
        return jsonify({'success': True, 'months': months})
    except Exception as e:
        return jsonify({'success': False, 'msg': str(e)}), 500

# --- Unmatched Tally Report Endpoints ---
@reports_bp.route('/reports/unmatched_tally', methods=['POST'])
def get_unmatched_tally_report():
    data = request.get_json()
    bank_code = data.get('bank_code')
    acct_no = data.get('acct_no')
    statement_month = data.get('statement_month')
    statement_year = data.get('statement_year')
    column_order = [
        'S/N', 'tally_uid', 'bank_code', 'acct_no', 'statement_month', 'statement_year', 'unit_name',
        'T_Date', 'dr_cr', 'T_Particulars', 'T_Vch_Type', 'T_Vch_No', 'T_Debit', 'T_Credit', 'tally_ven'
    ]
    if not all([bank_code, acct_no, statement_month, statement_year]):
        return jsonify({'success': False, 'msg': 'Missing one or more required filters'}), 400
    try:
        with engine.connect() as conn:
            query = text(
                "SELECT * FROM tally_data "
                "WHERE bt_is_matched = 0 AND bft_is_matched = 0 "
                "AND bank_code = :bank_code AND acct_no = :acct_no AND statement_month = :statement_month AND statement_year = :statement_year"
            )
            result = conn.execute(query, {
                "bank_code": bank_code,
                "acct_no": acct_no,
                "statement_month": statement_month,
                "statement_year": statement_year
            })
            rows = [dict(row) for row in result.mappings()]
        # Format date fields as YYYY-MM-DD using strftime
        date_fields = ['T_Date', 'bf_date_matched', 'bft_date_matched', 'bt_date_matched', 'input_date']
        for row in rows:
            for field in date_fields:
                if field in row and row[field]:
                    val = row[field]
                    if isinstance(val, (datetime.date, datetime.datetime)):
                        row[field] = val.strftime('%Y-%m-%d')
                    else:
                        row[field] = str(val)[:10]
        # Add serial number
        for idx, row in enumerate(rows):
            row['S/N'] = idx + 1
        # Only keep specified columns (now matching schema order)
        filtered = [
            {col: row.get(col, '') for col in column_order}
            for row in rows
        ]
        return jsonify({'success': True, 'data': filtered})
    except Exception as e:
        return jsonify({'success': False, 'msg': str(e)}), 500

@reports_bp.route('/reports/unmatched_tally_excel', methods=['POST'])
def download_unmatched_tally_excel():
    data = request.get_json()
    bank_code = data.get('bank_code')
    acct_no = data.get('acct_no')
    statement_month = data.get('statement_month')
    statement_year = data.get('statement_year')
    column_order = [
        'S/N', 'tally_uid', 'bank_code', 'acct_no', 'statement_month', 'statement_year', 'unit_name',
        'T_Date', 'dr_cr', 'T_Particulars', 'T_Vch_Type', 'T_Vch_No', 'T_Debit', 'T_Credit', 'tally_ven'
    ]
    if not all([bank_code, acct_no, statement_month, statement_year]):
        return jsonify({'success': False, 'msg': 'Missing one or more required filters'}), 400
    try:
        with engine.connect() as conn:
            query = text(
                "SELECT * FROM tally_data "
                "WHERE bt_is_matched = 0 AND bft_is_matched = 0 "
                "AND bank_code = :bank_code AND acct_no = :acct_no AND statement_month = :statement_month AND statement_year = :statement_year"
            )
            result = conn.execute(query, {
                "bank_code": bank_code,
                "acct_no": acct_no,
                "statement_month": statement_month,
                "statement_year": statement_year
            })
            rows = [dict(row) for row in result.mappings()]
        if not rows:
            return jsonify({'success': False, 'msg': 'No data to export'}), 404
        import pandas as pd, io
        df = pd.DataFrame(rows)
        df.insert(0, 'S/N', list(range(1, len(df) + 1)))
        # Format date columns as YYYY-MM-DD
        date_cols = ['T_Date', 'bf_date_matched', 'bft_date_matched', 'bt_date_matched', 'input_date']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
        df = df[[col for col in column_order if col in df.columns]]
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        output.seek(0)
        from flask import send_file
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='unmatched_tally_report.xlsx'
        )
    except Exception as e:
        return jsonify({'success': False, 'msg': str(e)}), 500

# Dropdown endpoints for tally_data
@reports_bp.route('/get_tally_bank_codes', methods=['GET'])
def get_tally_bank_codes():
    try:
        with engine.connect() as conn:
            query = text("SELECT DISTINCT bank_code FROM tally_data WHERE bank_code IS NOT NULL AND bank_code != ''")
            result = conn.execute(query)
            codes = [row[0] for row in result]
        return jsonify({'success': True, 'bank_codes': codes})
    except Exception as e:
        return jsonify({'success': False, 'msg': str(e)}), 500

@reports_bp.route('/get_tally_acct_nos', methods=['GET'])
def get_tally_acct_nos():
    bank_code = request.args.get('bank_code')
    if not bank_code:
        return jsonify({'success': False, 'msg': 'Missing bank_code'}), 400
    try:
        with engine.connect() as conn:
            query = text("SELECT DISTINCT acct_no FROM tally_data WHERE bank_code = :bank_code AND acct_no IS NOT NULL AND acct_no != ''")
            result = conn.execute(query, {"bank_code": bank_code})
            accts = [row[0] for row in result]
        return jsonify({'success': True, 'acct_nos': accts})
    except Exception as e:
        return jsonify({'success': False, 'msg': str(e)}), 500

@reports_bp.route('/get_tally_statement_years', methods=['GET'])
def get_tally_statement_years():
    try:
        with engine.connect() as conn:
            query = text("SELECT DISTINCT statement_year FROM tally_data WHERE statement_year IS NOT NULL AND statement_year != ''")
            result = conn.execute(query)
            years = [row[0] for row in result]
        return jsonify({'success': True, 'years': years})
    except Exception as e:
        return jsonify({'success': False, 'msg': str(e)}), 500

@reports_bp.route('/get_tally_statement_months', methods=['GET'])
def get_tally_statement_months():
    try:
        with engine.connect() as conn:
            query = text("SELECT DISTINCT statement_month FROM tally_data WHERE statement_month IS NOT NULL AND statement_month != ''")
            result = conn.execute(query)
            months = [row[0] for row in result]
        return jsonify({'success': True, 'months': months})
    except Exception as e:
        return jsonify({'success': False, 'msg': str(e)}), 500

@reports_bp.route('/reports/bank_fin_matched', methods=['POST'])
def get_bank_fin_matched_report():
    """
    Get matched Bank-Fin records filtered by bank_code, acct_no, statement_month, and statement_year.
    Expects JSON: { "bank_code": "...", "acct_no": "...", "statement_month": "...", "statement_year": "..." }
    """
    data = request.get_json()
    bank_code = data.get('bank_code')
    acct_no = data.get('acct_no')
    statement_month = data.get('statement_month')
    statement_year = data.get('statement_year')

    if not all([bank_code, acct_no, statement_month, statement_year]):
        return jsonify({'success': False, 'msg': 'Missing one or more required filters'}), 400

    try:
        with engine.connect() as conn:
            query = text(
                "SELECT * FROM bf_matched "
                "WHERE bank_code = :bank_code AND acct_no = :acct_no "
                "AND statement_month = :statement_month AND statement_year = :statement_year"
            )
            result = conn.execute(query, {
                "bank_code": bank_code,
                "acct_no": acct_no,
                "statement_month": statement_month,
                "statement_year": statement_year
            })
            rows = [dict(row) for row in result.mappings()]
        # Format date fields as YYYY-MM-DD using strftime
        date_fields = ['B_Date', 'F_Date', 'bf_date_matched', 'input_date']
        for row in rows:
            for field in date_fields:
                if field in row and row[field]:
                    val = row[field]
                    if isinstance(val, (datetime.date, datetime.datetime)):
                        row[field] = val.strftime('%Y-%m-%d')
                    else:
                        row[field] = str(val)[:10]
        # Add serial number
        for idx, row in enumerate(rows):
            row['S/N'] = idx + 1
        # Only keep specified columns (custom subset for Bank-Fin Match)
        column_order = [
            'S/N', 'bf_match_id', 'bf_source', 'bf_match_type', 'bank_uid', 'bank_code', 'acct_no',
            'B_Date', 'B_Particulars', 'B_Ref_Cheque', 'B_Withdrawal', 'B_Deposit', 'bank_ven',
            'statement_month', 'statement_year', 'fin_uid', 'F_Credit_Amount', 'F_Receiver_Name', 'F_Voucher_No', 'fin_ven'
        ]
        filtered = [
            {col: row.get(col, '') for col in column_order}
            for row in rows
        ]
        return jsonify({'success': True, 'data': filtered})
    except Exception as e:
        return jsonify({'success': False, 'msg': str(e)}), 500

@reports_bp.route('/reports/bank_fin_matched_excel', methods=['POST'])
def download_bank_fin_matched_excel():
    data = request.get_json()
    bank_code = data.get('bank_code')
    acct_no = data.get('acct_no')
    statement_month = data.get('statement_month')
    statement_year = data.get('statement_year')
    column_order = [
        'S/N', 'bf_match_id', 'bf_source', 'bf_match_type', 'bank_uid', 'bank_code', 'acct_no',
        'B_Date', 'B_Particulars', 'B_Ref_Cheque', 'B_Withdrawal', 'B_Deposit', 'bank_ven',
        'statement_month', 'statement_year', 'fin_uid', 'F_Credit_Amount', 'F_Receiver_Name', 'F_Voucher_No', 'fin_ven'
    ]
    if not all([bank_code, acct_no, statement_month, statement_year]):
        return jsonify({'success': False, 'msg': 'Missing one or more required filters'}), 400
    try:
        with engine.connect() as conn:
            query = text(
                "SELECT * FROM bf_matched "
                "WHERE bank_code = :bank_code AND acct_no = :acct_no "
                "AND statement_month = :statement_month AND statement_year = :statement_year"
            )
            result = conn.execute(query, {
                "bank_code": bank_code,
                "acct_no": acct_no,
                "statement_month": statement_month,
                "statement_year": statement_year
            })
            rows = [dict(row) for row in result.mappings()]
        if not rows:
            return jsonify({'success': False, 'msg': 'No data to export'}), 404
        import pandas as pd, io
        df = pd.DataFrame(rows)
        df.insert(0, 'S/N', list(range(1, len(df) + 1)))
        date_cols = ['B_Date', 'F_Date', 'bf_date_matched', 'input_date']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
        df = df[[col for col in column_order if col in df.columns]]
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        output.seek(0)
        from flask import send_file
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='bank_fin_matched_report.xlsx'
        )
    except Exception as e:
        return jsonify({'success': False, 'msg': str(e)}), 500

@reports_bp.route('/reports/bank_fin_tally_matched', methods=['POST'])
def get_bank_fin_tally_matched_report():
    data = request.get_json()
    bank_code = data.get('bank_code')
    acct_no = data.get('acct_no')
    statement_month = data.get('statement_month')
    statement_year = data.get('statement_year')
    column_order = [
        'S/N', 'bft_match_id', 'bft_source', 'bft_match_type', 'bank_uid', 'bank_code', 'acct_no',
        'B_Date', 'B_Particulars', 'B_Ref_Cheque', 'B_Withdrawal', 'B_Deposit', 'bank_ven',
        'fin_uid', 'F_Credit_Amount', 'F_Receiver_Name', 'F_Voucher_No', 'fin_ven',
        'tally_uid', 'T_Date', 'dr_cr', 'T_Particulars', 'T_Vch_No', 'T_Debit', 'T_Credit',
        'tally_ven', 'statement_month', 'statement_year'
    ]
    if not all([bank_code, acct_no, statement_month, statement_year]):
        return jsonify({'success': False, 'msg': 'Missing one or more required filters'}), 400
    try:
        with engine.connect() as conn:
            query = text(
                "SELECT * FROM bft_matched "
                "WHERE bank_code = :bank_code AND acct_no = :acct_no "
                "AND statement_month = :statement_month AND statement_year = :statement_year"
            )
            result = conn.execute(query, {
                "bank_code": bank_code,
                "acct_no": acct_no,
                "statement_month": statement_month,
                "statement_year": statement_year
            })
            rows = [dict(row) for row in result.mappings()]
        date_fields = ['B_Date', 'F_Date', 'T_Date', 'bft_date_matched', 'input_date']
        for row in rows:
            for field in date_fields:
                if field in row and row[field]:
                    val = row[field]
                    if isinstance(val, (datetime.date, datetime.datetime)):
                        row[field] = val.strftime('%Y-%m-%d')
                    else:
                        row[field] = str(val)[:10]
        for idx, row in enumerate(rows):
            row['S/N'] = idx + 1
        filtered = [
            {col: row.get(col, '') for col in column_order}
            for row in rows
        ]
        return jsonify({'success': True, 'data': filtered})
    except Exception as e:
        return jsonify({'success': False, 'msg': str(e)}), 500

@reports_bp.route('/reports/bank_fin_tally_matched_excel', methods=['POST'])
def download_bank_fin_tally_matched_excel():
    data = request.get_json()
    bank_code = data.get('bank_code')
    acct_no = data.get('acct_no')
    statement_month = data.get('statement_month')
    statement_year = data.get('statement_year')
    column_order = [
        'S/N', 'bft_match_id', 'bft_source', 'bft_match_type', 'bank_uid', 'bank_code', 'acct_no',
        'B_Date', 'B_Particulars', 'B_Ref_Cheque', 'B_Withdrawal', 'B_Deposit', 'bank_ven',
        'fin_uid', 'F_Credit_Amount', 'F_Receiver_Name', 'F_Voucher_No', 'fin_ven',
        'tally_uid', 'T_Date', 'dr_cr', 'T_Particulars', 'T_Vch_No', 'T_Debit', 'T_Credit',
        'tally_ven', 'statement_month', 'statement_year'
    ]
    if not all([bank_code, acct_no, statement_month, statement_year]):
        return jsonify({'success': False, 'msg': 'Missing one or more required filters'}), 400
    try:
        with engine.connect() as conn:
            query = text(
                "SELECT * FROM bft_matched "
                "WHERE bank_code = :bank_code AND acct_no = :acct_no "
                "AND statement_month = :statement_month AND statement_year = :statement_year"
            )
            result = conn.execute(query, {
                "bank_code": bank_code,
                "acct_no": acct_no,
                "statement_month": statement_month,
                "statement_year": statement_year
            })
            rows = [dict(row) for row in result.mappings()]
        if not rows:
            return jsonify({'success': False, 'msg': 'No data to export'}), 404
        import pandas as pd, io
        df = pd.DataFrame(rows)
        df.insert(0, 'S/N', list(range(1, len(df) + 1)))
        date_cols = ['B_Date', 'F_Date', 'T_Date', 'bft_date_matched', 'input_date']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
        df = df[[col for col in column_order if col in df.columns]]
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        output.seek(0)
        from flask import send_file
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='bank_fin_tally_matched_report.xlsx'
        )
    except Exception as e:
        return jsonify({'success': False, 'msg': str(e)}), 500

@reports_bp.route('/reports/bank_tally_matched', methods=['POST'])
def get_bank_tally_matched_report():
    data = request.get_json()
    bank_code = data.get('bank_code')
    acct_no = data.get('acct_no')
    statement_month = data.get('statement_month')
    statement_year = data.get('statement_year')
    column_order = [
        'S/N', 'bt_match_id', 'bt_source', 'bank_uid', 'acct_no', 'bank_code', 'B_Date',
        'B_Particulars', 'B_Ref_Cheque', 'B_Withdrawal', 'B_Deposit', 'bank_ven',
        'tally_uid', 'T_Date', 'dr_cr', 'T_Particulars', 'T_Vch_No', 'T_Debit', 'T_Credit',
        'tally_ven', 'statement_month', 'statement_year'
    ]
    if not all([bank_code, acct_no, statement_month, statement_year]):
        return jsonify({'success': False, 'msg': 'Missing one or more required filters'}), 400
    try:
        with engine.connect() as conn:
            query = text(
                "SELECT * FROM bt_matched "
                "WHERE bank_code = :bank_code AND acct_no = :acct_no "
                "AND statement_month = :statement_month AND statement_year = :statement_year"
            )
            result = conn.execute(query, {
                "bank_code": bank_code,
                "acct_no": acct_no,
                "statement_month": statement_month,
                "statement_year": statement_year
            })
            rows = [dict(row) for row in result.mappings()]
        date_fields = ['B_Date', 'T_Date', 'bt_date_matched', 'input_date']
        for row in rows:
            for field in date_fields:
                if field in row and row[field]:
                    val = row[field]
                    if isinstance(val, (datetime.date, datetime.datetime)):
                        row[field] = val.strftime('%Y-%m-%d')
                    else:
                        row[field] = str(val)[:10]
        for idx, row in enumerate(rows):
            row['S/N'] = idx + 1
        filtered = [
            {col: row.get(col, '') for col in column_order}
            for row in rows
        ]
        return jsonify({'success': True, 'data': filtered})
    except Exception as e:
        return jsonify({'success': False, 'msg': str(e)}), 500

@reports_bp.route('/reports/bank_tally_matched_excel', methods=['POST'])
def download_bank_tally_matched_excel():
    data = request.get_json()
    bank_code = data.get('bank_code')
    acct_no = data.get('acct_no')
    statement_month = data.get('statement_month')
    statement_year = data.get('statement_year')
    column_order = [
        'S/N', 'bt_match_id', 'bt_source', 'bank_uid', 'acct_no', 'bank_code', 'B_Date',
        'B_Particulars', 'B_Ref_Cheque', 'B_Withdrawal', 'B_Deposit', 'bank_ven',
        'tally_uid', 'T_Date', 'dr_cr', 'T_Particulars', 'T_Vch_No', 'T_Debit', 'T_Credit',
        'tally_ven', 'statement_month', 'statement_year'
    ]
    if not all([bank_code, acct_no, statement_month, statement_year]):
        return jsonify({'success': False, 'msg': 'Missing one or more required filters'}), 400
    try:
        with engine.connect() as conn:
            query = text(
                "SELECT * FROM bt_matched "
                "WHERE bank_code = :bank_code AND acct_no = :acct_no "
                "AND statement_month = :statement_month AND statement_year = :statement_year"
            )
            result = conn.execute(query, {
                "bank_code": bank_code,
                "acct_no": acct_no,
                "statement_month": statement_month,
                "statement_year": statement_year
            })
            rows = [dict(row) for row in result.mappings()]
        if not rows:
            return jsonify({'success': False, 'msg': 'No data to export'}), 404
        import pandas as pd, io
        df = pd.DataFrame(rows)
        df.insert(0, 'S/N', list(range(1, len(df) + 1)))
        date_cols = ['B_Date', 'T_Date', 'bt_date_matched', 'input_date']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
        df = df[[col for col in column_order if col in df.columns]]
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        output.seek(0)
        from flask import send_file
        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='bank_tally_matched_report.xlsx'
        )
    except Exception as e:
        return jsonify({'success': False, 'msg': str(e)}), 500