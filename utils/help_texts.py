"""
HelpTexts: Centralized help text provider for all bank and parser modules in the Bank Reconcile Helper application.
"""

class HelpTexts:
    MDB = '''OVERVIEW
The MDB Parser processes Midland Bank (MDB) statements and prepares them for the Bank-Fin reconciliation workflow in this web application. It automates data extraction, cleaning, and structuring to ensure statements are ready for matching with finance records in your database.

KEY FEATURES
- Parses Midland Bank (MDB) PDF statements converted to Excel (.xlsx)
- Automatically extracts and cleans transaction data for reconciliation
- Removes duplicate headers, totals, and non-transaction rows
- Extracts and summarizes key account metadata (e.g., account number, account name, statement period)
- Automatically generates a unique UID for each transaction, ensuring traceability
- Adds a standardized vendor column for downstream reconciliation logic
- Saves clean, structured output with separate sheets for transaction data and account details
- Provides clear status messages after parsing

HOW TO USE
- Go to the Bank Parser tab in the web application.
- Select 'Midland Bank' as the bank type.
- Upload your MDB statement in Excel format (.xlsx).
- Click 'Parse' to begin extracting and cleaning data.
- Wait for the parsing process to complete. You will see a message when it’s done.

BEST PRACTICES
- Use only Midland Bank statements converted to Excel (.xlsx) via Foxit Phantom PDF or similar tools.
- Ensure the file you upload is not open in another program.
'''

    MTB = '''OVERVIEW
The MTB Parser processes Mutual Trust Bank (MTB) statements for reconciliation in this web application. It extracts, cleans, and structures transaction data for matching with finance records in your database.

KEY FEATURES
- Parses MTB statements in Excel format (.xls)
- Cleans and standardizes transaction data
- Extracts account metadata and statement period
- Generates unique UIDs for traceability
- Adds a vendor column for reconciliation
- Provides clear status messages after parsing

HOW TO USE
- Go to the Bank Parser tab in the web application.
- Select 'Mutual Trust Bank' as the bank type.
- Upload your MTB statement in Excel format (.xls).
- Click 'Parse' to process the data.
- Wait for the parsing process to complete. You will see a message when it’s done.
'''

    PBL = '''OVERVIEW
The PBL Parser processes Prime Bank statements for reconciliation in this web application. It prepares transaction data for matching with finance records in your database.

KEY FEATURES
- Parses Prime Bank statements in Excel format (.xlsx)
- Cleans and standardizes transaction data
- Extracts account metadata and statement period
- Generates unique UIDs for traceability
- Adds a vendor column for reconciliation
- Provides clear status messages after parsing

HOW TO USE
- Go to the Bank Parser tab in the web application.
- Select 'Prime Bank' as the bank type.
- Upload your Prime Bank statement in Excel format (.xlsx).
- Click 'Parse' to process the data.
- Wait for the parsing process to complete. You will see a message when it’s done.
'''

    AIBL = '*Help text for AIBL will be added soon.*'
    OBL = '*Help text for OBL will be added soon.*'

    BANK = f"""
==== Midland Bank (MDB) Parser ====
{MDB}

==== Mutual Trust Bank (MTB) Parser ====
{MTB}

==== Prime Bank (PBL) Parser ====
{PBL}

==== AIBL Parser ====
{AIBL}

==== OBL Parser ====
{OBL}

SUPPORT
If you have questions or require technical support, contact your IT department.
"""

    TALLY = '''OVERVIEW
- The Tally Parser processes Tally Excel statements and prepares them for Bank-Fin reconciliation workflows.

KEY FEATURES
- Parses Tally Excel statements (.xlsx, .xls, .xlsm) exported from Tally or converted from PDF
- Automatically extracts and cleans transaction data, removes duplicates, and standardizes fields
- Identifies and summarizes key metadata, including account information and statement period
- Automatically generates a unique UID for each transaction for traceability and downstream matching
- Adds a normalized vendor column to support robust reconciliation logic
- Saves structured output with separate sheets for transaction data and account metadata
- Provides user feedback through progress bar and status messages
- Allows instant review of parsed results with a one-click “Open File” option

HOW TO USE
- Launch the Tally Parser application.
- Click “Select File” and choose a Tally statement in Excel format (.xlsx, .xls, or .xlsm).
- Choose the correct worksheet (if prompted).
- Click “Parse” to begin data extraction and cleaning.
- Monitor progress via the progress bar and status messages.
- When parsing is complete, review the output summary.
- Click “Open File” to open your cleaned Excel statement immediately.

BEST PRACTICES
- Use original or properly exported Tally Excel statements for best results.
- If working with PDF statements, convert to Excel using a reliable tool before parsing.
- Ensure the output file is closed in Excel before running the parser again.
- Retain parsed files as part of your audit or reconciliation records.

SUPPORT
- If you have questions or require technical support, contact your IT department '''

    FIN = '''OVERVIEW
The Finance Paid List Parser processes Excel files of paid lists for normalization and preparation for reconciliation.

KEY FEATURES
-  Parses finance paid list Excel files (.xlsx)
-  Automatically extracts and cleans transaction data
-  Generates a unique UID for each transaction
-  Adds a standardized vendor column for matching
-  Splits output by bank and sender account, with separate files per month
-  Provides user feedback through progress bar and status messages
-  Includes a summary popup after parsing- 

HOW TO USE
- Launch the Finance Paid List Parser application.
-  Click “Select File” and choose your paid list Excel file.
- Select the desired worksheet and payment month.
- Click “Parse” to process and export cleaned files.
- Review the summary when finished.

BEST PRACTICES
- Use original paid list Excel files.
- Ensure the output files are not open before re-running.
- Keep parsed files for audit/reference.

SUPPORT
- Contact your IT department for questions or assistance.
'''

    @classmethod
    def get(cls, key):
        if key.upper() == 'BANK':
            return cls.BANK
        return getattr(cls, key.upper(), 'No help text available for this parser.') 