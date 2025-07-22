CREATE DATABASE IF NOT EXISTS bank_reconciliation;
USE bank_reconciliation;

-- 1. BANK DATA
CREATE TABLE IF NOT EXISTS bank_data (
    bank_id INT AUTO_INCREMENT PRIMARY KEY,                -- Internal row ID
    bank_uid VARCHAR(50) NOT NULL UNIQUE,                  -- Unique identifier from source file

    bank_code VARCHAR(4),                                  -- Short code for the bank
    acct_no VARCHAR(20),                                   -- Bank account number

    statement_month VARCHAR(10),                           -- Month of the statement
    statement_year VARCHAR(10),                            -- Year of the statement

    B_Date DATE,                                           -- Transaction date
    B_Particulars VARCHAR(255),                            -- Transaction particulars/narration
    B_Ref_Cheque VARCHAR(50),                              -- Cheque/reference number
    B_Withdrawal DECIMAL(18,2),                            -- Withdrawal amount
    B_Deposit DECIMAL(18,2),                               -- Deposit amount
    B_Balance DECIMAL(18,2),                               -- Balance

    bank_ven VARCHAR(100),                                 -- Vendor/party name from bank

    input_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,        -- When the row was inserted

    -- Match flags: Status in reconciliation pipeline
    bf_is_matched TINYINT DEFAULT 0,                       -- Matched in Bank-Finance stage
    bf_date_matched DATETIME DEFAULT NULL,                 -- Date/time of Bank-Finance match

    bft_is_matched TINYINT DEFAULT 0,                      -- Matched in Bank-Finance-Tally stage
    bft_date_matched DATETIME DEFAULT NULL,                -- Date/time of Bank-Finance-Tally match

    bt_is_matched TINYINT DEFAULT 0,                       -- Matched in Bank-Tally stage
    bt_date_matched DATETIME DEFAULT NULL                  -- Date/time of Bank-Tally match
);

-- 2. FINANCE DATA
CREATE TABLE IF NOT EXISTS fin_data (
    fin_id INT AUTO_INCREMENT PRIMARY KEY,                  -- Internal row ID
    fin_uid VARCHAR(50) NOT NULL UNIQUE,                    -- Unique identifier from source file

    bank_code VARCHAR(4),                                   -- Short code for the bank
    acct_no VARCHAR(20),                                    -- Bank account number

    statement_month VARCHAR(10),                            -- Month of the statement
    statement_year VARCHAR(10),                             -- Year of the statement

    F_Routing_No VARCHAR(50),                               -- Routing number (for inter-bank)
    F_Receiving_AC_No VARCHAR(50),                          -- Receiving account number
    F_Credit_Amount DECIMAL(18,2),                          -- Credit amount (should match bank deposit)
    F_Receiver_Name VARCHAR(255),                           -- Name of payment receiver
    F_Bank_Name VARCHAR(255),                               -- Receiver's bank name
    F_Branch_Name VARCHAR(255),                             -- Receiver's bank branch name
    F_Sender_Name VARCHAR(255),                             -- Name of payment sender
    F_Sender_Account VARCHAR(50),                           -- Sender's account number
    F_Sender_Bank VARCHAR(255),                             -- Sender's bank name
    F_Unit_Name VARCHAR(255),                               -- Company unit name
    F_Team_Name VARCHAR(255),                               -- Team name
    F_New_Project VARCHAR(255),                             -- New project name/code
    F_Project VARCHAR(255),                                 -- Project name/code
    F_Sub_Project VARCHAR(255),                             -- Sub-project name/code
    F_PO VARCHAR(255),                                      -- Purchase Order
    F_Status VARCHAR(255),                                  -- Status field
    F_Voucher_Date DATE,                                    -- Voucher date
    F_Voucher_No VARCHAR(255),                              -- Voucher number
    F_Payment_Date DATE,                                    -- Actual payment date
    F_Payment_Month VARCHAR(255),                           -- Month of payment
    F_Remarks TEXT,                                         -- Extra remarks
    F_Mark VARCHAR(255),                                    -- Mark/label
    F_Concern VARCHAR(255),                                 -- Concern

    fin_ven VARCHAR(100),                                   -- Vendor/party name in finance

    input_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,          -- When the row was inserted

    -- Match flags: Status in reconciliation pipeline
    bf_is_matched TINYINT DEFAULT 0,                        -- Matched in Bank-Finance stage
    bf_date_matched DATETIME DEFAULT NULL,                  -- Date/time of Bank-Finance match

    bft_is_matched TINYINT DEFAULT 0,                       -- Matched in Bank-Finance-Tally stage
    bft_date_matched DATETIME DEFAULT NULL                  -- Date/time of Bank-Finance-Tally match
);

-- 3. TALLY DATA
CREATE TABLE IF NOT EXISTS tally_data (
    tally_id INT AUTO_INCREMENT PRIMARY KEY,                -- Internal row ID
    tally_uid VARCHAR(50) NOT NULL UNIQUE,                  -- Unique identifier from source file

    bank_code VARCHAR(4),                                   -- Bank code
    acct_no VARCHAR(20),                                    -- Account number (links to bank_data)

    statement_month VARCHAR(10),                            -- Statement month
    statement_year VARCHAR(10),                             -- Statement year

    unit_name VARCHAR(255),                                 -- Unit name

    T_Date DATE,                                            -- Tally voucher date
    dr_cr VARCHAR(255),                                     -- Debit/Credit indicator
    T_Particulars TEXT,                                     -- Narration/particulars
    T_Vch_Type VARCHAR(255),                                -- Voucher type
    T_Vch_No VARCHAR(255),                                  -- Voucher number
    T_Debit DECIMAL(18,2),                                  -- Debit amount
    T_Credit DECIMAL(18,2),                                 -- Credit amount

    tally_ven TEXT,                                         -- Tally ledger vendor/party

    input_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,          -- When the row was inserted

    -- Match flags: Status in reconciliation pipeline
    bft_is_matched TINYINT DEFAULT 0,                       -- Matched in Bank-Finance-Tally stage?
    bft_date_matched DATETIME DEFAULT NULL,                 -- Date/time of Bank-Finance-Tally match

    bt_is_matched TINYINT DEFAULT 0,                        -- Matched in Bank-Tally stage
    bt_date_matched DATETIME DEFAULT NULL                   -- Date/time of Bank-Tally match
);

-- 4. BANK-FIN MATCHED DATA
CREATE TABLE IF NOT EXISTS bf_matched (
    bf_id INT AUTO_INCREMENT PRIMARY KEY,                   -- Internal row ID for this table

    -- Match group metadata
    bf_match_id VARCHAR(50) NOT NULL,                       -- Unique match group ID for this match
    bf_source VARCHAR(10) NOT NULL,                         -- Source of the row: 'Bank' or 'Finance'
    bf_match_type VARCHAR(32),                              -- Match type (e.g. 1-to-1, 1-to-N)

    -- Bank columns (mirrored from bank_data)
    bank_id INT,
    bank_uid VARCHAR(50),
    
    bank_code VARCHAR(4),
    acct_no VARCHAR(20),
    
    statement_month VARCHAR(10),
    statement_year VARCHAR(10),
    
    B_Date DATE,
    B_Particulars VARCHAR(255),
    B_Ref_Cheque VARCHAR(50),
    B_Withdrawal DECIMAL(18,2),
    B_Deposit DECIMAL(18,2),
    B_Balance DECIMAL(18,2),
    bank_ven VARCHAR(100),

    -- Finance columns (mirrored from fin_data)
    fin_id INT,
    fin_uid VARCHAR(50),
    F_Routing_No VARCHAR(50),
    F_Receiving_AC_No VARCHAR(50),
    F_Credit_Amount DECIMAL(18,2),
    F_Receiver_Name VARCHAR(255),
    F_Bank_Name VARCHAR(255),
    F_Branch_Name VARCHAR(255),
    F_Sender_Name VARCHAR(255),
    F_Sender_Account VARCHAR(50),
    F_Sender_Bank VARCHAR(255),
    F_Unit_Name VARCHAR(255),
    F_Team_Name VARCHAR(255),
    F_New_Project VARCHAR(255),
    F_Project VARCHAR(255),
    F_Sub_Project VARCHAR(255),
    F_PO VARCHAR(255),
    F_Status VARCHAR(255),
    F_Voucher_Date DATE,
    F_Voucher_No VARCHAR(255),
    F_Payment_Date DATE,
    F_Payment_Month VARCHAR(255),
    F_Remarks TEXT,
    F_Mark VARCHAR(255),
    F_Concern VARCHAR(255),
    fin_ven VARCHAR(100),

    input_date DATETIME DEFAULT NULL,                        -- When the row was inserted (carried from source)

    -- Bank-Fin-Tally match flags (for further BFT reconciliation)
    bf_is_matched TINYINT DEFAULT 0,                        -- Is this row matched in Bank-Fin stage
    bf_date_matched DATETIME DEFAULT NULL,                  -- Date/time of Bank-Finance match

    bft_is_matched TINYINT DEFAULT 0,                       -- Matched in Bank-Finance-Tally stage
    bft_date_matched DATETIME DEFAULT NULL,                 -- Date/time of BFT match

    bt_is_matched TINYINT DEFAULT 0,                        -- Matched in Bank-Tally stage
    bt_date_matched DATETIME DEFAULT NULL                   -- Date/time of Bank-Tally match
);

-- 5. BANK-FIN-TALLY MATCHED DATA
CREATE TABLE IF NOT EXISTS bft_matched (
    bft_id INT AUTO_INCREMENT PRIMARY KEY,                  -- Internal row ID

    -- BFT match group metadata
    bft_match_id VARCHAR(50) NOT NULL,                      -- Unique BFT match group ID
    bft_source VARCHAR(16) NOT NULL,                        -- Source: 'Bank', 'Finance', 'Tally'
    bft_match_type VARCHAR(32),                             -- Match type (e.g. 1-to-N-to-N)

    -- Link to Bank-Fin match group
    bf_id INT,                                              -- bf_matched internal ID
    bf_match_id VARCHAR(50),                                -- bf_matched group ID
    bf_source VARCHAR(10),                                  -- bf_matched source
    bf_match_type VARCHAR(32),                              -- bf_matched match type

    -- Bank columns (mirrored from bank_data)
    bank_id INT,
    bank_uid VARCHAR(50),
    
    bank_code VARCHAR(4),
    acct_no VARCHAR(20),
    
    statement_month VARCHAR(10),
    statement_year VARCHAR(10),
    
    B_Date DATE,
    B_Particulars VARCHAR(255),
    B_Ref_Cheque VARCHAR(50),
    B_Withdrawal DECIMAL(18,2),
    B_Deposit DECIMAL(18,2),
    B_Balance DECIMAL(18,2),
    bank_ven VARCHAR(100),

    -- Finance columns (mirrored from fin_data)
    fin_id INT,
    fin_uid VARCHAR(50),
    F_Routing_No VARCHAR(50),
    F_Receiving_AC_No VARCHAR(50),
    F_Credit_Amount DECIMAL(18,2),
    F_Receiver_Name VARCHAR(255),
    F_Bank_Name VARCHAR(255),
    F_Branch_Name VARCHAR(255),
    F_Sender_Name VARCHAR(255),
    F_Sender_Account VARCHAR(50),
    F_Sender_Bank VARCHAR(255),
    F_Unit_Name VARCHAR(255),
    F_Team_Name VARCHAR(255),
    F_New_Project VARCHAR(255),
    F_Project VARCHAR(255),
    F_Sub_Project VARCHAR(255),
    F_PO VARCHAR(255),
    F_Status VARCHAR(255),
    F_Voucher_Date DATE,
    F_Voucher_No VARCHAR(255),
    F_Payment_Date DATE,
    F_Payment_Month VARCHAR(255),
    F_Remarks TEXT,
    F_Mark VARCHAR(255),
    F_Concern VARCHAR(255),
    fin_ven VARCHAR(100),

    -- Tally columns (mirrored from tally_data)
    tally_id INT,
    tally_uid VARCHAR(50),
    unit_name VARCHAR(255),
    T_Date DATE,
    dr_cr VARCHAR(255),
    T_Particulars TEXT,
    T_Vch_Type VARCHAR(255),
    T_Vch_No VARCHAR(255),
    T_Debit DECIMAL(18,2),
    T_Credit DECIMAL(18,2),
    tally_ven TEXT,

    input_date DATETIME DEFAULT NULL,                        -- When the row was inserted (carried from source)

    bf_is_matched TINYINT DEFAULT 0,                        -- Matched in Bank-Finance stage
    bf_date_matched DATETIME DEFAULT NULL,                  -- Date/time of BF match

    bft_is_matched TINYINT DEFAULT 0,                       -- Matched in Bank-Finance-Tally stage
    bft_date_matched DATETIME DEFAULT NULL,                 -- Date/time of BFT match

    bt_is_matched TINYINT DEFAULT 0,                        -- Matched in Bank-Tally stage
    bt_date_matched DATETIME DEFAULT NULL                   -- Date/time of Bank-Tally match
);

-- 6. BANK-TALLY MATCHED DATA
CREATE TABLE IF NOT EXISTS bt_matched (
    bt_id INT AUTO_INCREMENT PRIMARY KEY,                   -- Internal row ID

    bt_match_id VARCHAR(50),                                -- Unique match group ID for Bank-Tally match
    bt_source VARCHAR(50),                                  -- Source: 'Bank' or 'Tally'
    cheque_ref VARCHAR(50),                                 -- Cheque/reference number used for match

    -- Bank columns (mirrored from bank_data)
    bank_id INT,
    bank_uid VARCHAR(50),
    
    bank_code VARCHAR(4),
    acct_no VARCHAR(20),
    
    statement_month VARCHAR(10),
    statement_year VARCHAR(10),
    
    B_Date DATE,
    B_Particulars VARCHAR(255),
    B_Ref_Cheque VARCHAR(50),
    B_Withdrawal DECIMAL(18,2),
    B_Deposit DECIMAL(18,2),
    B_Balance DECIMAL(18,2),
    bank_ven VARCHAR(100),

    -- Tally columns (mirrored from tally_data)
    tally_id INT,
    tally_uid VARCHAR(50),
    T_Date DATE,
    dr_cr VARCHAR(255),
    T_Particulars TEXT,
    T_Vch_Type VARCHAR(255),
    T_Vch_No VARCHAR(255),
    T_Debit DECIMAL(18,2),
    T_Credit DECIMAL(18,2),
    tally_ven TEXT,
    unit_name VARCHAR(255),

    input_date DATETIME DEFAULT NULL,                        -- When the row was inserted (carried from source)

    bf_is_matched TINYINT DEFAULT 0,                        -- Matched in Bank-Finance stage
    bf_date_matched DATETIME DEFAULT NULL,                  -- Date/time of BF match

    bft_is_matched TINYINT DEFAULT 0,                       -- Matched in Bank-Finance-Tally stage
    bft_date_matched DATETIME DEFAULT NULL,                 -- Date/time of BFT match

    bt_is_matched TINYINT DEFAULT 0,                        -- Matched in Bank-Tally stage
    bt_date_matched DATETIME DEFAULT NULL                   -- Date/time of Bank-Tally match
);
