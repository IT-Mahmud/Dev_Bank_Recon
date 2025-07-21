// static/parser_tabs.js - Modularized JS for parser_tabs.html

function showTab(tabId) {
    document.querySelectorAll('.tab-pane').forEach(function(pane) {
        pane.style.display = 'none';
    });
    document.querySelectorAll('.tab-btn').forEach(function(btn) {
        btn.removeAttribute('data-active');
    });
    document.getElementById('pane-' + tabId).style.display = 'block';
    document.getElementById('btn-' + tabId).setAttribute('data-active','1');
}

function fetchAndSetBanks(selectId) {
    const bankSelect = document.getElementById(selectId);
    bankSelect.innerHTML = '<option value="">Loading...</option>';
    fetch('/get_banks', {
        method: 'POST'
    })
    .then(resp => resp.json())
    .then(data => {
        bankSelect.innerHTML = '';
        if(data.success && data.banks.length > 0) {
            bankSelect.innerHTML = '<option value="">-- Select Bank --</option>';
            data.banks.forEach(function(code) {
                const opt = document.createElement('option');
                opt.value = code;
                opt.text = code;
                bankSelect.appendChild(opt);
            });
        } else {
            bankSelect.innerHTML = '<option value="">No banks found</option>';
        }
    })
    .catch(() => {
        bankSelect.innerHTML = '<option value="">Error</option>';
    });
}

document.querySelectorAll('.parser-form').forEach(function(form) {
    if(form.id === "reconcile-form" || form.id === "bft-reconcile-form" || form.id === "bank-tally-reconcile-form") return;

    const fileInput = form.querySelector('.file-input');
    const sheetRow = form.querySelector('[id$="-sheetRow"]');
    const sheetSelect = form.querySelector('.sheet-select');
    const parseBtn = form.querySelector('.parser-parse-btn');
    const msgDiv = form.nextElementSibling;
    const uploadedDiv = msgDiv.nextElementSibling;
    let sheetNames = [];
    const parserId = form.id.replace('form-','');

    let bankSelect = null;
    if (parserId === "bank") {
        bankSelect = form.querySelector('#bank-bankSelect');
    }

    function updateParseButtonState() {
        if (parserId === "bank") {
            parseBtn.disabled = !(fileInput && fileInput.files.length && bankSelect && bankSelect.value);
        } else {
            if (sheetSelect && sheetSelect.style.display !== 'none') {
                parseBtn.disabled = !(fileInput && fileInput.files.length && sheetSelect.value);
            } else {
                parseBtn.disabled = !(fileInput && fileInput.files.length);
            }
        }
    }

    if (parserId === "bank" && bankSelect) {
        bankSelect.addEventListener('change', updateParseButtonState);
    }

    if (fileInput) {
    fileInput.addEventListener('change', function(e) {
            if (sheetSelect) sheetSelect.innerHTML = "";
            if (msgDiv) msgDiv.textContent = "";
            if (uploadedDiv) uploadedDiv.textContent = "";
            if (parseBtn) parseBtn.disabled = true;
            if (sheetRow) sheetRow.style.display = "none";
        if (!fileInput.files.length) {
            updateParseButtonState();
            return;
        }

        const file = fileInput.files[0];
        if (parserId === "bank" && file.name.endsWith('.csv')) {
                if (sheetRow) sheetRow.style.display = "none";
            updateParseButtonState();
            return;
        }

        var reader = new FileReader();
        reader.onload = function(e) {
            var data = new Uint8Array(e.target.result);
            var workbook = XLSX.read(data, {type: 'array'});
            sheetNames = workbook.SheetNames;

            if (sheetNames.length === 1 && parserId === "bank") {
                    if (sheetRow) sheetRow.style.display = "none";
                    if (sheetSelect) {
                sheetSelect.innerHTML = "";
                var opt = document.createElement('option');
                opt.value = sheetNames[0];
                opt.text = sheetNames[0];
                sheetSelect.appendChild(opt);
                sheetSelect.value = sheetNames[0];
                    }
                updateParseButtonState();
            } else if (sheetNames.length) {
                    if (sheetSelect) {
                sheetSelect.innerHTML = "";
                sheetNames.forEach(function(name) {
                    var opt = document.createElement('option');
                    opt.value = name;
                    opt.text = name;
                    sheetSelect.appendChild(opt);
                });
                    }
                    if (sheetRow) sheetRow.style.display = "flex";
                updateParseButtonState();
            }
        };
        reader.readAsArrayBuffer(file);
    });
    }

    if (sheetSelect) {
        sheetSelect.addEventListener('change', function() {
        updateParseButtonState();
    });
    }

    form.addEventListener('submit', function(e) {
        e.preventDefault();
        if (parseBtn) parseBtn.disabled = true;
        if (msgDiv) msgDiv.textContent = "";
        if (uploadedDiv) uploadedDiv.textContent = "";

        var fileField = fileInput ? fileInput.name : null;
        var sheetName = (sheetSelect && sheetSelect.style.display !== "none") ? sheetSelect.value : "";
        var file = fileInput ? fileInput.files[0] : null;

        var bankName = "";
        if (parserId === "bank" && bankSelect) {
            bankName = bankSelect.value;
            if (!bankName) {
                msgDiv.innerText = "Please select a bank.";
                if (parseBtn) parseBtn.disabled = false;
                return;
            }
        }

        if (!file || (sheetSelect && sheetSelect.style.display !== "none" && !sheetName)) {
            msgDiv.innerText = "Please select file and sheet.";
            if (parseBtn) parseBtn.disabled = false;
            return;
        }
        var formData = new FormData();
        if (fileField && file) formData.append(fileField, file);
        if (sheetName) formData.append('sheet_name', sheetName);
        if (parserId === "bank" && bankName) formData.append('bank_name', bankName);

        var parserRoute = {
            finance: '/parse_finance',
            bank: '/parse_bank',
            tally: '/parse_tally'
        }[parserId];

        fetch(parserRoute, {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(resp => {
            if (msgDiv) msgDiv.textContent = resp.msg || "";
            if (resp.success && resp.uploaded_filename) {
                if (uploadedDiv) uploadedDiv.innerHTML = "<b>Uploaded Filename:</b> " + resp.uploaded_filename;
            } else {
                if (uploadedDiv) uploadedDiv.innerHTML = "";
            }
            if (parseBtn) parseBtn.disabled = false;
        })
        .catch(err => {
            if (msgDiv) msgDiv.textContent = "Error uploading or parsing file.";
            if (parseBtn) parseBtn.disabled = false;
        });
    });

    updateParseButtonState();
});

// --- Account dropdown logic for Bank-Fin Match ---
// const bankTableSelect = document.getElementById('bank-table-select');
// const accountNumberSelect = document.getElementById('account-number-select');

// function fetchAndSetAccounts() {
//     const bank_table = bankTableSelect.value;
//     accountNumberSelect.innerHTML = '<option value="">Loading...</option>';
//     fetch('/get_accounts', {
//         method: 'POST',
//         headers: {'Content-Type': 'application/x-www-form-urlencoded'},
//         body: `bank_table=${encodeURIComponent(bank_table)}`
//     })
//     .then(resp => resp.json())
//     .then(data => {
//         accountNumberSelect.innerHTML = '';
//         if(data.success && data.accounts.length > 0){
//             accountNumberSelect.innerHTML = '<option value="">-- Select Account --</option>';
//             data.accounts.forEach(function(acct){
//                 const opt = document.createElement('option');
//                 opt.value = acct;
//                 opt.text = acct;
//                 accountNumberSelect.appendChild(opt);
//             });
//         } else {
//             accountNumberSelect.innerHTML = '<option value="">No accounts found</option>';
//         }
//     })
//     .catch(() => {
//         accountNumberSelect.innerHTML = '<option value="">Error</option>';
//     });
// }

// if (bankTableSelect) {
//     fetchAndSetAccounts();
//     bankTableSelect.addEventListener('change', fetchAndSetAccounts);
// }



const bankCodeSelect = document.getElementById('bank-code-select');
const accountNumberSelect = document.getElementById('account-number-select');

function fetchAndSetAccounts() {
    const bank_code = bankCodeSelect.value;
    accountNumberSelect.innerHTML = '<option value="">Loading...</option>';
    fetch('/get_accounts', {
        method: 'POST',
        headers: {'Content-Type': 'application/x-www-form-urlencoded'},
        body: `bank_code=${encodeURIComponent(bank_code)}`
    })
    .then(resp => resp.json())
    .then(data => {
        accountNumberSelect.innerHTML = '';
        if(data.success && data.accounts.length > 0){
            accountNumberSelect.innerHTML = '<option value="">-- Select Account --</option>';
            data.accounts.forEach(function(acct){
                const opt = document.createElement('option');
                opt.value = acct;
                opt.text = acct;
                accountNumberSelect.appendChild(opt);
            });
        } else {
            accountNumberSelect.innerHTML = '<option value="">No accounts found</option>';
        }
    })
    .catch(() => {
        accountNumberSelect.innerHTML = '<option value="">Error</option>';
    });
}

if (bankCodeSelect) {
    fetchAndSetAccounts();
    bankCodeSelect.addEventListener('change', fetchAndSetAccounts);
}




// --- FINAL MINIMAL RECONCILE SCRIPT (UPDATED) ---
// document.getElementById('reconcile-form').addEventListener('submit', function(e) {
//     e.preventDefault();
//     const btn = document.getElementById('reconcile-btn');
//     btn.disabled = true;
//     btn.textContent = 'Reconciling...';
//     const resultDiv = document.getElementById('reconcile-result');
//     resultDiv.textContent = 'Working...';

//     // const bank_table = bankTableSelect.value;
//     // const account_number = accountNumberSelect.value;
//     // const bank_code = bankTableSelect.selectedOptions[0].getAttribute('data-code');
//     // const fin_table = 'fin_data';

//     // const formData = new URLSearchParams();
//     // formData.append('bank_table', bank_table);
//     // formData.append('fin_table', fin_table);
//     // formData.append('bank_code', bank_code);
//     // formData.append('account_number', account_number);

//     const bank_code = document.getElementById('bank-code-select').value;
//     const account_number = document.getElementById('account-number-select').value;

//     const formData = new URLSearchParams();
//     formData.append('bank_code', bank_code);
//     formData.append('account_number', account_number);


//     fetch('/reconcile', {
//         method: 'POST',
//         headers: {'Content-Type': 'application/x-www-form-urlencoded'},
//         body: formData
//     })
//     .then(resp => resp.json())
//     .then(data => {
//         if(data.success) {
//             resultDiv.innerText =
//                 `Matched: ${data.matched_count}\n` +
//                 `Unmatched (Bank): ${data.unmatched_bank_count}\n` +
//                 `Unmatched (Finance): ${data.unmatched_finance_count}`;
//         } else {
//             resultDiv.innerText = data.msg || 'Unknown error';
//         }
//     })
//     .catch(err => {
//         resultDiv.innerText = `Error: ${err}`;
//     })
//     .finally(() => {
//         btn.disabled = false;
//         btn.textContent = 'Reconcile';
//     });
// });



document.getElementById('reconcile-form').addEventListener('submit', function(e) {
    e.preventDefault();
    const btn = document.getElementById('reconcile-btn');
    btn.disabled = true;
    btn.textContent = 'Reconciling...';
    const resultDiv = document.getElementById('reconcile-result');
    resultDiv.textContent = 'Working...';

    const bank_code = document.getElementById('bank-code-select').value;
    const account_number = document.getElementById('account-number-select').value;

    const formData = new URLSearchParams();
    formData.append('bank_code', bank_code);
    formData.append('account_number', account_number);

    fetch('/reconcile', {
        method: 'POST',
        headers: {'Content-Type': 'application/x-www-form-urlencoded'},
        body: formData.toString()
    })
    .then(resp => resp.json())
    .then(data => {
        if(data.success) {
            resultDiv.innerText =
                `Matched: ${data.matched_count}\n` +
                `Unmatched (Bank): ${data.unmatched_bank_count}\n` +
                `Unmatched (Finance): ${data.unmatched_finance_count}`;
        } else {
            resultDiv.innerText = data.msg || 'Unknown error';
        }
    })
    .catch(err => {
        resultDiv.innerText = 'Error: ' + err;
    })
    .finally(() => {
        btn.disabled = false;
        btn.textContent = 'Reconcile';
    });
});




// --- Account dropdown logic for Bank-Fin-Tally Match ---
const bftBankSelect = document.getElementById('bft-bank-code-select');
const bftAcctSelect = document.getElementById('bft-account-number-select');

function fetchBFTAccounts() {
    bftAcctSelect.innerHTML = '<option value="">Loading...</option>';
    fetch('/get_bft_accounts', {
        method: 'POST',
        headers: {'Content-Type':'application/x-www-form-urlencoded'},
        body: `bank_code=${encodeURIComponent(bftBankSelect.value)}`
    })
    .then(r => r.json())
    .then(d => {
        bftAcctSelect.innerHTML = '';
        if(d.success && d.accounts.length) {
            bftAcctSelect.innerHTML = '<option value="">-- Select Account --</option>';
            d.accounts.forEach(a => {
                const opt = document.createElement('option');
                opt.value = a;
                opt.text = a;
                bftAcctSelect.appendChild(opt);
            });
        } else {
            bftAcctSelect.innerHTML = '<option value="">No accounts found</option>';
        }
    })
    .catch(() => {
        bftAcctSelect.innerHTML = '<option value="">Error</option>';
    });
}

if (bftBankSelect) {
    fetchBFTAccounts();
    bftBankSelect.addEventListener('change', fetchBFTAccounts);
}

document.getElementById('bft-reconcile-form').addEventListener('submit', function(e) {
    e.preventDefault();
    const btn = document.getElementById('bft-reconcile-btn');
    btn.disabled = true;
    btn.textContent = 'Reconciling...';
    const resultDiv = document.getElementById('bft-reconcile-result');
    resultDiv.textContent = 'Working...';

    const bank_code = bftBankSelect.value;
    const account_number = bftAcctSelect.value;

    const formData = new URLSearchParams();
    formData.append('bank_code', bank_code);
    formData.append('account_number', account_number);

    fetch('/reconcile_bft', {
        method: 'POST',
        headers: {'Content-Type':'application/x-www-form-urlencoded'},
        body: formData
    })
    .then(resp => resp.json())
    .then(data => {
        if(data.success) {
            resultDiv.innerText =
                `Matched: ${data.matched_count}\n` +
                `Unmatched (BF): ${data.unmatched_bf_count}\n` +
                `Unmatched (Tally): ${data.unmatched_tally_count}`;
        } else {
            resultDiv.innerText = data.msg || 'Unknown error';
        }
    })
    .catch(err => {
        resultDiv.innerText = `Error: ${err}`;
    })
    .finally(() => {
        btn.disabled = false;
        btn.textContent = 'Reconcile';
    });
});

// --- Account dropdown logic for Bank-Tally Match ---
const btBankSelect = document.getElementById('bank-tally-bank-code-select');
const btAcctSelect = document.getElementById('bank-tally-account-number-select');

function fetchBTAccounts() {
    btAcctSelect.innerHTML = '<option value="">Loading...</option>';
    fetch('/get_accounts', {
        method: 'POST',
        headers: {'Content-Type':'application/x-www-form-urlencoded'},
        // body: `bank_table=${btBankSelect.value === "MDB" ? "mdb_data" : "mtb_data"}`
        body: `bank_code=${encodeURIComponent(btBankSelect.value)}`

    })
    .then(r => r.json())
    .then(d => {
        btAcctSelect.innerHTML = '';
        if(d.success && d.accounts.length) {
            btAcctSelect.innerHTML = '<option value="">-- Select Account --</option>';
            d.accounts.forEach(a => {
                const opt = document.createElement('option');
                opt.value = a;
                opt.text = a;
                btAcctSelect.appendChild(opt);
            });
        } else {
            btAcctSelect.innerHTML = '<option value="">No accounts found</option>';
        }
    })
    .catch(() => {
        btAcctSelect.innerHTML = '<option value="">Error</option>';
    });
}

if (btBankSelect) {
    fetchBTAccounts();
    btBankSelect.addEventListener('change', fetchBTAccounts);
}

document.getElementById('bank-tally-reconcile-form').addEventListener('submit', function(e) {
    e.preventDefault();
    const btn = document.getElementById('bank-tally-reconcile-btn');
    btn.disabled = true;
    btn.textContent = 'Reconciling...';
    const resultDiv = document.getElementById('bank-tally-reconcile-result');
    resultDiv.textContent = 'Working...';

    const bank_code = btBankSelect.value;
    const account_number = btAcctSelect.value;

    const formData = new URLSearchParams();
    formData.append('bank_code', bank_code);
    formData.append('account_number', account_number);

    fetch('/bank_tally/reconcile', {
        method: 'POST',
        headers: {'Content-Type':'application/x-www-form-urlencoded'},
        body: formData
    })
    .then(resp => resp.json())
    .then(data => {
        if(data.success) {
            resultDiv.innerText = `Matched: ${data.matched_count}`;
        } else {
            resultDiv.innerText = data.msg || 'Unknown error';
        }
    })
    .catch(err => {
        resultDiv.innerText = `Error: ${err}`;
    })
    .finally(() => {
        btn.disabled = false;
        btn.textContent = 'Reconcile';
    });

});

// --- Universal Bank/Account Dropdown Logic (all tabs) ---

function populateBankDropdown(selectId, callback) {
    const bankSelect = document.getElementById(selectId);
    if (!bankSelect) return;
    bankSelect.innerHTML = '<option value="">Loading...</option>';
    fetch('/get_bank_codes', { method: 'GET' })
        .then(resp => resp.json())
        .then(data => {
            bankSelect.innerHTML = '<option value="">-- Select Bank --</option>';
            if (data.success && data.bank_codes.length > 0) {
                data.bank_codes.forEach(function(code) {
                    const opt = document.createElement('option');
                    opt.value = code;
                    opt.text = code;
                    bankSelect.appendChild(opt);
                });
            } else {
                bankSelect.innerHTML = '<option value="">No banks found</option>';
            }
            if (callback) callback();
        })
        .catch(() => {
            bankSelect.innerHTML = '<option value="">Error</option>';
        });
}

function populateAccountDropdown(selectId, bankCode) {
    const acctSelect = document.getElementById(selectId);
    if (!acctSelect) return;
    if (!bankCode) {
        acctSelect.innerHTML = '<option value="">-- Select Account --</option>';
        return;
    }
    acctSelect.innerHTML = '<option value="">Loading...</option>';
    fetch(`/get_acct_nos?bank_code=${encodeURIComponent(bankCode)}`, { method: 'GET' })
        .then(resp => resp.json())
        .then(data => {
            acctSelect.innerHTML = '<option value="">-- Select Account --</option>';
            if (data.success && data.acct_nos.length > 0) {
                data.acct_nos.forEach(function(acct) {
                    const opt = document.createElement('option');
                    opt.value = acct;
                    opt.text = acct;
                    acctSelect.appendChild(opt);
                });
            } else {
                acctSelect.innerHTML = '<option value="">No accounts found</option>';
            }
        })
        .catch(() => {
            acctSelect.innerHTML = '<option value="">Error</option>';
        });
}

// --- Bank-Fin Match Tab ---
if (document.getElementById('bank-code-select')) {
    populateBankDropdown('bank-code-select', function() {
        const bankCode = document.getElementById('bank-code-select').value;
        populateAccountDropdown('account-number-select', bankCode);
    });
    document.getElementById('bank-code-select').addEventListener('change', function() {
        const bankCode = this.value;
        populateAccountDropdown('account-number-select', bankCode);
    });
}

// --- Bank-Fin-Tally Match Tab ---
if (document.getElementById('bft-bank-code-select')) {
    populateBankDropdown('bft-bank-code-select', function() {
        const bankCode = document.getElementById('bft-bank-code-select').value;
        populateAccountDropdown('bft-account-number-select', bankCode);
    });
    document.getElementById('bft-bank-code-select').addEventListener('change', function() {
        const bankCode = this.value;
        populateAccountDropdown('bft-account-number-select', bankCode);
    });
}

// --- Bank-Tally Match Tab ---
if (document.getElementById('bank-tally-bank-code-select')) {
    populateBankDropdown('bank-tally-bank-code-select', function() {
        const bankCode = document.getElementById('bank-tally-bank-code-select').value;
        populateAccountDropdown('bank-tally-account-number-select', bankCode);
    });
    document.getElementById('bank-tally-bank-code-select').addEventListener('change', function() {
        const bankCode = this.value;
        populateAccountDropdown('bank-tally-account-number-select', bankCode);
    });
}

// --- Reports Tab: Unmatched Bank Report ---

// Helper: Fetch unique bank codes
function fetchUnmatchedBankCodes() {
    const select = document.getElementById('unmatched-bank-code-select');
    select.innerHTML = '<option value="">Loading...</option>';
    fetch('/get_bank_codes', { method: 'GET' })
        .then(resp => resp.json())
        .then(data => {
            select.innerHTML = '<option value="">-- Select Bank --</option>';
            if (data.success && data.bank_codes.length) {
                data.bank_codes.forEach(code => {
                    const opt = document.createElement('option');
                    opt.value = code;
                    opt.text = code;
                    select.appendChild(opt);
                });
            } else {
                select.innerHTML = '<option value="">No banks found</option>';
            }
        })
        .catch(() => {
            select.innerHTML = '<option value="">Error</option>';
        });
}

// Helper: Fetch unique account numbers for selected bank code
function fetchUnmatchedAcctNos() {
    const bankCode = document.getElementById('unmatched-bank-code-select').value;
    const select = document.getElementById('unmatched-acct-no-select');
    select.innerHTML = '<option value="">Loading...</option>';
    if (!bankCode) {
        select.innerHTML = '<option value="">-- Select Account --</option>';
        return;
    }
    fetch(`/get_acct_nos?bank_code=${encodeURIComponent(bankCode)}`, { method: 'GET' })
        .then(resp => resp.json())
        .then(data => {
            select.innerHTML = '<option value="">-- Select Account --</option>';
            if (data.success && data.acct_nos.length) {
                data.acct_nos.forEach(acct => {
                    const opt = document.createElement('option');
                    opt.value = acct;
                    opt.text = acct;
                    select.appendChild(opt);
                });
            } else {
                select.innerHTML = '<option value="">No accounts found</option>';
            }
        })
        .catch(() => {
            select.innerHTML = '<option value="">Error</option>';
        });
}

// Helper: Fetch unique years for statement_year dropdown
function fetchUnmatchedStatementYears() {
    const select = document.getElementById('unmatched-statement-year-select');
    select.innerHTML = '<option value="">Loading...</option>';
    fetch('/get_statement_years', { method: 'GET' })
    .then(resp => resp.json())
    .then(data => {
            select.innerHTML = '<option value="">-- Select Year --</option>';
            if (data.success && data.years.length) {
                data.years.forEach(year => {
                    const opt = document.createElement('option');
                    opt.value = year;
                    opt.text = year;
                    select.appendChild(opt);
                });
            } else {
                select.innerHTML = '<option value="">No years found</option>';
            }
        })
        .catch(() => {
            select.innerHTML = '<option value="">Error</option>';
        });
}

// Helper: Fetch unique months for statement_month dropdown
function fetchUnmatchedStatementMonths() {
    const select = document.getElementById('unmatched-statement-month-select');
    select.innerHTML = '<option value="">Loading...</option>';
    fetch('/get_statement_months', { method: 'GET' })
        .then(resp => resp.json())
        .then(data => {
            select.innerHTML = '<option value="">-- Select Month --</option>';
            if (data.success && data.months.length) {
                data.months.forEach(month => {
                    const opt = document.createElement('option');
                    opt.value = month;
                    opt.text = month;
                    select.appendChild(opt);
                });
        } else {
                select.innerHTML = '<option value="">No months found</option>';
            }
        })
        .catch(() => {
            select.innerHTML = '<option value="">Error</option>';
        });
}

// Populate filters on tab show
if (document.getElementById('unmatched-bank-code-select')) {
    fetchUnmatchedBankCodes();
    fetchUnmatchedStatementYears();
    fetchUnmatchedStatementMonths();
    document.getElementById('unmatched-bank-code-select').addEventListener('change', fetchUnmatchedAcctNos);
}

// --- Column order for unmatched bank report (custom subset) ---
const unmatchedBankColumnOrder = [
    'S/N', 'bank_uid', 'bank_code', 'acct_no', 'statement_month', 'statement_year',
    'B_Date', 'B_Particulars', 'B_Ref_Cheque', 'B_Withdrawal', 'B_Deposit', 'bank_ven'
];

// --- Date columns for formatting ---
const unmatchedBankDateColumns = ['B_Date'];

// Handle form submission
const unmatchedForm = document.getElementById('unmatched-bank-form');
if (unmatchedForm) {
    unmatchedForm.addEventListener('submit', function(e) {
        e.preventDefault();
        const btn = document.getElementById('unmatched-bank-btn');
    btn.disabled = true;
        btn.textContent = 'Generating...';
        const resultDiv = document.getElementById('unmatched-bank-result');
        resultDiv.textContent = 'Generating report...';

        const bank_code = document.getElementById('unmatched-bank-code-select').value;
        const acct_no = document.getElementById('unmatched-acct-no-select').value;
        const statement_month = document.getElementById('unmatched-statement-month-select').value;
        const statement_year = document.getElementById('unmatched-statement-year-select').value;

        fetch('/reports/unmatched_bank', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ bank_code, acct_no, statement_month, statement_year })
    })
    .then(resp => resp.json())
    .then(data => {
        if (data.success) {
                if (data.data.length === 0) {
                    resultDiv.textContent = 'No unmatched records found.';
                } else {
                    // Add serial number to each row
                    const rowsWithSN = data.data.map((row, idx) => ({...row, 'S/N': idx + 1}));
                    // Only display the specified columns, in order
                    const columns = unmatchedBankColumnOrder.filter(col => rowsWithSN[0].hasOwnProperty(col));
                    let html = `<b>Unmatched Records:</b><br><div class='report-table-wrapper'><table class="report-table"><tr>`;
                    // Table headers
                    columns.forEach(key => {
                        html += `<th>${key}</th>`;
                    });
                    html += '</tr></div>';
                    // Table rows
                    rowsWithSN.forEach(row => {
                        html += '<tr>';
                        columns.forEach(key => {
                            let val = row[key];
                            html += `<td>${val === null ? '' : val}</td>`;
                        });
                        html += '</tr>';
                    });
                    html += '</table>';
                    resultDiv.innerHTML = html;
                }
        } else {
                resultDiv.textContent = 'Error: ' + (data.msg || 'Failed to generate report');
        }
    })
    .catch(err => {
        resultDiv.textContent = 'Error: ' + err;
    })
    .finally(() => {
        btn.disabled = false;
            btn.textContent = 'Generate Report';
        });
    });
}

// --- Enable/Disable Generate Report button in Reports tab ---
function updateUnmatchedReportBtnState() {
    const bank = document.getElementById('unmatched-bank-code-select').value;
    const acct = document.getElementById('unmatched-acct-no-select').value;
    const month = document.getElementById('unmatched-statement-month-select').value;
    const year = document.getElementById('unmatched-statement-year-select').value;
    const btn = document.getElementById('unmatched-bank-btn');
    btn.disabled = !(bank && acct && month && year);
}

if (document.getElementById('unmatched-bank-form')) {
    // Disable button by default
    document.getElementById('unmatched-bank-btn').disabled = true;
    // Attach to dropdowns
    document.getElementById('unmatched-bank-code-select').addEventListener('change', updateUnmatchedReportBtnState);
    document.getElementById('unmatched-acct-no-select').addEventListener('change', updateUnmatchedReportBtnState);
    document.getElementById('unmatched-statement-month-select').addEventListener('change', updateUnmatchedReportBtnState);
    document.getElementById('unmatched-statement-year-select').addEventListener('change', updateUnmatchedReportBtnState);
}

// --- Download Excel for Unmatched Bank Report ---
if (document.getElementById('unmatched-bank-form')) {
    // Add Download Excel button if not present
    if (!document.getElementById('unmatched-bank-excel-btn')) {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'parser-parse-btn';
        btn.id = 'unmatched-bank-excel-btn';
        btn.textContent = 'Download Excel';
    btn.disabled = true;
        document.querySelector('#unmatched-bank-form .parser-row-parse').appendChild(btn);
    }
    // Enable/disable Download Excel button with same logic as Generate Report
    function updateUnmatchedExcelBtnState() {
        const bank = document.getElementById('unmatched-bank-code-select').value;
        const acct = document.getElementById('unmatched-acct-no-select').value;
        const month = document.getElementById('unmatched-statement-month-select').value;
        const year = document.getElementById('unmatched-statement-year-select').value;
        const btn = document.getElementById('unmatched-bank-excel-btn');
        btn.disabled = !(bank && acct && month && year);
    }
    document.getElementById('unmatched-bank-code-select').addEventListener('change', updateUnmatchedExcelBtnState);
    document.getElementById('unmatched-acct-no-select').addEventListener('change', updateUnmatchedExcelBtnState);
    document.getElementById('unmatched-statement-month-select').addEventListener('change', updateUnmatchedExcelBtnState);
    document.getElementById('unmatched-statement-year-select').addEventListener('change', updateUnmatchedExcelBtnState);
    // Download logic
    document.getElementById('unmatched-bank-excel-btn').addEventListener('click', function() {
        const bank_code = document.getElementById('unmatched-bank-code-select').value;
        const acct_no = document.getElementById('unmatched-acct-no-select').value;
        const statement_month = document.getElementById('unmatched-statement-month-select').value;
        const statement_year = document.getElementById('unmatched-statement-year-select').value;
        const payload = { bank_code, acct_no, statement_month, statement_year };
        const btn = this;
    btn.disabled = true;
        btn.textContent = 'Preparing...';
        fetch('/reports/unmatched_bank_excel', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        })
        .then(resp => {
            if (!resp.ok) throw new Error('Failed to download Excel');
            return resp.blob();
        })
        .then(blob => {
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'unmatched_bank_report.xlsx';
            document.body.appendChild(a);
            a.click();
            a.remove();
            window.URL.revokeObjectURL(url);
        })
        .catch(() => {
            alert('Failed to download Excel report.');
        })
        .finally(() => {
        btn.disabled = false;
            btn.textContent = 'Download Excel';
        });
    });
}

// --- Column order for unmatched tally report (custom subset) ---
const unmatchedTallyColumnOrder = [
    'S/N', 'tally_uid', 'bank_code', 'acct_no', 'statement_month', 'statement_year', 'unit_name',
    'T_Date', 'dr_cr', 'T_Particulars', 'T_Vch_Type', 'T_Vch_No', 'T_Debit', 'T_Credit', 'tally_ven'
];

// --- Helper functions for tally_data dropdowns ---
function fetchUnmatchedTallyBankCodes() {
    const select = document.getElementById('unmatched-tally-bank-code-select');
    select.innerHTML = '<option value="">Loading...</option>';
    fetch('/get_tally_bank_codes', { method: 'GET' })
        .then(resp => resp.json())
        .then(data => {
            select.innerHTML = '<option value="">-- Select Bank --</option>';
            if (data.success && data.bank_codes.length) {
                data.bank_codes.forEach(code => {
                    const opt = document.createElement('option');
                    opt.value = code;
                    opt.text = code;
                    select.appendChild(opt);
                });
            } else {
                select.innerHTML = '<option value="">No banks found</option>';
            }
        })
        .catch(() => {
            select.innerHTML = '<option value="">Error</option>';
        });
}
function fetchUnmatchedTallyAcctNos() {
    const bankCode = document.getElementById('unmatched-tally-bank-code-select').value;
    const select = document.getElementById('unmatched-tally-acct-no-select');
    select.innerHTML = '<option value="">Loading...</option>';
    if (!bankCode) {
        select.innerHTML = '<option value="">-- Select Account --</option>';
        return;
    }
    fetch(`/get_tally_acct_nos?bank_code=${encodeURIComponent(bankCode)}`, { method: 'GET' })
    .then(resp => resp.json())
    .then(data => {
            select.innerHTML = '<option value="">-- Select Account --</option>';
            if (data.success && data.acct_nos.length) {
                data.acct_nos.forEach(acct => {
                    const opt = document.createElement('option');
                    opt.value = acct;
                    opt.text = acct;
                    select.appendChild(opt);
                });
            } else {
                select.innerHTML = '<option value="">No accounts found</option>';
            }
        })
        .catch(() => {
            select.innerHTML = '<option value="">Error</option>';
        });
}
function fetchUnmatchedTallyStatementYears() {
    const select = document.getElementById('unmatched-tally-statement-year-select');
    select.innerHTML = '<option value="">Loading...</option>';
    fetch('/get_tally_statement_years', { method: 'GET' })
        .then(resp => resp.json())
        .then(data => {
            select.innerHTML = '<option value="">-- Select Year --</option>';
            if (data.success && data.years.length) {
                data.years.forEach(year => {
                    const opt = document.createElement('option');
                    opt.value = year;
                    opt.text = year;
                    select.appendChild(opt);
                });
        } else {
                select.innerHTML = '<option value="">No years found</option>';
            }
        })
        .catch(() => {
            select.innerHTML = '<option value="">Error</option>';
        });
}
function fetchUnmatchedTallyStatementMonths() {
    const select = document.getElementById('unmatched-tally-statement-month-select');
    select.innerHTML = '<option value="">Loading...</option>';
    fetch('/get_tally_statement_months', { method: 'GET' })
        .then(resp => resp.json())
        .then(data => {
            select.innerHTML = '<option value="">-- Select Month --</option>';
            if (data.success && data.months.length) {
                data.months.forEach(month => {
                    const opt = document.createElement('option');
                    opt.value = month;
                    opt.text = month;
                    select.appendChild(opt);
                });
            } else {
                select.innerHTML = '<option value="">No months found</option>';
            }
        })
        .catch(() => {
            select.innerHTML = '<option value="">Error</option>';
        });
}

// --- Date columns for formatting ---
const unmatchedTallyDateColumns = ['T_Date'];

// Populate filters on tab show for tally
if (document.getElementById('unmatched-tally-bank-code-select')) {
    fetchUnmatchedTallyBankCodes();
    fetchUnmatchedTallyStatementYears();
    fetchUnmatchedTallyStatementMonths();
    document.getElementById('unmatched-tally-bank-code-select').addEventListener('change', fetchUnmatchedTallyAcctNos);
}

// Enable/Disable Generate Report button in Tally Reports tab
function updateUnmatchedTallyReportBtnState() {
    const bank = document.getElementById('unmatched-tally-bank-code-select').value;
    const acct = document.getElementById('unmatched-tally-acct-no-select').value;
    const month = document.getElementById('unmatched-tally-statement-month-select').value;
    const year = document.getElementById('unmatched-tally-statement-year-select').value;
    const btn = document.getElementById('unmatched-tally-btn');
    btn.disabled = !(bank && acct && month && year);
}
if (document.getElementById('unmatched-tally-form')) {
    document.getElementById('unmatched-tally-btn').disabled = true;
    document.getElementById('unmatched-tally-bank-code-select').addEventListener('change', updateUnmatchedTallyReportBtnState);
    document.getElementById('unmatched-tally-acct-no-select').addEventListener('change', updateUnmatchedTallyReportBtnState);
    document.getElementById('unmatched-tally-statement-month-select').addEventListener('change', updateUnmatchedTallyReportBtnState);
    document.getElementById('unmatched-tally-statement-year-select').addEventListener('change', updateUnmatchedTallyReportBtnState);
}

// Download Excel button for Tally
if (document.getElementById('unmatched-tally-form')) {
    if (!document.getElementById('unmatched-tally-excel-btn')) {
        const btn = document.createElement('button');
        btn.type = 'button';
        btn.className = 'parser-parse-btn';
        btn.id = 'unmatched-tally-excel-btn';
        btn.textContent = 'Download Excel';
    btn.disabled = true;
        document.querySelector('#unmatched-tally-form .parser-row-parse').appendChild(btn);
    }
    function updateUnmatchedTallyExcelBtnState() {
        const bank = document.getElementById('unmatched-tally-bank-code-select').value;
        const acct = document.getElementById('unmatched-tally-acct-no-select').value;
        const month = document.getElementById('unmatched-tally-statement-month-select').value;
        const year = document.getElementById('unmatched-tally-statement-year-select').value;
        const btn = document.getElementById('unmatched-tally-excel-btn');
        btn.disabled = !(bank && acct && month && year);
    }
    document.getElementById('unmatched-tally-bank-code-select').addEventListener('change', updateUnmatchedTallyExcelBtnState);
    document.getElementById('unmatched-tally-acct-no-select').addEventListener('change', updateUnmatchedTallyExcelBtnState);
    document.getElementById('unmatched-tally-statement-month-select').addEventListener('change', updateUnmatchedTallyExcelBtnState);
    document.getElementById('unmatched-tally-statement-year-select').addEventListener('change', updateUnmatchedTallyExcelBtnState);
    document.getElementById('unmatched-tally-excel-btn').addEventListener('click', function() {
        const bank_code = document.getElementById('unmatched-tally-bank-code-select').value;
        const acct_no = document.getElementById('unmatched-tally-acct-no-select').value;
        const statement_month = document.getElementById('unmatched-tally-statement-month-select').value;
        const statement_year = document.getElementById('unmatched-tally-statement-year-select').value;
        const payload = { bank_code, acct_no, statement_month, statement_year };
        const btn = this;
        btn.disabled = true;
        btn.textContent = 'Preparing...';
        fetch('/reports/unmatched_tally_excel', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        })
        .then(resp => {
            if (!resp.ok) throw new Error('Failed to download Excel');
            return resp.blob();
        })
        .then(blob => {
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'unmatched_tally_report.xlsx';
            document.body.appendChild(a);
            a.click();
            a.remove();
            window.URL.revokeObjectURL(url);
        })
        .catch(() => {
            alert('Failed to download Excel report.');
        })
        .finally(() => {
            btn.disabled = false;
            btn.textContent = 'Download Excel';
        });
    });
}

// Handle form submission for Tally
const unmatchedTallyForm = document.getElementById('unmatched-tally-form');
if (unmatchedTallyForm) {
    unmatchedTallyForm.addEventListener('submit', function(e) {
        e.preventDefault();
        const btn = document.getElementById('unmatched-tally-btn');
        btn.disabled = true;
        btn.textContent = 'Generating...';
        const resultDiv = document.getElementById('unmatched-tally-result');
        resultDiv.textContent = 'Generating report...';
        const bank_code = document.getElementById('unmatched-tally-bank-code-select').value;
        const acct_no = document.getElementById('unmatched-tally-acct-no-select').value;
        const statement_month = document.getElementById('unmatched-tally-statement-month-select').value;
        const statement_year = document.getElementById('unmatched-tally-statement-year-select').value;
        fetch('/reports/unmatched_tally', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ bank_code, acct_no, statement_month, statement_year })
        })
        .then(resp => resp.json())
        .then(data => {
            if (data.success) {
                if (data.data.length === 0) {
                    resultDiv.textContent = 'No unmatched records found.';
                } else {
                    // Add serial number to each row
                    const rowsWithSN = data.data.map((row, idx) => ({...row, 'S/N': idx + 1}));
                    // Only display the specified columns, in order
                    const columns = unmatchedTallyColumnOrder.filter(col => rowsWithSN[0].hasOwnProperty(col));
                    let html = `<b>Unmatched Records:</b><br><div class='report-table-wrapper'><table class="report-table"><tr>`;
                    // Table headers
                    columns.forEach(key => {
                        html += `<th>${key}</th>`;
                    });
                    html += '</tr></div>';
                    // Table rows
                    rowsWithSN.forEach(row => {
                        html += '<tr>';
                        columns.forEach(key => {
                            let val = row[key];
                            html += `<td>${val === null ? '' : val}</td>`;
                        });
                        html += '</tr>';
                    });
                    html += '</table>';
                    resultDiv.innerHTML = html;
                }
            } else {
                resultDiv.textContent = 'Error: ' + (data.msg || 'Failed to generate report');
            }
        })
        .catch(err => {
            resultDiv.textContent = 'Error: ' + err;
        })
        .finally(() => {
        btn.disabled = false;
            btn.textContent = 'Generate Report';
        });
    });
}

// --- Bank-Fin Matched Report Tab Logic ---

function fetchBFMatchedBankCodes() {
    const select = document.getElementById('bank-fin-matched-bank-code-select');
    select.innerHTML = '<option value="">Loading...</option>';
    fetch('/get_bank_codes', { method: 'GET' })
        .then(resp => resp.json())
        .then(data => {
            select.innerHTML = '<option value="">-- Select Bank --</option>';
            if (data.success && data.bank_codes.length) {
                data.bank_codes.forEach(code => {
                    const opt = document.createElement('option');
                    opt.value = code;
                    opt.text = code;
                    select.appendChild(opt);
                });
            } else {
                select.innerHTML = '<option value="">No banks found</option>';
            }
        })
        .catch(() => {
            select.innerHTML = '<option value="">Error</option>';
        });
}

function fetchBFMatchedAcctNos() {
    const bankCode = document.getElementById('bank-fin-matched-bank-code-select').value;
    const select = document.getElementById('bank-fin-matched-acct-no-select');
    select.innerHTML = '<option value="">Loading...</option>';
    if (!bankCode) {
        select.innerHTML = '<option value="">-- Select Account --</option>';
        return;
    }
    fetch(`/get_acct_nos?bank_code=${encodeURIComponent(bankCode)}`, { method: 'GET' })
        .then(resp => resp.json())
        .then(data => {
            select.innerHTML = '<option value="">-- Select Account --</option>';
            if (data.success && data.acct_nos.length) {
                data.acct_nos.forEach(acct => {
                    const opt = document.createElement('option');
                    opt.value = acct;
                    opt.text = acct;
                    select.appendChild(opt);
                });
            } else {
                select.innerHTML = '<option value="">No accounts found</option>';
            }
        })
        .catch(() => {
            select.innerHTML = '<option value="">Error</option>';
        });
}

function fetchBFMatchedStatementYears() {
    const select = document.getElementById('bank-fin-matched-statement-year-select');
    select.innerHTML = '<option value="">Loading...</option>';
    fetch('/get_statement_years', { method: 'GET' })
        .then(resp => resp.json())
        .then(data => {
            select.innerHTML = '<option value="">-- Select Year --</option>';
            if (data.success && data.years.length) {
                data.years.forEach(year => {
                    const opt = document.createElement('option');
                    opt.value = year;
                    opt.text = year;
                    select.appendChild(opt);
                });
            } else {
                select.innerHTML = '<option value="">No years found</option>';
            }
        })
        .catch(() => {
            select.innerHTML = '<option value="">Error</option>';
        });
}

function fetchBFMatchedStatementMonths() {
    const select = document.getElementById('bank-fin-matched-statement-month-select');
    select.innerHTML = '<option value="">Loading...</option>';
    fetch('/get_statement_months', { method: 'GET' })
        .then(resp => resp.json())
        .then(data => {
            select.innerHTML = '<option value="">-- Select Month --</option>';
            if (data.success && data.months.length) {
                data.months.forEach(month => {
                    const opt = document.createElement('option');
                    opt.value = month;
                    opt.text = month;
                    select.appendChild(opt);
                });
            } else {
                select.innerHTML = '<option value="">No months found</option>';
            }
        })
        .catch(() => {
            select.innerHTML = '<option value="">Error</option>';
        });
}

// Populate filters on tab show
if (document.getElementById('bank-fin-matched-bank-code-select')) {
    fetchBFMatchedBankCodes();
    fetchBFMatchedStatementYears();
    fetchBFMatchedStatementMonths();
    document.getElementById('bank-fin-matched-bank-code-select').addEventListener('change', fetchBFMatchedAcctNos);
}

// Enable/Disable Generate Report button
function updateBFMatchedReportBtnState() {
    const bank = document.getElementById('bank-fin-matched-bank-code-select').value;
    const acct = document.getElementById('bank-fin-matched-acct-no-select').value;
    const month = document.getElementById('bank-fin-matched-statement-month-select').value;
    const year = document.getElementById('bank-fin-matched-statement-year-select').value;
    const btn = document.getElementById('bank-fin-matched-btn');
    btn.disabled = !(bank && acct && month && year);
}
if (document.getElementById('bank-fin-matched-form')) {
    document.getElementById('bank-fin-matched-btn').disabled = true;
    document.getElementById('bank-fin-matched-bank-code-select').addEventListener('change', updateBFMatchedReportBtnState);
    document.getElementById('bank-fin-matched-acct-no-select').addEventListener('change', updateBFMatchedReportBtnState);
    document.getElementById('bank-fin-matched-statement-month-select').addEventListener('change', updateBFMatchedReportBtnState);
    document.getElementById('bank-fin-matched-statement-year-select').addEventListener('change', updateBFMatchedReportBtnState);
}

// --- Column order for Bank-Fin Matched report (custom subset) ---
const bfMatchedColumnOrder = [
    'S/N', 'bf_match_id', 'bf_source', 'bf_match_type', 'bank_uid', 'bank_code', 'acct_no',
    'B_Date', 'B_Particulars', 'B_Ref_Cheque', 'B_Withdrawal', 'B_Deposit', 'bank_ven',
    'statement_month', 'statement_year', 'fin_uid', 'F_Credit_Amount', 'F_Receiver_Name', 'F_Voucher_No', 'fin_ven'
];

// Handle form submission
const bfMatchedForm = document.getElementById('bank-fin-matched-form');
if (bfMatchedForm) {
    bfMatchedForm.addEventListener('submit', function(e) {
        e.preventDefault();
        const btn = document.getElementById('bank-fin-matched-btn');
        btn.disabled = true;
        btn.textContent = 'Generating...';
        const resultDiv = document.getElementById('bank-fin-matched-result');
        resultDiv.textContent = 'Generating report...';
        const bank_code = document.getElementById('bank-fin-matched-bank-code-select').value;
        const acct_no = document.getElementById('bank-fin-matched-acct-no-select').value;
        const statement_month = document.getElementById('bank-fin-matched-statement-month-select').value;
        const statement_year = document.getElementById('bank-fin-matched-statement-year-select').value;
        fetch('/reports/bank_fin_matched', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ bank_code, acct_no, statement_month, statement_year })
        })
        .then(resp => resp.json())
        .then(data => {
            if (data.success) {
                if (data.data.length === 0) {
                    resultDiv.textContent = 'No matched records found.';
                } else {
                    // Add serial number to each row
                    const rowsWithSN = data.data.map((row, idx) => ({...row, 'S/N': idx + 1}));
                    // Only display the specified columns, in order
                    const columns = bfMatchedColumnOrder.filter(col => rowsWithSN[0].hasOwnProperty(col));
                    let html = `<b>Matched Records:</b><br><div class='report-table-wrapper'><table class="report-table"><tr>`;
                    // Table headers
                    columns.forEach(key => {
                        html += `<th>${key}</th>`;
                    });
                    html += '</tr></div>';
                    // Table rows
                    rowsWithSN.forEach(row => {
                        html += '<tr>';
                        columns.forEach(key => {
                            let val = row[key];
                            html += `<td>${val === null ? '' : val}</td>`;
                        });
                        html += '</tr>';
                    });
                    html += '</table>';
                    resultDiv.innerHTML = html;
                }
            } else {
                resultDiv.textContent = 'Error: ' + (data.msg || 'Failed to generate report');
            }
        })
        .catch(err => {
            resultDiv.textContent = 'Error: ' + err;
        })
        .finally(() => {
            btn.disabled = false;
            btn.textContent = 'Generate Report';
        });
    });
}

// --- Bank-Fin Matched Excel Download ---
if (document.getElementById('bank-fin-matched-form')) {
    const excelBtn = document.getElementById('bank-fin-matched-excel-btn');
    excelBtn.disabled = true;
    function updateExcelBtnState() {
        const bank = document.getElementById('bank-fin-matched-bank-code-select').value;
        const acct = document.getElementById('bank-fin-matched-acct-no-select').value;
        const month = document.getElementById('bank-fin-matched-statement-month-select').value;
        const year = document.getElementById('bank-fin-matched-statement-year-select').value;
        excelBtn.disabled = !(bank && acct && month && year);
    }
    document.getElementById('bank-fin-matched-bank-code-select').addEventListener('change', updateExcelBtnState);
    document.getElementById('bank-fin-matched-acct-no-select').addEventListener('change', updateExcelBtnState);
    document.getElementById('bank-fin-matched-statement-month-select').addEventListener('change', updateExcelBtnState);
    document.getElementById('bank-fin-matched-statement-year-select').addEventListener('change', updateExcelBtnState);
    excelBtn.addEventListener('click', function() {
        const bank_code = document.getElementById('bank-fin-matched-bank-code-select').value;
        const acct_no = document.getElementById('bank-fin-matched-acct-no-select').value;
        const statement_month = document.getElementById('bank-fin-matched-statement-month-select').value;
        const statement_year = document.getElementById('bank-fin-matched-statement-year-select').value;
        const payload = { bank_code, acct_no, statement_month, statement_year };
        excelBtn.disabled = true;
        excelBtn.textContent = 'Preparing...';
        fetch('/reports/bank_fin_matched_excel', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        })
        .then(resp => {
            if (!resp.ok) throw new Error('Failed to download Excel');
            return resp.blob();
        })
        .then(blob => {
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'bank_fin_matched_report.xlsx';
            document.body.appendChild(a);
            a.click();
            a.remove();
            window.URL.revokeObjectURL(url);
        })
        .catch(() => {
            alert('Failed to download Excel report.');
        })
        .finally(() => {
            excelBtn.disabled = false;
            excelBtn.textContent = 'Download Excel';
        });
    });
}

// --- Bank-Fin-Tally Matched Report Tab Logic ---
function fetchBFTMatchedBankCodes() {
    const select = document.getElementById('bank-fin-tally-matched-bank-code-select');
    select.innerHTML = '<option value="">Loading...</option>';
    fetch('/get_bank_codes', { method: 'GET' })
        .then(resp => resp.json())
        .then(data => {
            select.innerHTML = '<option value="">-- Select Bank --</option>';
            if (data.success && data.bank_codes.length) {
                data.bank_codes.forEach(code => {
                    const opt = document.createElement('option');
                    opt.value = code;
                    opt.text = code;
                    select.appendChild(opt);
                });
            } else {
                select.innerHTML = '<option value="">No banks found</option>';
            }
        })
        .catch(() => {
            select.innerHTML = '<option value="">Error</option>';
        });
}
function fetchBFTMatchedAcctNos() {
    const bankCode = document.getElementById('bank-fin-tally-matched-bank-code-select').value;
    const select = document.getElementById('bank-fin-tally-matched-acct-no-select');
    select.innerHTML = '<option value="">Loading...</option>';
    if (!bankCode) {
        select.innerHTML = '<option value="">-- Select Account --</option>';
        return;
    }
    fetch(`/get_acct_nos?bank_code=${encodeURIComponent(bankCode)}`, { method: 'GET' })
        .then(resp => resp.json())
        .then(data => {
            select.innerHTML = '<option value="">-- Select Account --</option>';
            if (data.success && data.acct_nos.length) {
                data.acct_nos.forEach(acct => {
                    const opt = document.createElement('option');
                    opt.value = acct;
                    opt.text = acct;
                    select.appendChild(opt);
                });
            } else {
                select.innerHTML = '<option value="">No accounts found</option>';
            }
        })
        .catch(() => {
            select.innerHTML = '<option value="">Error</option>';
        });
}
function fetchBFTMatchedStatementYears() {
    const select = document.getElementById('bank-fin-tally-matched-statement-year-select');
    select.innerHTML = '<option value="">Loading...</option>';
    fetch('/get_statement_years', { method: 'GET' })
        .then(resp => resp.json())
        .then(data => {
            select.innerHTML = '<option value="">-- Select Year --</option>';
            if (data.success && data.years.length) {
                data.years.forEach(year => {
                    const opt = document.createElement('option');
                    opt.value = year;
                    opt.text = year;
                    select.appendChild(opt);
                });
            } else {
                select.innerHTML = '<option value="">No years found</option>';
            }
        })
        .catch(() => {
            select.innerHTML = '<option value="">Error</option>';
        });
}
function fetchBFTMatchedStatementMonths() {
    const select = document.getElementById('bank-fin-tally-matched-statement-month-select');
    select.innerHTML = '<option value="">Loading...</option>';
    fetch('/get_statement_months', { method: 'GET' })
        .then(resp => resp.json())
        .then(data => {
            select.innerHTML = '<option value="">-- Select Month --</option>';
            if (data.success && data.months.length) {
                data.months.forEach(month => {
                    const opt = document.createElement('option');
                    opt.value = month;
                    opt.text = month;
                    select.appendChild(opt);
                });
            } else {
                select.innerHTML = '<option value="">No months found</option>';
            }
        })
        .catch(() => {
            select.innerHTML = '<option value="">Error</option>';
        });
}
if (document.getElementById('bank-fin-tally-matched-bank-code-select')) {
    fetchBFTMatchedBankCodes();
    fetchBFTMatchedStatementYears();
    fetchBFTMatchedStatementMonths();
    document.getElementById('bank-fin-tally-matched-bank-code-select').addEventListener('change', fetchBFTMatchedAcctNos);
}
function updateBFTMatchedReportBtnState() {
    const bank = document.getElementById('bank-fin-tally-matched-bank-code-select').value;
    const acct = document.getElementById('bank-fin-tally-matched-acct-no-select').value;
    const month = document.getElementById('bank-fin-tally-matched-statement-month-select').value;
    const year = document.getElementById('bank-fin-tally-matched-statement-year-select').value;
    const btn = document.getElementById('bank-fin-tally-matched-btn');
    btn.disabled = !(bank && acct && month && year);
}
if (document.getElementById('bank-fin-tally-matched-form')) {
    document.getElementById('bank-fin-tally-matched-btn').disabled = true;
    document.getElementById('bank-fin-tally-matched-bank-code-select').addEventListener('change', updateBFTMatchedReportBtnState);
    document.getElementById('bank-fin-tally-matched-acct-no-select').addEventListener('change', updateBFTMatchedReportBtnState);
    document.getElementById('bank-fin-tally-matched-statement-month-select').addEventListener('change', updateBFTMatchedReportBtnState);
    document.getElementById('bank-fin-tally-matched-statement-year-select').addEventListener('change', updateBFTMatchedReportBtnState);
}
const bftMatchedColumnOrder = [
    'S/N', 'bft_match_id', 'bft_source', 'bft_match_type', 'bank_uid', 'bank_code', 'acct_no',
    'B_Date', 'B_Particulars', 'B_Ref_Cheque', 'B_Withdrawal', 'B_Deposit', 'bank_ven',
    'fin_uid', 'F_Credit_Amount', 'F_Receiver_Name', 'F_Voucher_No', 'fin_ven',
    'tally_uid', 'T_Date', 'dr_cr', 'T_Particulars', 'T_Vch_No', 'T_Debit', 'T_Credit',
    'tally_ven', 'statement_month', 'statement_year'
];
const bftMatchedForm = document.getElementById('bank-fin-tally-matched-form');
if (bftMatchedForm) {
    bftMatchedForm.addEventListener('submit', function(e) {
        e.preventDefault();
        const btn = document.getElementById('bank-fin-tally-matched-btn');
        btn.disabled = true;
        btn.textContent = 'Generating...';
        const resultDiv = document.getElementById('bank-fin-tally-matched-result');
        resultDiv.textContent = 'Generating report...';
        const bank_code = document.getElementById('bank-fin-tally-matched-bank-code-select').value;
        const acct_no = document.getElementById('bank-fin-tally-matched-acct-no-select').value;
        const statement_month = document.getElementById('bank-fin-tally-matched-statement-month-select').value;
        const statement_year = document.getElementById('bank-fin-tally-matched-statement-year-select').value;
        fetch('/reports/bank_fin_tally_matched', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ bank_code, acct_no, statement_month, statement_year })
        })
        .then(resp => resp.json())
        .then(data => {
            if (data.success) {
                if (data.data.length === 0) {
                    resultDiv.textContent = 'No matched records found.';
                } else {
                    const rowsWithSN = data.data.map((row, idx) => ({...row, 'S/N': idx + 1}));
                    const columns = bftMatchedColumnOrder.filter(col => rowsWithSN[0].hasOwnProperty(col));
                    let html = `<b>Matched Records:</b><br><div class='report-table-wrapper'><table class="report-table"><tr>`;
                    columns.forEach(key => { html += `<th>${key}</th>`; });
                    html += '</tr></div>';
                    rowsWithSN.forEach(row => {
                        html += '<tr>';
                        columns.forEach(key => { let val = row[key]; html += `<td>${val === null ? '' : val}</td>`; });
                        html += '</tr>';
                    });
                    html += '</table>';
                    resultDiv.innerHTML = html;
                }
            } else {
                resultDiv.textContent = 'Error: ' + (data.msg || 'Failed to generate report');
            }
        })
        .catch(err => { resultDiv.textContent = 'Error: ' + err; })
        .finally(() => { btn.disabled = false; btn.textContent = 'Generate Report'; });
    });
    // Excel download
    const excelBtn = document.getElementById('bank-fin-tally-matched-excel-btn');
    excelBtn.disabled = true;
    function updateExcelBtnState() {
        const bank = document.getElementById('bank-fin-tally-matched-bank-code-select').value;
        const acct = document.getElementById('bank-fin-tally-matched-acct-no-select').value;
        const month = document.getElementById('bank-fin-tally-matched-statement-month-select').value;
        const year = document.getElementById('bank-fin-tally-matched-statement-year-select').value;
        excelBtn.disabled = !(bank && acct && month && year);
    }
    document.getElementById('bank-fin-tally-matched-bank-code-select').addEventListener('change', updateExcelBtnState);
    document.getElementById('bank-fin-tally-matched-acct-no-select').addEventListener('change', updateExcelBtnState);
    document.getElementById('bank-fin-tally-matched-statement-month-select').addEventListener('change', updateExcelBtnState);
    document.getElementById('bank-fin-tally-matched-statement-year-select').addEventListener('change', updateExcelBtnState);
    excelBtn.addEventListener('click', function() {
        const bank_code = document.getElementById('bank-fin-tally-matched-bank-code-select').value;
        const acct_no = document.getElementById('bank-fin-tally-matched-acct-no-select').value;
        const statement_month = document.getElementById('bank-fin-tally-matched-statement-month-select').value;
        const statement_year = document.getElementById('bank-fin-tally-matched-statement-year-select').value;
        const payload = { bank_code, acct_no, statement_month, statement_year };
        excelBtn.disabled = true;
        excelBtn.textContent = 'Preparing...';
        fetch('/reports/bank_fin_tally_matched_excel', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        })
        .then(resp => {
            if (!resp.ok) throw new Error('Failed to download Excel');
            return resp.blob();
        })
        .then(blob => {
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'bank_fin_tally_matched_report.xlsx';
            document.body.appendChild(a);
            a.click();
            a.remove();
            window.URL.revokeObjectURL(url);
        })
        .catch(() => { alert('Failed to download Excel report.'); })
        .finally(() => { excelBtn.disabled = false; excelBtn.textContent = 'Download Excel'; });
    });
}

// --- Bank-Tally Matched Report Tab Logic ---
function fetchBTMatchedBankCodes() {
    const select = document.getElementById('bank-tally-matched-bank-code-select');
    select.innerHTML = '<option value="">Loading...</option>';
    fetch('/get_bank_codes', { method: 'GET' })
        .then(resp => resp.json())
        .then(data => {
            select.innerHTML = '<option value="">-- Select Bank --</option>';
            if (data.success && data.bank_codes.length) {
                data.bank_codes.forEach(code => {
                    const opt = document.createElement('option');
                    opt.value = code;
                    opt.text = code;
                    select.appendChild(opt);
                });
            } else {
                select.innerHTML = '<option value="">No banks found</option>';
            }
        })
        .catch(() => {
            select.innerHTML = '<option value="">Error</option>';
        });
}
function fetchBTMatchedAcctNos() {
    const bankCode = document.getElementById('bank-tally-matched-bank-code-select').value;
    const select = document.getElementById('bank-tally-matched-acct-no-select');
    select.innerHTML = '<option value="">Loading...</option>';
    if (!bankCode) {
        select.innerHTML = '<option value="">-- Select Account --</option>';
        return;
    }
    fetch(`/get_acct_nos?bank_code=${encodeURIComponent(bankCode)}`, { method: 'GET' })
        .then(resp => resp.json())
        .then(data => {
            select.innerHTML = '<option value="">-- Select Account --</option>';
            if (data.success && data.acct_nos.length) {
                data.acct_nos.forEach(acct => {
                    const opt = document.createElement('option');
                    opt.value = acct;
                    opt.text = acct;
                    select.appendChild(opt);
                });
            } else {
                select.innerHTML = '<option value="">No accounts found</option>';
            }
        })
        .catch(() => {
            select.innerHTML = '<option value="">Error</option>';
        });
}
function fetchBTMatchedStatementYears() {
    const select = document.getElementById('bank-tally-matched-statement-year-select');
    select.innerHTML = '<option value="">Loading...</option>';
    fetch('/get_statement_years', { method: 'GET' })
        .then(resp => resp.json())
        .then(data => {
            select.innerHTML = '<option value="">-- Select Year --</option>';
            if (data.success && data.years.length) {
                data.years.forEach(year => {
                    const opt = document.createElement('option');
                    opt.value = year;
                    opt.text = year;
                    select.appendChild(opt);
                });
            } else {
                select.innerHTML = '<option value="">No years found</option>';
            }
        })
        .catch(() => {
            select.innerHTML = '<option value="">Error</option>';
        });
}
function fetchBTMatchedStatementMonths() {
    const select = document.getElementById('bank-tally-matched-statement-month-select');
    select.innerHTML = '<option value="">Loading...</option>';
    fetch('/get_statement_months', { method: 'GET' })
        .then(resp => resp.json())
        .then(data => {
            select.innerHTML = '<option value="">-- Select Month --</option>';
            if (data.success && data.months.length) {
                data.months.forEach(month => {
                    const opt = document.createElement('option');
                    opt.value = month;
                    opt.text = month;
                    select.appendChild(opt);
                });
            } else {
                select.innerHTML = '<option value="">No months found</option>';
            }
        })
        .catch(() => {
            select.innerHTML = '<option value="">Error</option>';
        });
}
if (document.getElementById('bank-tally-matched-bank-code-select')) {
    fetchBTMatchedBankCodes();
    fetchBTMatchedStatementYears();
    fetchBTMatchedStatementMonths();
    document.getElementById('bank-tally-matched-bank-code-select').addEventListener('change', fetchBTMatchedAcctNos);
}
function updateBTMatchedReportBtnState() {
    const bank = document.getElementById('bank-tally-matched-bank-code-select').value;
    const acct = document.getElementById('bank-tally-matched-acct-no-select').value;
    const month = document.getElementById('bank-tally-matched-statement-month-select').value;
    const year = document.getElementById('bank-tally-matched-statement-year-select').value;
    const btn = document.getElementById('bank-tally-matched-btn');
    btn.disabled = !(bank && acct && month && year);
}
if (document.getElementById('bank-tally-matched-form')) {
    document.getElementById('bank-tally-matched-btn').disabled = true;
    document.getElementById('bank-tally-matched-bank-code-select').addEventListener('change', updateBTMatchedReportBtnState);
    document.getElementById('bank-tally-matched-acct-no-select').addEventListener('change', updateBTMatchedReportBtnState);
    document.getElementById('bank-tally-matched-statement-month-select').addEventListener('change', updateBTMatchedReportBtnState);
    document.getElementById('bank-tally-matched-statement-year-select').addEventListener('change', updateBTMatchedReportBtnState);
}
const btMatchedColumnOrder = [
    'S/N', 'bt_match_id', 'bt_source', 'bank_uid', 'acct_no', 'bank_code', 'B_Date',
    'B_Particulars', 'B_Ref_Cheque', 'B_Withdrawal', 'B_Deposit', 'bank_ven',
    'tally_uid', 'T_Date', 'dr_cr', 'T_Particulars', 'T_Vch_No', 'T_Debit', 'T_Credit',
    'tally_ven', 'statement_month', 'statement_year'
];
const btMatchedForm = document.getElementById('bank-tally-matched-form');
if (btMatchedForm) {
    btMatchedForm.addEventListener('submit', function(e) {
        e.preventDefault();
        const btn = document.getElementById('bank-tally-matched-btn');
        btn.disabled = true;
        btn.textContent = 'Generating...';
        const resultDiv = document.getElementById('bank-tally-matched-result');
        resultDiv.textContent = 'Generating report...';
        const bank_code = document.getElementById('bank-tally-matched-bank-code-select').value;
        const acct_no = document.getElementById('bank-tally-matched-acct-no-select').value;
        const statement_month = document.getElementById('bank-tally-matched-statement-month-select').value;
        const statement_year = document.getElementById('bank-tally-matched-statement-year-select').value;
        fetch('/reports/bank_tally_matched', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ bank_code, acct_no, statement_month, statement_year })
        })
        .then(resp => resp.json())
        .then(data => {
            if (data.success) {
                if (data.data.length === 0) {
                    resultDiv.textContent = 'No matched records found.';
                } else {
                    const rowsWithSN = data.data.map((row, idx) => ({...row, 'S/N': idx + 1}));
                    const columns = btMatchedColumnOrder.filter(col => rowsWithSN[0].hasOwnProperty(col));
                    let html = `<b>Matched Records:</b><br><div class='report-table-wrapper'><table class="report-table"><tr>`;
                    columns.forEach(key => { html += `<th>${key}</th>`; });
                    html += '</tr></div>';
                    rowsWithSN.forEach(row => {
                        html += '<tr>';
                        columns.forEach(key => { let val = row[key]; html += `<td>${val === null ? '' : val}</td>`; });
                        html += '</tr>';
                    });
                    html += '</table>';
                    resultDiv.innerHTML = html;
                }
            } else {
                resultDiv.textContent = 'Error: ' + (data.msg || 'Failed to generate report');
            }
        })
        .catch(err => { resultDiv.textContent = 'Error: ' + err; })
        .finally(() => { btn.disabled = false; btn.textContent = 'Generate Report'; });
    });
    // Excel download
    const excelBtn = document.getElementById('bank-tally-matched-excel-btn');
    excelBtn.disabled = true;
    function updateExcelBtnState() {
        const bank = document.getElementById('bank-tally-matched-bank-code-select').value;
        const acct = document.getElementById('bank-tally-matched-acct-no-select').value;
        const month = document.getElementById('bank-tally-matched-statement-month-select').value;
        const year = document.getElementById('bank-tally-matched-statement-year-select').value;
        excelBtn.disabled = !(bank && acct && month && year);
    }
    document.getElementById('bank-tally-matched-bank-code-select').addEventListener('change', updateExcelBtnState);
    document.getElementById('bank-tally-matched-acct-no-select').addEventListener('change', updateExcelBtnState);
    document.getElementById('bank-tally-matched-statement-month-select').addEventListener('change', updateExcelBtnState);
    document.getElementById('bank-tally-matched-statement-year-select').addEventListener('change', updateExcelBtnState);
    excelBtn.addEventListener('click', function() {
        const bank_code = document.getElementById('bank-tally-matched-bank-code-select').value;
        const acct_no = document.getElementById('bank-tally-matched-acct-no-select').value;
        const statement_month = document.getElementById('bank-tally-matched-statement-month-select').value;
        const statement_year = document.getElementById('bank-tally-matched-statement-year-select').value;
        const payload = { bank_code, acct_no, statement_month, statement_year };
        excelBtn.disabled = true;
        excelBtn.textContent = 'Preparing...';
        fetch('/reports/bank_tally_matched_excel', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        })
        .then(resp => {
            if (!resp.ok) throw new Error('Failed to download Excel');
            return resp.blob();
        })
        .then(blob => {
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'bank_tally_matched_report.xlsx';
            document.body.appendChild(a);
            a.click();
            a.remove();
            window.URL.revokeObjectURL(url);
        })
        .catch(() => { alert('Failed to download Excel report.'); })
        .finally(() => { excelBtn.disabled = false; excelBtn.textContent = 'Download Excel'; });
    });
}
