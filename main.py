from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel
from typing import Optional
import httpx
import os
import re
from datetime import datetime, timedelta, date as DateType
from zoneinfo import ZoneInfo
import psycopg2

# Pacific timezone
PACIFIC = ZoneInfo("America/Los_Angeles")

def now_pacific():
    """Get current datetime in Pacific time"""
    return datetime.now(PACIFIC)

def today_pacific():
    """Get current date in Pacific time"""
    return datetime.now(PACIFIC).date()
from psycopg2.extras import RealDictCursor

app = FastAPI(title="Casablanca Cash Flow API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Access code
ACCESS_CODE = "cflownk"

# Webhook URL for triggering updates
WEBHOOK_URL = "https://webhooks.tasklet.ai/v1/public/webhook?token=739e742528fc953b33f7fddb05705e9f"

# Database connection
DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db():
    """Get database connection"""
    if not DATABASE_URL:
        return None
    conn = psycopg2.connect(DATABASE_URL)
    return conn


def get_recent_bank_transactions(days: int = 15) -> list:
    """Get recent bank transactions for matching against scheduled transactions"""
    conn = get_db()
    if not conn:
        return []
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cutoff_date = today_pacific() - timedelta(days=days)
    
    cur.execute("""
        SELECT date, description, debit, credit, balance
        FROM bank_transactions 
        WHERE date >= %s
        ORDER BY date DESC
    """, (cutoff_date,))
    
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    return [dict(r) for r in rows]


def check_transaction_cleared(scheduled_txn: dict, real_transactions: list, scheduled_date: str) -> bool:
    """
    Check if a scheduled transaction has already cleared by matching against real bank transactions.
    
    Matching rules:
    - AmEx: Look for 'AMEX' or 'AMERICAN EXPRESS' in description, amount within 20% of scheduled
    - Payroll checks: Look for check numbers starting with 5 or containing '5-series', total within 20%
    - ADP/Tax: Look for 'ADP' in description
    - Comms & Execs: Look for recurring check patterns, large check amounts
    - Blue Shield: Look for 'BLUE SHIELD' or 'HEALTH' in description
    - TM Travel: Look for 'TM TRAVEL' or similar in description
    - General: Match by amount (within 10%) and date (within 3 days)
    """
    from datetime import datetime
    
    txn_type = scheduled_txn.get("type", "other")
    txn_amount = abs(scheduled_txn.get("amount", 0))
    txn_desc = scheduled_txn.get("desc", "").upper()
    scheduled_dt = datetime.strptime(scheduled_date, "%Y-%m-%d").date()
    
    # Look for matches in the date range (scheduled date - 3 days to + 2 days)
    date_range_start = scheduled_dt - timedelta(days=3)
    date_range_end = scheduled_dt + timedelta(days=2)
    
    for real_txn in real_transactions:
        real_date = real_txn["date"]
        if isinstance(real_date, str):
            real_date = datetime.strptime(real_date, "%Y-%m-%d").date()
        
        # Check if within date range
        if not (date_range_start <= real_date <= date_range_end):
            continue
        
        real_desc = (real_txn.get("description") or "").upper()
        real_debit = abs(float(real_txn.get("debit") or 0))
        real_credit = abs(float(real_txn.get("credit") or 0))
        
        # Different matching rules based on transaction type
        if txn_type == "amex":
            if ("AMEX" in real_desc or "AMERICAN EXPRESS" in real_desc) and real_debit > 0:
                # Check if amount is within 30% (AmEx payments can vary)
                if 0.7 * txn_amount <= real_debit <= 1.3 * txn_amount:
                    return True
        
        elif txn_type == "payroll":
            # Match payroll checks - look for 5-series check numbers or 'PAYROLL'
            if ("CHECK" in real_desc or real_desc.startswith("5")) and real_debit > 0:
                if 0.8 * txn_amount <= real_debit <= 1.2 * txn_amount:
                    return True
        
        elif txn_type == "payroll_tax":
            if ("ADP" in real_desc or "401K" in real_desc or "TAX" in real_desc) and real_debit > 0:
                if 0.8 * txn_amount <= real_debit <= 1.2 * txn_amount:
                    return True
        
        elif txn_type == "comms_execs":
            # Comms & Execs are recurring checks - match by amount
            if real_debit > 0 and 0.9 * txn_amount <= real_debit <= 1.1 * txn_amount:
                return True
        
        elif txn_type == "blue_shield":
            if ("BLUE SHIELD" in real_desc or "HEALTH" in real_desc or "INSURANCE" in real_desc):
                if real_debit > 0 and 0.8 * txn_amount <= real_debit <= 1.2 * txn_amount:
                    return True
        
        elif txn_type == "income":
            # Income transactions - look for matching credits
            if real_credit > 0 and 0.9 * txn_amount <= real_credit <= 1.1 * txn_amount:
                # Check if description matches somewhat
                if any(word in real_desc for word in ["WIRE", "TRANSFER", "DEPOSIT", "MVW", "CFI"]):
                    return True
        
        elif txn_type == "other":
            # General matching - look for similar amounts
            if real_debit > 0 and 0.9 * txn_amount <= real_debit <= 1.1 * txn_amount:
                # Check for keyword matches in description
                desc_words = txn_desc.split()
                if any(word in real_desc for word in desc_words if len(word) > 3):
                    return True
    
    return False


def get_pending_special_transactions(days_lookback: int = 15) -> dict:
    """
    Get special transactions that haven't cleared yet.
    Compares SPECIAL_TRANSACTIONS against real bank transactions.
    
    Returns dict with same structure as SPECIAL_TRANSACTIONS but only with pending items.
    """
    real_txns = get_recent_bank_transactions(days_lookback)
    today = today_pacific()
    
    pending = {}
    
    for date_str, txns in SPECIAL_TRANSACTIONS.items():
        scheduled_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        
        # Skip dates more than lookback days in the past
        if scheduled_date < today - timedelta(days=days_lookback):
            continue
        
        pending_txns = []
        for txn in txns:
            # For future dates, include all transactions
            if scheduled_date > today:
                pending_txns.append(txn)
            # For past/current dates, check if cleared
            elif not check_transaction_cleared(txn, real_txns, date_str):
                pending_txns.append(txn)
        
        if pending_txns:
            pending[date_str] = pending_txns
    
    return pending

def init_db():
    """Initialize database tables"""
    conn = get_db()
    if not conn:
        return
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS forecast (
            date DATE PRIMARY KEY,
            balance DECIMAL(12,2) NOT NULL,
            note TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bank_transactions (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            description TEXT,
            debit DECIMAL(12,2),
            credit DECIMAL(12,2),
            balance DECIMAL(12,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

def populate_forecast_if_needed():
    """Populate forecast table with default data if empty or stale"""
    conn = get_db()
    if not conn:
        return
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    # Check if we have proper forecast data (not just transaction-based entries)
    cur.execute("SELECT COUNT(*) as cnt FROM forecast WHERE note != 'Actual' OR note IS NULL")
    result = cur.fetchone()
    proper_count = result['cnt'] if result else 0
    
    if proper_count < 10:  # Need to repopulate
        # Clear existing forecast
        cur.execute("DELETE FROM forecast")
        
        # Insert default forecast
        for date_str, data in DEFAULT_FORECAST.items():
            cur.execute("""
                INSERT INTO forecast (date, balance, note)
                VALUES (%s, %s, %s)
            """, (date_str, data['balance'], data.get('note', '')))
        
        conn.commit()
        print(f"Populated forecast with {len(DEFAULT_FORECAST)} entries")
    
    cur.close()
    conn.close()

# Initialize on startup
@app.on_event("startup")
async def startup():
    init_db()
    populate_forecast_if_needed()

def verify_code(code: str):
    if code != ACCESS_CODE:
        raise HTTPException(status_code=401, detail="Invalid access code")

# Rolling 30-day averages (updated when data is submitted)
ROLLING_30_DAY = {
    "cash_in": 285000,
    "cash_out": 199500,
    "gross_profit": 40000,  # ~$40K/month cash flow profit (user estimate, additional sales revenue not fully tracked)
}

MONTHLY_PAYROLL = 206000

# Fallback forecast if database is empty
DEFAULT_FORECAST = {
    "2026-01-15": {"balance": 237000, "note": "Normal ops"},
    "2026-01-16": {"balance": 225000, "note": "AmEx $112K payment ($26.8K + $85.6K)"},
    "2026-01-17": {"balance": 221000, "note": "Normal ops"},
    "2026-01-20": {"balance": 184000, "note": "LOW POINT - MLK holiday"},
    "2026-01-21": {"balance": 195000, "note": "Recovery begins"},
    "2026-01-22": {"balance": 210000, "note": "Deposits flowing"},
    "2026-01-23": {"balance": 225000, "note": "Continued recovery"},
    "2026-01-24": {"balance": 240000, "note": "Strong deposits"},
    "2026-01-27": {"balance": 260000, "note": "Week start"},
    "2026-01-28": {"balance": 275000, "note": "Building toward month end"},
    "2026-01-29": {"balance": 290000, "note": "Pre-AmEx peak"},
    "2026-01-30": {"balance": 224000, "note": "After $130K AmEx payment"},
    "2026-01-31": {"balance": 230000, "note": "January close"},
    "2026-02-02": {"balance": 220000, "note": "ADP Tax + 401K + Fees"},
    "2026-02-03": {"balance": 160000, "note": "Payroll checks"},
    "2026-02-04": {"balance": 175000, "note": "Recovery"},
    "2026-02-05": {"balance": 190000, "note": "Recovery continues"},
    "2026-02-06": {"balance": 205000, "note": "Week buildup"},
    "2026-02-09": {"balance": 235000, "note": "Week buildup"},
    "2026-02-10": {"balance": 265000, "note": "Strong week"},
    "2026-02-11": {"balance": 300000, "note": "Approaching peak"},
    "2026-02-12": {"balance": 369000, "note": "PEAK - Best for distribution"},
    "2026-02-13": {"balance": 269000, "note": "After $100K AmEx payment"},
    "2026-02-16": {"balance": 260000, "note": "ADP Tax + 401K + Fees"},
    "2026-02-17": {"balance": 200000, "note": "Payroll checks"},
    "2026-02-18": {"balance": 215000, "note": "Recovery"},
    "2026-02-19": {"balance": 230000, "note": "Recovery continues"},
    "2026-02-20": {"balance": 245000, "note": "Week buildup"},
    "2026-02-24": {"balance": 341000, "note": "End of forecast period"}
}

def get_forecast_from_db():
    """Get forecast data from database"""
    conn = get_db()
    if not conn:
        return DEFAULT_FORECAST
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT date, balance, note FROM forecast ORDER BY date")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    if not rows:
        return DEFAULT_FORECAST
    
    forecast = {}
    for row in rows:
        date_str = row['date'].strftime("%Y-%m-%d")
        forecast[date_str] = {
            "balance": float(row['balance']),
            "note": row['note'] or ""
        }
    return forecast

def get_today_balance():
    """Get balance from forecast - transaction balances are for historical reference only"""
    today = today_pacific()
    
    # Use forecast for current balance display
    forecast = get_forecast_from_db()
    today_str = today.strftime("%Y-%m-%d")
    if today_str in forecast:
        return forecast[today_str]["balance"]
    sorted_dates = sorted(forecast.keys())
    for d in sorted_dates:
        if d >= today_str:
            return forecast[d]["balance"]
    return forecast[sorted_dates[-1]]["balance"] if sorted_dates else 237000

class DataSubmission(BaseModel):
    data: str

def parse_bank_data(raw_data: str) -> list:
    """Parse bank transaction data from various formats including messy web-copied data.
    
    Handles:
    - Tab-separated format: Description\tDebit\tCredit\tBalance (with date headers)
    - Web-copied format with date headers like "JAN 13, 2026 (31)"
    - Multi-line descriptions followed by amount on separate line
    
    Date headers set the date for all following transactions until the next header.
    """
    lines = raw_data.strip().split('\n')
    transactions = []
    current_date = now_pacific()  # Default to today if no date header found
    
    # Patterns
    date_pattern = re.compile(r'(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+(\d{1,2}),?\s+(\d{4})', re.IGNORECASE)
    amount_pattern = re.compile(r'^-?[\d,]+\.\d{2}$')
    months = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
             'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}
    
    # First check if this is tab-separated format WITH dates in first column
    tab_with_dates = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        parts = line.split('\t')
        if len(parts) >= 5:
            date_str = parts[0].strip()
            # Try to parse as date
            for fmt in ['%m/%d/%Y', '%Y-%m-%d', '%m/%d/%y', '%m-%d-%Y']:
                try:
                    date = datetime.strptime(date_str, fmt)
                    desc = parts[1].strip()
                    debit = parts[2].strip().replace(',', '').replace('$', '')
                    credit = parts[3].strip().replace(',', '').replace('$', '')
                    balance = parts[4].strip().replace(',', '').replace('$', '')
                    tab_with_dates.append({
                        'date': date,
                        'description': desc,
                        'debit': float(debit) if debit else 0,
                        'credit': float(credit) if credit else 0,
                        'balance': float(balance) if balance else 0
                    })
                    break
                except:
                    continue
    
    if tab_with_dates:
        return tab_with_dates
    
    # Check for tab-separated format WITHOUT dates (Description\tDebit\t\tCredit\tBalance)
    # This format has date headers on separate lines
    tab_transactions = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        # Check for date header first
        date_match = date_pattern.search(line)
        if date_match:
            month_str = date_match.group(1).upper()
            day = int(date_match.group(2))
            year = int(date_match.group(3))
            current_date = datetime(year, months[month_str], day)
            continue
        
        # Try tab-separated: Description\tDebit\t\tCredit\tBalance or Description\tDebit\tCredit\tBalance
        parts = line.split('\t')
        if len(parts) >= 3:
            # Find description (first non-empty part)
            desc = parts[0].strip()
            if not desc:
                continue
            
            # Look for numeric values in remaining parts
            amounts = []
            for p in parts[1:]:
                p = p.strip().replace(',', '').replace('$', '')
                if p and re.match(r'^-?[\d]+\.?\d*$', p):
                    try:
                        amounts.append(float(p))
                    except:
                        pass
            
            if amounts:
                # Format: Desc, Debit (empty or value), Credit (empty or value), Balance
                # If 3+ amounts: likely [debit, credit, balance] or variations
                # If 2 amounts: could be [debit, balance] or [credit, balance]
                # If 1 amount: could be debit or credit based on sign/description
                
                debit = 0
                credit = 0
                balance = 0
                
                if len(amounts) >= 3:
                    # Assume: debit, credit, balance (some may be 0)
                    debit = amounts[0] if amounts[0] > 0 else 0
                    credit = amounts[1] if amounts[1] > 0 else 0
                    balance = amounts[2]
                elif len(amounts) == 2:
                    # Could be [amount, balance]
                    # Determine debit vs credit from sign or keywords
                    amt = amounts[0]
                    balance = amounts[1]
                    if amt < 0 or 'CHECK' in desc.upper() or 'DEBIT' in desc.upper():
                        debit = abs(amt)
                    else:
                        credit = amt
                elif len(amounts) == 1:
                    amt = amounts[0]
                    if amt < 0 or 'CHECK' in desc.upper() or 'DEBIT' in desc.upper():
                        debit = abs(amt)
                    else:
                        credit = amt
                
                if debit > 0 or credit > 0:
                    tab_transactions.append({
                        'date': current_date,
                        'description': desc,
                        'debit': debit,
                        'credit': credit,
                        'balance': balance
                    })
    
    if tab_transactions:
        return tab_transactions
    
    # Parse web-copied format with multi-line descriptions followed by amount
    # Format:
    # JAN 21, 2026 (31)
    # E-DEPOSIT 
    # 1
    # 16,826.00
    # CHECK 
    # 55866
    # -182.76
    
    desc_lines = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Skip "Pending" lines and header text
        if line.lower() in ['pending', 'by descriptionby serial number', 'by description', 'by serial number']:
            continue
        
        # Check for date header (e.g., "JAN 21, 2026 (31)")
        date_match = date_pattern.search(line)
        if date_match:
            month_str = date_match.group(1).upper()
            day = int(date_match.group(2))
            year = int(date_match.group(3))
            current_date = datetime(year, months[month_str], day)
            desc_lines = []  # Reset description
            continue
        
        # Check if this is an amount (e.g., "1,333.00" or "-325.00")
        clean_line = line.replace(',', '').replace('$', '')
        if amount_pattern.match(clean_line):
            amount = float(clean_line)
            
            # Create transaction if we have date and description
            if current_date and desc_lines:
                description = ' '.join(desc_lines)
                transactions.append({
                    'date': current_date,
                    'description': description,
                    'debit': abs(amount) if amount < 0 else 0,
                    'credit': amount if amount > 0 else 0,
                    'balance': 0  # No balance in web format
                })
            desc_lines = []  # Reset for next transaction
            continue
        
        # This is a description line - accumulate it
        desc_lines.append(line)
    
    # If still no transactions, try simple format: "Description Amount" or "Description -Amount"
    if not transactions:
        simple_pattern = re.compile(r'^(.+?)\s+(-?\$?[\d,]+\.?\d*)$')
        for line in lines:
            line = line.strip()
            if not line:
                continue
            match = simple_pattern.match(line)
            if match:
                desc = match.group(1).strip()
                amount_str = match.group(2).replace(',', '').replace('$', '')
                try:
                    amount = float(amount_str)
                    transactions.append({
                        'date': current_date,
                        'description': desc,
                        'debit': abs(amount) if amount < 0 else 0,
                        'credit': amount if amount > 0 else 0,
                        'balance': 0
                    })
                except:
                    continue
    
    return transactions


def get_existing_transactions(conn, date: datetime.date) -> set:
    """Get existing transaction signatures for deduplication"""
    cur = conn.cursor()
    cur.execute("""
        SELECT date, description, debit, credit, balance 
        FROM bank_transactions 
        WHERE date >= %s
    """, (date - timedelta(days=7),))  # Check last 7 days for duplicates
    
    existing = set()
    for row in cur.fetchall():
        # Create signature from date + description + amounts
        sig = f"{row[0]}|{row[1][:30] if row[1] else ''}|{row[2]}|{row[3]}|{row[4]}"
        existing.add(sig)
    cur.close()
    return existing

def categorize_transaction(desc: str, debit: float, credit: float) -> str:
    """Categorize a transaction based on description and type"""
    desc_upper = desc.upper() if desc else ""
    
    if credit > 0:
        # Income categories
        if 'PAYMENTECH' in desc_upper and 'DEPOSIT' in desc_upper:
            return 'CC Deposits (Visa/MC)'
        elif 'AMERICAN EXPRESS' in desc_upper and 'SETTLEMENT' in desc_upper:
            return 'CC Deposits (AmEx)'
        elif 'CMS' in desc_upper and ('RELEASE' in desc_upper or 'DEPOSIT' in desc_upper):
            return 'CC Deposits (CMS)'
        elif 'E-DEPOSIT' in desc_upper:
            return 'Check Deposits'
        elif 'MVW' in desc_upper or 'WIRE' in desc_upper:
            return 'Wire Income'
        elif 'CHARGEBACK' in desc_upper:
            return 'Chargeback Reversal'
        else:
            return 'Other Income'
    else:
        # Expense categories
        if desc_upper.startswith('CHECK 5') or 'PAYROLL' in desc_upper:
            return 'Payroll Checks'
        elif desc_upper.startswith('CHECK '):
            check_num = desc_upper.replace('CHECK ', '').split()[0]
            if check_num.isdigit() and int(check_num) < 100000:
                return 'Refund Checks'
            return 'Other Checks'
        elif 'AMEX' in desc_upper and ('EPAYMENT' in desc_upper or 'ACH' in desc_upper):
            return 'AmEx Payment'
        elif 'ADP' in desc_upper:
            if 'TAX' in desc_upper:
                return 'ADP Tax/Fees'
            elif '401K' in desc_upper:
                return 'ADP 401K'
            return 'ADP Payroll'
        elif 'BLUE SHIELD' in desc_upper or 'HEALTH' in desc_upper:
            return 'Health Insurance'
        elif 'CHARGEBACK' in desc_upper:
            return 'Chargebacks'
        elif 'ACCT ANALYSIS' in desc_upper:
            return 'Bank Fees'
        else:
            return 'Other Expenses'

@app.post("/submit-data")
async def submit_data(submission: DataSubmission, code: str = Query(...)):
    verify_code(code)
    
    # Parse the data
    transactions = parse_bank_data(submission.data)
    
    if not transactions:
        # Fallback: send to webhook for manual processing
        async with httpx.AsyncClient() as client:
            await client.post(WEBHOOK_URL, json={
                "type": "data_submission",
                "data": submission.data[:5000],
                "timestamp": now_pacific().isoformat()
            })
        return {"status": "queued", "message": "Data sent for processing"}
    
    conn = get_db()
    if not conn:
        return {"status": "error", "message": "Database not available"}
    
    cur = conn.cursor()
    
    # Get existing transactions for deduplication (use date + desc + amounts, NOT balance)
    min_date = min(tx['date'] for tx in transactions).date()
    cur.execute("""
        SELECT date, description, debit, credit 
        FROM bank_transactions 
        WHERE date >= %s
    """, (min_date - timedelta(days=7),))
    
    existing = set()
    for row in cur.fetchall():
        # Signature without balance - just date + description + amounts
        sig = f"{row[0]}|{row[1][:50] if row[1] else ''}|{float(row[2]):.2f}|{float(row[3]):.2f}"
        existing.add(sig)
    
    # Sort transactions by date (oldest first)
    transactions.sort(key=lambda x: x['date'])
    
    added_count = 0
    skipped_count = 0
    added_transactions = []  # Track what we added for categorization
    latest_balance = None  # Track the latest balance from bank data
    
    for tx in transactions:
        tx_date = tx['date'].date() if hasattr(tx['date'], 'date') else tx['date']
        
        # Check for duplicate (without balance in signature)
        sig = f"{tx_date}|{tx['description'][:50] if tx['description'] else ''}|{float(tx['debit']):.2f}|{float(tx['credit']):.2f}"
        if sig in existing:
            skipped_count += 1
            continue
        
        # Use the balance from bank data if available
        tx_balance = tx.get('balance', 0)
        if tx_balance and tx_balance > 0:
            latest_balance = tx_balance
        
        # Insert transaction with bank's balance (or 0 if not available)
        cur.execute("""
            INSERT INTO bank_transactions (date, description, debit, credit, balance)
            VALUES (%s, %s, %s, %s, %s)
        """, (tx_date, tx['description'], tx['debit'], tx['credit'], tx_balance))
        
        existing.add(sig)  # Prevent duplicates within same submission
        added_count += 1
        added_transactions.append({
            'date': str(tx_date),
            'description': tx['description'],
            'debit': float(tx['debit']),
            'credit': float(tx['credit'])
        })
    
    conn.commit()
    cur.close()
    conn.close()
    
    # Categorize added transactions
    categories = {}
    for tx in added_transactions:
        cat = categorize_transaction(tx['description'], tx['debit'], tx['credit'])
        if cat not in categories:
            categories[cat] = {'count': 0, 'total': 0}
        categories[cat]['count'] += 1
        if tx['credit'] > 0:
            categories[cat]['total'] += tx['credit']
        else:
            categories[cat]['total'] -= tx['debit']
    
    # Format categories for display
    category_summary = []
    for cat, data in sorted(categories.items(), key=lambda x: abs(x[1]['total']), reverse=True):
        category_summary.append({
            'name': cat,
            'count': data['count'],
            'total': data['total'],
            'type': 'credit' if data['total'] > 0 else 'debit'
        })
    
    # Update forecast balance to latest bank balance and rebuild projections
    # Use the latest balance from bank data, or get current forecast balance if none
    if latest_balance is None or latest_balance <= 0:
        latest_balance = get_today_balance()
    
    if added_count > 0:
        rebuild_forecast(latest_balance)
    
    return {
        "status": "success",
        "message": f"Added {added_count} new transactions" + (f", skipped {skipped_count} duplicates" if skipped_count else ""),
        "added": added_count,
        "skipped": skipped_count,
        "latest_balance": latest_balance,
        "categories": category_summary
    }


@app.post("/cleanup-transactions")
async def cleanup_transactions(code: str = Query(...)):
    """Remove duplicate transactions, keeping the one with the highest balance"""
    verify_code(code)
    
    conn = get_db()
    if not conn:
        return {"status": "error", "message": "Database not available"}
    
    cur = conn.cursor()
    
    # Find and remove duplicates (keep the one with highest balance)
    cur.execute("""
        DELETE FROM bank_transactions
        WHERE id NOT IN (
            SELECT MAX(id) FROM bank_transactions
            GROUP BY date, SUBSTRING(description, 1, 50), debit, credit
        )
    """)
    deleted_dups = cur.rowcount
    
    # Delete test entries
    cur.execute("DELETE FROM bank_transactions WHERE description LIKE '%TEST%'")
    deleted_test = cur.rowcount
    
    conn.commit()
    cur.close()
    conn.close()
    
    return {
        "status": "success",
        "deleted_duplicates": deleted_dups,
        "deleted_test": deleted_test,
        "message": f"Cleaned up {deleted_dups} duplicates and {deleted_test} test entries"
    }


@app.post("/recalculate-balances")
async def recalculate_balances(code: str = Query(...)):
    """Recalculate running balances for all transactions with zero balance"""
    verify_code(code)
    
    conn = get_db()
    if not conn:
        return {"status": "error", "message": "Database not available"}
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Get the last transaction with a valid balance
    cur.execute("""
        SELECT id, date, balance FROM bank_transactions 
        WHERE balance > 0 
        ORDER BY date DESC, id DESC 
        LIMIT 1
    """)
    last_good = cur.fetchone()
    
    if not last_good:
        # Start from forecast if no good balances
        running_balance = get_today_balance()
        start_date = today_pacific() - timedelta(days=30)
    else:
        running_balance = float(last_good['balance'])
        start_date = last_good['date']
    
    # Get all transactions from that date forward, ordered by date and id
    cur.execute("""
        SELECT id, date, debit, credit, balance FROM bank_transactions 
        WHERE date >= %s 
        ORDER BY date ASC, id ASC
    """, (start_date,))
    
    rows = cur.fetchall()
    updated_count = 0
    
    for row in rows:
        if row['balance'] and row['balance'] > 0:
            # Use existing balance as new starting point
            running_balance = float(row['balance'])
        else:
            # Calculate new balance
            running_balance = running_balance + float(row['credit']) - float(row['debit'])
            # Update the row
            cur.execute("""
                UPDATE bank_transactions SET balance = %s WHERE id = %s
            """, (running_balance, row['id']))
            updated_count += 1
    
    conn.commit()
    cur.close()
    conn.close()
    
    return {
        "status": "success",
        "updated": updated_count,
        "latest_balance": running_balance,
        "message": f"Recalculated balances for {updated_count} transactions"
    }


@app.post("/set-balance")
async def set_balance(
    code: str = Query(...),
    balance: float = Query(..., description="Current balance to set"),
    as_of_date: Optional[str] = Query(default=None, description="Date for the balance (defaults to most recent transaction date)")
):
    """Set current balance and recalculate all prior transaction balances backwards"""
    verify_code(code)
    
    conn = get_db()
    if not conn:
        return {"status": "error", "message": "Database not available"}
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Determine the anchor date
    if as_of_date:
        anchor_date = datetime.strptime(as_of_date, "%Y-%m-%d").date()
    else:
        # Use the most recent transaction date
        cur.execute("SELECT MAX(date) as max_date FROM bank_transactions")
        row = cur.fetchone()
        anchor_date = row['max_date'] if row and row['max_date'] else today_pacific()
    
    # Get all transactions ordered by date DESC, id DESC (newest first)
    cur.execute("""
        SELECT id, date, debit, credit, balance 
        FROM bank_transactions 
        ORDER BY date DESC, id DESC
    """)
    rows = cur.fetchall()
    
    if not rows:
        cur.close()
        conn.close()
        return {"status": "error", "message": "No transactions found"}
    
    # Start with the provided balance
    running_balance = balance
    updated_count = 0
    anchor_found = False
    
    for row in rows:
        # Find the first transaction on or before anchor date to set as anchor
        if not anchor_found and row['date'] <= anchor_date:
            anchor_found = True
        
        if anchor_found:
            # Update this transaction's balance
            cur.execute("""
                UPDATE bank_transactions SET balance = %s WHERE id = %s
            """, (running_balance, row['id']))
            updated_count += 1
            
            # Work backwards: previous balance = current balance + debit - credit
            running_balance = running_balance + float(row['debit']) - float(row['credit'])
    
    conn.commit()
    cur.close()
    conn.close()
    
    # Also update the forecast starting balance
    update_forecast_balance(balance)
    
    return {
        "status": "success",
        "updated": updated_count,
        "current_balance": balance,
        "earliest_balance": running_balance,
        "anchor_date": anchor_date.strftime("%Y-%m-%d"),
        "message": f"Set balance to ${balance:,.2f} and recalculated {updated_count} prior transactions"
    }


@app.post("/delete-transactions")
async def delete_transactions(date_from: str = None, date_to: str = None, code: str = None):
    """Delete transactions in a date range"""
    if code != ACCESS_CODE:
        return {"status": "error", "message": "Invalid access code"}
    
    conn = get_db()
    if not conn:
        return {"status": "error", "message": "Database unavailable"}
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    if date_from and date_to:
        cur.execute("DELETE FROM bank_transactions WHERE date >= %s AND date <= %s", (date_from, date_to))
    elif date_from:
        cur.execute("DELETE FROM bank_transactions WHERE date >= %s", (date_from,))
    elif date_to:
        cur.execute("DELETE FROM bank_transactions WHERE date <= %s", (date_to,))
    else:
        return {"status": "error", "message": "Provide date_from and/or date_to"}
    
    deleted = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    
    return {"status": "success", "deleted": deleted, "message": f"Deleted {deleted} transactions"}


class DeleteByIdsRequest(BaseModel):
    ids: list[int]

@app.post("/delete-transactions-by-ids")
async def delete_transactions_by_ids(request: DeleteByIdsRequest, code: str = None):
    """Delete transactions by their IDs"""
    if code != ACCESS_CODE:
        return {"status": "error", "message": "Invalid access code"}
    
    if not request.ids:
        return {"status": "error", "message": "No IDs provided"}
    
    conn = get_db()
    if not conn:
        return {"status": "error", "message": "Database unavailable"}
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Delete by IDs
    placeholders = ','.join(['%s'] * len(request.ids))
    cur.execute(f"DELETE FROM bank_transactions WHERE id IN ({placeholders})", request.ids)
    deleted = cur.rowcount
    
    conn.commit()
    cur.close()
    conn.close()
    
    return {"status": "success", "deleted": deleted, "message": f"Deleted {deleted} transactions"}


class TransactionMatch(BaseModel):
    date: str
    description: str
    debit: float
    credit: float

class DeleteByMatchRequest(BaseModel):
    transactions: list[TransactionMatch]

@app.post("/delete-transactions-by-match")
async def delete_transactions_by_match(request: DeleteByMatchRequest, code: str = None):
    """Delete transactions by matching date, description prefix, and amounts"""
    if code != ACCESS_CODE:
        return {"status": "error", "message": "Invalid access code"}
    
    conn = get_db()
    if not conn:
        return {"status": "error", "message": "Database unavailable"}
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    deleted = 0
    
    for tx in request.transactions:
        # Match by date, description prefix (first 30 chars), and amounts
        cur.execute("""
            DELETE FROM bank_transactions 
            WHERE date = %s 
            AND LEFT(description, 30) = %s
            AND debit = %s 
            AND credit = %s
            LIMIT 1
        """, (tx.date, tx.description[:30], tx.debit, tx.credit))
        deleted += cur.rowcount
    
    conn.commit()
    cur.close()
    conn.close()
    
    return {"status": "success", "deleted": deleted, "message": f"Deleted {deleted} transactions"}


def update_forecast_balance(new_balance: float):
    """Rebuild forecast from today's balance forward using actual projections"""
    rebuild_forecast(new_balance)

def rebuild_forecast(starting_balance: float, days_ahead: int = 90):
    """Rebuild the entire forecast from today forward with proper calculations.
    
    Uses smart matching to compare scheduled transactions against real bank transactions
    from the last 15 days to identify which have already cleared.
    """
    conn = get_db()
    if not conn:
        return
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    today = today_pacific()
    today_str = today.strftime("%Y-%m-%d")
    
    # Daily rates (business days only)
    DAILY_CC = 15836      # CC processor deposits
    DAILY_EDEPOSIT = 14059  # E-deposits
    DAILY_WIRES = 1907    # Wire income
    DAILY_OPS = -9044     # Daily operations (refund checks)
    
    # Net daily income on business days (excluding special transactions)
    DAILY_NET = DAILY_CC + DAILY_EDEPOSIT + DAILY_WIRES + DAILY_OPS  # ~$22,758
    
    def is_business_day(d):
        """Check if date is a business day (weekday, not a bank holiday)"""
        if d.weekday() >= 5:  # Saturday=5, Sunday=6
            return False
        # Bank holidays (add more as needed)
        holidays = [
            DateType(2026, 1, 1),   # New Year's
            DateType(2026, 1, 19),  # MLK Day
            # Presidents Day (Feb 16) - USER WORKS, not treated as bank holiday
            DateType(2026, 5, 25),  # Memorial Day
            DateType(2026, 7, 3),   # July 4th observed
            DateType(2026, 9, 7),   # Labor Day
            DateType(2026, 11, 26), # Thanksgiving
            DateType(2026, 12, 25), # Christmas
        ]
        return d not in holidays
    
    # Get pending special transactions (those that haven't cleared yet)
    pending_special = get_pending_special_transactions(days_lookback=15)
    
    # Build forecast day by day
    balance = starting_balance
    forecast_data = []
    
    low_point = {"date": today_str, "balance": balance}
    high_point = {"date": today_str, "balance": balance}
    
    for i in range(days_ahead + 1):
        current_date = today + timedelta(days=i)
        date_str = current_date.strftime("%Y-%m-%d")
        
        if i == 0:
            # Today - use the starting balance directly (already reflects today's activity)
            note = "Current balance"
        else:
            # Add daily income on business days
            if is_business_day(current_date):
                balance += DAILY_NET
            
            # Add only PENDING special transactions for this date
            if date_str in pending_special:
                for txn in pending_special[date_str]:
                    balance += txn["amount"]
            
            note = get_note_for_date(date_str, balance, pending_special)
        
        forecast_data.append({
            "date": date_str,
            "balance": round(balance, 2),
            "note": note
        })
        
        # Track low and high points
        if balance < low_point["balance"]:
            low_point = {"date": date_str, "balance": balance}
        if balance > high_point["balance"]:
            high_point = {"date": date_str, "balance": balance}
    
    # Clear existing forecast from today forward and insert new data
    cur.execute("DELETE FROM forecast WHERE date >= %s", (today_str,))
    
    for entry in forecast_data:
        cur.execute("""
            INSERT INTO forecast (date, balance, note) VALUES (%s, %s, %s)
            ON CONFLICT (date) DO UPDATE SET balance = %s, note = %s
        """, (entry["date"], entry["balance"], entry["note"], entry["balance"], entry["note"]))
    
    conn.commit()
    cur.close()
    conn.close()
    
    return {"low_point": low_point, "high_point": high_point, "days_projected": len(forecast_data)}

def get_note_for_date(date_str: str, balance: float, pending_special: dict = None) -> str:
    """Generate appropriate note for a forecast date"""
    # Use pending_special if provided, otherwise fall back to SPECIAL_TRANSACTIONS
    txn_source = pending_special if pending_special is not None else SPECIAL_TRANSACTIONS
    
    if date_str in txn_source:
        txns = txn_source[date_str]
        descriptions = [t["desc"] for t in txns if t["amount"] < -10000]
        if descriptions:
            return ", ".join(descriptions[:2])
    
    # Check for notable balance levels
    if balance < 100000:
        return "LOW - Watch closely"
    elif balance < 150000:
        return "Tight cash position"
    elif balance > 400000:
        return "Peak - Good for distribution"
    elif balance > 300000:
        return "Strong position"
    
    return "Normal operations"


@app.get("/transactions")
async def get_transactions(
    code: str = Query(...),
    limit: int = Query(default=10, le=100),
    offset: int = Query(default=0),
    date_from: Optional[str] = Query(default=None),
    date_to: Optional[str] = Query(default=None),
    type: Optional[str] = Query(default=None),  # 'credit' or 'debit'
    amount_min: Optional[float] = Query(default=None),
    amount_max: Optional[float] = Query(default=None),
    description: Optional[str] = Query(default=None)
):
    """Get bank transactions with pagination and search filters"""
    verify_code(code)
    
    conn = get_db()
    if not conn:
        return {"transactions": [], "total": 0, "message": "Database not available"}
    
    # Build WHERE clause based on filters
    conditions = []
    params = []
    
    if date_from:
        conditions.append("date >= %s")
        params.append(date_from)
    if date_to:
        conditions.append("date <= %s")
        params.append(date_to)
    if type == 'credit':
        conditions.append("(credit > 0 OR credit IS NOT NULL AND credit > 0)")
    elif type == 'debit':
        conditions.append("(debit > 0 OR debit IS NOT NULL AND debit > 0)")
    if amount_min is not None:
        conditions.append("(COALESCE(debit, 0) >= %s OR COALESCE(credit, 0) >= %s)")
        params.extend([amount_min, amount_min])
    if amount_max is not None:
        conditions.append("(COALESCE(debit, 0) <= %s OR COALESCE(credit, 0) <= %s OR (debit IS NULL AND credit IS NULL))")
        params.extend([amount_max, amount_max])
    if description:
        conditions.append("UPPER(description) LIKE UPPER(%s)")
        params.append(f"%{description}%")
    
    where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Get total count
    count_query = f"SELECT COUNT(*) as cnt FROM bank_transactions {where_clause}"
    cur.execute(count_query, params)
    total = cur.fetchone()['cnt']
    
    # Get paginated results
    query = f"""
        SELECT id, date, description, debit, credit, balance, created_at
        FROM bank_transactions
        {where_clause}
        ORDER BY date DESC, id DESC
        LIMIT %s OFFSET %s
    """
    cur.execute(query, params + [limit, offset])
    
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    transactions = []
    for row in rows:
        transactions.append({
            "id": row['id'],
            "date": row['date'].strftime("%Y-%m-%d") if row['date'] else None,
            "description": row['description'],
            "debit": float(row['debit']) if row['debit'] else 0,
            "credit": float(row['credit']) if row['credit'] else 0,
            "balance": float(row['balance']) if row['balance'] else 0
        })
    
    return {
        "transactions": transactions,
        "total": total,
        "limit": limit,
        "offset": offset,
        "hasMore": offset + limit < total
    }

# Projection generators
# Special transactions calendar
SPECIAL_TRANSACTIONS = {
    # Expected extra income - late January (MVW wire already hit Jan 20 @ $20,141)
    "2026-01-28": [
        {"type": "income", "amount": 30000, "desc": "Expected Extra Income"},
    ],
    
    # Jan 21 - Remaining items from Jan 20 that haven't cleared yet + outstanding CC deposits
    "2026-01-21": [
        {"type": "income", "amount": 12000, "desc": "CC Deposits (Jan 19 settlements)"},
        {"type": "amex", "amount": -26763, "desc": "AmEx Payment (Remaining)"},
        {"type": "payroll", "amount": -58500, "desc": "Payroll Checks (Remaining)"},
        {"type": "other", "amount": -38000, "desc": "TM Travel"},
        {"type": "comms_execs", "amount": -14000, "desc": "Comms & Execs (Outstanding)"},
    ],
    "2026-01-31": [{"type": "amex", "amount": -130000, "desc": "AmEx Payment"}],
    # Feb 2: Comms BOM + Blue Shield + ADP (Feb 1 is Sunday)
    "2026-02-02": [
        {"type": "comms_execs", "amount": -51000, "desc": "Comms & Execs"},
        {"type": "blue_shield", "amount": -15000, "desc": "Blue Shield"},
        {"type": "payroll_tax", "amount": -25430, "desc": "ADP Tax + 401K + Fees"},
        {"type": "income", "amount": 15000, "desc": "BOM Spike (Reduced)"},
        {"type": "income", "amount": 30000, "desc": "Expected Extra Income"},
    ],
    "2026-02-03": [{"type": "payroll", "amount": -60000, "desc": "Payroll Checks"}],
    # Feb 16: Comms MID + ADP (Feb 15 is Sunday, Feb 16 is Presidents Day but not a bank holiday for checks)
    "2026-02-16": [
        {"type": "comms_execs", "amount": -46000, "desc": "Comms & Execs"},
        {"type": "payroll_tax", "amount": -25430, "desc": "ADP Tax + 401K + Fees"},
    ],
    # Feb 17: Payroll + AmEx
    "2026-02-17": [
        {"type": "payroll", "amount": -60000, "desc": "Payroll Checks"},
        {"type": "amex", "amount": -130000, "desc": "AmEx Payment"},
    ],
    # Feb end-of-month AmEx (Feb 28 is Saturday)
    "2026-02-28": [{"type": "amex", "amount": -130000, "desc": "AmEx Payment"}],
    # March BOM (Mar 1 is Sunday)
    "2026-03-02": [
        {"type": "comms_execs", "amount": -51000, "desc": "Comms & Execs"},
        {"type": "blue_shield", "amount": -15000, "desc": "Blue Shield"},
        {"type": "payroll_tax", "amount": -25430, "desc": "ADP Tax + 401K + Fees"},
        {"type": "income", "amount": 60000, "desc": "BOM Spike"},
    ],
    "2026-03-03": [{"type": "payroll", "amount": -60000, "desc": "Payroll Checks"}],
    "2026-03-05": [{"type": "income", "amount": 60000, "desc": "Client Revenue Payment"}],
    "2026-03-16": [
        {"type": "comms_execs", "amount": -46000, "desc": "Comms & Execs"},
        {"type": "payroll_tax", "amount": -25430, "desc": "ADP Tax + 401K + Fees"},
    ],
    "2026-03-17": [
        {"type": "payroll", "amount": -60000, "desc": "Payroll Checks"},
        {"type": "amex", "amount": -130000, "desc": "AmEx Payment"},
    ],
    # March end-of-month AmEx
    "2026-03-31": [{"type": "amex", "amount": -130000, "desc": "AmEx Payment"}],
    
    # April 2026
    "2026-04-01": [
        {"type": "comms", "amount": -51000, "desc": "Comms & Execs (BOM)"},
        {"type": "insurance", "amount": -15000, "desc": "Blue Shield"},
        {"type": "income", "amount": 60000, "desc": "BOM Spike"},
    ],
    "2026-04-02": [{"type": "payroll", "amount": -25430, "desc": "ADP Tax/401K/Fees"}],
    "2026-04-03": [{"type": "payroll", "amount": -60000, "desc": "Payroll Checks"}],
    "2026-04-15": [
        {"type": "comms", "amount": -46000, "desc": "Comms & Execs (Mid)"},
        {"type": "amex", "amount": -130000, "desc": "AmEx Payment"},
    ],
    "2026-04-16": [{"type": "payroll", "amount": -25430, "desc": "ADP Tax/401K/Fees"}],
    "2026-04-17": [{"type": "payroll", "amount": -60000, "desc": "Payroll Checks"}],
    "2026-04-30": [{"type": "amex", "amount": -130000, "desc": "AmEx Payment"}],
    
    # May 2026
    "2026-05-01": [
        {"type": "comms", "amount": -51000, "desc": "Comms & Execs (BOM)"},
        {"type": "insurance", "amount": -15000, "desc": "Blue Shield"},
        {"type": "income", "amount": 60000, "desc": "BOM Spike"},
    ],
    "2026-05-04": [{"type": "payroll", "amount": -25430, "desc": "ADP Tax/401K/Fees"}],
    "2026-05-05": [{"type": "payroll", "amount": -60000, "desc": "Payroll Checks"}],
    "2026-05-15": [
        {"type": "comms", "amount": -46000, "desc": "Comms & Execs (Mid)"},
        {"type": "amex", "amount": -130000, "desc": "AmEx Payment"},
    ],
    "2026-05-18": [{"type": "payroll", "amount": -25430, "desc": "ADP Tax/401K/Fees"}],
    "2026-05-19": [{"type": "payroll", "amount": -60000, "desc": "Payroll Checks"}],
    "2026-05-31": [{"type": "amex", "amount": -130000, "desc": "AmEx Payment"}],
    
    # June 2026
    "2026-06-01": [
        {"type": "comms", "amount": -51000, "desc": "Comms & Execs (BOM)"},
        {"type": "insurance", "amount": -15000, "desc": "Blue Shield"},
        {"type": "income", "amount": 60000, "desc": "BOM Spike"},
    ],
    "2026-06-02": [{"type": "payroll", "amount": -25430, "desc": "ADP Tax/401K/Fees"}],
    "2026-06-03": [{"type": "payroll", "amount": -60000, "desc": "Payroll Checks"}],
    "2026-06-15": [
        {"type": "comms", "amount": -46000, "desc": "Comms & Execs (Mid)"},
        {"type": "amex", "amount": -130000, "desc": "AmEx Payment"},
    ],
    "2026-06-16": [{"type": "payroll", "amount": -25430, "desc": "ADP Tax/401K/Fees"}],
    "2026-06-17": [{"type": "payroll", "amount": -60000, "desc": "Payroll Checks"}],
    "2026-06-30": [{"type": "amex", "amount": -130000, "desc": "AmEx Payment"}],
    
    # July 2026
    "2026-07-01": [
        {"type": "comms", "amount": -51000, "desc": "Comms & Execs (BOM)"},
        {"type": "insurance", "amount": -15000, "desc": "Blue Shield"},
        {"type": "income", "amount": 60000, "desc": "BOM Spike"},
    ],
    "2026-07-02": [{"type": "payroll", "amount": -25430, "desc": "ADP Tax/401K/Fees"}],
    "2026-07-06": [{"type": "payroll", "amount": -60000, "desc": "Payroll Checks"}],  # After July 4th
    "2026-07-15": [
        {"type": "comms", "amount": -46000, "desc": "Comms & Execs (Mid)"},
        {"type": "amex", "amount": -130000, "desc": "AmEx Payment"},
    ],
    "2026-07-16": [{"type": "payroll", "amount": -25430, "desc": "ADP Tax/401K/Fees"}],
    "2026-07-17": [{"type": "payroll", "amount": -60000, "desc": "Payroll Checks"}],
    "2026-07-31": [{"type": "amex", "amount": -130000, "desc": "AmEx Payment"}],
}

# Daily averages for credits
DAILY_AUTHNET = 15836  # CC processor deposits - weighted avg (80% last 2 weeks, 20% 90-day)
DAILY_CHECK_DEPOSITS = 14059  # E-Deposits - weighted avg (50% last 2 weeks, 50% 60-day normalized)
DAILY_WIRE = 1907  # ~$9.5K/week from CFI and FRE - weighted avg (50% last 2 weeks, 50% 60-day normalized)
DAILY_OPS = 9044  # Daily ops - refund checks only (< $1,500, excludes 5-series payroll)
MONTHLY_RENT = 8500  # Approximate monthly rent
MONTHLY_RECURRING = 5000  # Insurance, utilities, etc.

# Comms & Execs checks (excluded from daily ops)
BOM_CHECKS = 51000  # 1st of month: $6K + $15K + $30K
MID_CHECKS = 46000  # 15th of month: $25K + $6K + $15K

def should_include_special_transaction(txn_type: str, amount: int, scheduled_date: str) -> bool:
    """Determine if a scheduled special transaction should be included in projections.
    
    Rules:
    1. If scheduled date is in the future: include it
    2. If scheduled date is today or in the past:
       - Check if it's been confirmed in bank data -> exclude if found
       - If more than 2 days past and not confirmed -> exclude (assume didn't happen)
       - If within 2 days and not confirmed -> include (still waiting for it)
    """
    from datetime import datetime, timedelta
    
    today = today_pacific()
    scheduled = datetime.strptime(scheduled_date, "%Y-%m-%d").date()
    days_since_scheduled = (today - scheduled).days
    
    # Future transactions: always include
    if days_since_scheduled < 0:
        return True
    
    # Check if confirmed in bank data
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        # Look for matching debit within 3 days of scheduled date
        # Match by approximate amount (within 10%)
        abs_amount = abs(amount)
        min_amount = abs_amount * 0.9
        max_amount = abs_amount * 1.1
        
        # Search pattern based on transaction type
        if txn_type == 'amex':
            search_pattern = '%AMEX%'
        elif txn_type in ('payroll', 'payroll_tax'):
            search_pattern = '%ADP%'  # ADP handles payroll
        elif txn_type == 'comms_execs':
            # These are individual checks, harder to match - check by amount range
            search_pattern = '%'
        else:
            search_pattern = '%'
        
        cur.execute("""
            SELECT id FROM bank_transactions 
            WHERE date >= %s::date - interval '1 day'
            AND date <= %s::date + interval '3 days'
            AND debit_amount >= %s AND debit_amount <= %s
            AND UPPER(description) LIKE %s
            LIMIT 1
        """, (scheduled_date, scheduled_date, min_amount, max_amount, search_pattern))
        
        result = cur.fetchone()
        cur.close()
        conn.close()
        
        # If found in bank data, exclude from projection
        if result is not None:
            return False
            
    except Exception as e:
        print(f"Error checking confirmed transaction: {e}")
    
    # If more than 2 days past and not confirmed, exclude it
    if days_since_scheduled > 2:
        return False
    
    # Within 2 days window and not yet confirmed - include it
    return True

BANK_HOLIDAYS_2026 = [
    "2026-01-19",  # MLK Day
    # Presidents Day removed - user works this day and expects normal deposits
    "2026-05-25",  # Memorial Day
    "2026-07-03",  # Independence Day observed
    "2026-09-07",  # Labor Day
    "2026-11-26",  # Thanksgiving
    "2026-12-25",  # Christmas
]

def get_daily_detail(date: datetime, forecast: dict, pending_special: dict = None) -> dict:
    """Get detailed breakdown for a single day.
    
    If pending_special is provided, uses that instead of SPECIAL_TRANSACTIONS
    to show only transactions that haven't cleared yet.
    """
    date_str = date.strftime("%Y-%m-%d")
    dow = date.weekday()
    is_weekend = dow >= 5
    is_bank_holiday = date_str in BANK_HOLIDAYS_2026
    
    # Get pending special transactions if not provided
    if pending_special is None:
        pending_special = get_pending_special_transactions(days_lookback=15)
    
    # Start with base structure
    detail = {
        "credits": {"authnet": 0, "checks": 0, "wires": 0, "total": 0},
        "debits": {"ops": 0, "total": 0},
        "special": [],
        "net": 0
    }
    
    # Normal credits/debits only on weekdays (excluding bank holidays)
    if not is_weekend and not is_bank_holiday:
        detail["credits"]["authnet"] = DAILY_AUTHNET
        detail["credits"]["checks"] = DAILY_CHECK_DEPOSITS
        detail["credits"]["wires"] = DAILY_WIRE if dow in [1, 3] else 0  # Tue/Thu
        detail["credits"]["total"] = detail["credits"]["authnet"] + detail["credits"]["checks"] + detail["credits"]["wires"]
        
        detail["debits"]["ops"] = DAILY_OPS
        detail["debits"]["total"] = DAILY_OPS
        
        # Note: Comms & Execs and Blue Shield are now tracked in special transactions
        # to properly handle weekend/holiday adjustments
    
    # Add special transactions (AmEx, payroll, etc.) on ANY day including weekends
    # Only show PENDING transactions (those that haven't cleared yet)
    if date_str in pending_special:
        for txn in pending_special[date_str]:
            if should_include_special_transaction(txn["type"], txn["amount"], date_str):
                detail["special"].append(txn)
    
    # Calculate net
    detail["net"] = detail["credits"]["total"] - detail["debits"]["total"]
    
    return detail

def generate_daily_projection(days: int) -> dict:
    """Generate daily projection using forecast balances from database.
    
    Uses smart matching to only show pending special transactions (those that
    haven't cleared yet based on real bank transaction history).
    """
    forecast = get_forecast_from_db()
    start_date = now_pacific().replace(hour=0, minute=0, second=0, microsecond=0)
    today_str = start_date.strftime("%Y-%m-%d")
    
    # Pre-fetch pending special transactions (avoids repeated DB calls)
    pending_special = get_pending_special_transactions(days_lookback=15)
    
    rows = []
    low_bal, low_date, low_note = float('inf'), None, ""
    high_bal, high_date, high_note = 0, None, ""
    
    for i in range(days):
        date = start_date + timedelta(days=i)
        date_str = date.strftime("%Y-%m-%d")
        
        # Use forecast balance directly (already includes special transactions)
        if date_str in forecast:
            balance = int(forecast[date_str]["balance"])
            note = forecast[date_str].get("note", "")
        else:
            # If date not in forecast, estimate from last known date
            sorted_dates = sorted([d for d in forecast.keys() if d <= date_str])
            if sorted_dates:
                last_date = sorted_dates[-1]
                balance = int(forecast[last_date]["balance"])
                note = ""
            else:
                balance = int(get_today_balance())
                note = ""
        
        # Get detail breakdown for display purposes (uses pending_special)
        detail = get_daily_detail(date, forecast, pending_special)
        
        if balance < low_bal:
            low_bal, low_date, low_note = balance, date.strftime("%Y-%m-%d"), note
        if balance > high_bal:
            high_bal, high_date, high_note = balance, date.strftime("%Y-%m-%d"), note
        
        rows.append({
            "date": date.strftime("%a %b %d"),
            "iso_date": date.strftime("%Y-%m-%d"),
            "balance": balance,
            "note": note,
            "credits": detail["credits"],
            "debits": detail["debits"],
            "special": detail["special"]
        })
    
    return {
        "type": "projection",
        "title": f"{days}-Day Projection",
        "period": "daily",
        "rows": rows,
        "low": {"value": low_bal, "label": low_date, "note": low_note},
        "high": {"value": high_bal, "label": high_date, "note": high_note}
    }

def generate_weekly_projection(weeks: int) -> dict:
    """Generate weekly projection with smart transaction matching.
    
    Uses pending special transactions (those that haven't cleared yet).
    """
    forecast = get_forecast_from_db()
    start_date = now_pacific().replace(hour=0, minute=0, second=0, microsecond=0)
    today_str = start_date.strftime("%Y-%m-%d")
    
    # Pre-fetch pending special transactions (avoids repeated DB calls)
    pending_special = get_pending_special_transactions(days_lookback=15)
    
    if today_str in forecast:
        start_balance = forecast[today_str]["balance"]
    else:
        start_balance = get_today_balance()
    
    rows = []
    balance = start_balance
    overall_low, overall_high = float('inf'), 0
    
    for i in range(weeks):
        week_start = start_date + timedelta(weeks=i)
        week_end = week_start + timedelta(days=6)
        
        # Aggregate week data
        week_credits = {"authnet": 0, "checks": 0, "wires": 0, "total": 0}
        week_debits = {"ops": 0, "total": 0}
        week_special = {"amex": 0, "payroll": 0, "payroll_tax": 0, "rent": 0, "recurring": 0, "distribution": 0}
        week_low, week_high = float('inf'), 0
        
        for j in range(7):
            d = week_start + timedelta(days=j)
            d_str = d.strftime("%Y-%m-%d")
            detail = get_daily_detail(d, forecast, pending_special)
            
            # Accumulate credits
            week_credits["authnet"] += detail["credits"]["authnet"]
            week_credits["checks"] += detail["credits"]["checks"]
            week_credits["wires"] += detail["credits"]["wires"]
            week_credits["total"] += detail["credits"]["total"]
            
            # Accumulate debits
            week_debits["ops"] += detail["debits"]["ops"]
            
            # Accumulate special
            for txn in detail["special"]:
                if txn["type"] in week_special:
                    week_special[txn["type"]] += abs(txn["amount"])
            
            # Track daily balance for range
            if d_str in forecast:
                day_bal = forecast[d_str]["balance"]
            else:
                balance = int(balance + detail["net"])
                day_bal = balance
            
            week_low = min(week_low, day_bal)
            week_high = max(week_high, day_bal)
        
        week_debits["total"] = week_debits["ops"] + sum(week_special.values())
        
        # Update overall tracking
        overall_low = min(overall_low, week_low)
        overall_high = max(overall_high, week_high)
        
        # Get end-of-week balance
        week_end_str = week_end.strftime("%Y-%m-%d")
        if week_end_str in forecast:
            balance = forecast[week_end_str]["balance"]
        
        rows.append({
            "date": f"Week of {week_start.strftime('%b %d')}",
            "balance": balance,
            "credits": week_credits,
            "debits": week_debits,
            "special": week_special,
            "range": {"low": week_low, "high": week_high}
        })
    
    return {
        "type": "projection",
        "title": f"{weeks}-Week Projection",
        "period": "weekly",
        "rows": rows,
        "low": {"value": overall_low, "label": "", "note": ""},
        "high": {"value": overall_high, "label": "", "note": ""}
    }

def generate_monthly_projection(months: int) -> dict:
    forecast = get_forecast_from_db()
    start_balance = get_today_balance()
    
    rows = []
    balance = start_balance
    overall_low, overall_high = float('inf'), 0
    
    current = now_pacific().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    for i in range(months):
        if i == 0:
            month_date = current
        else:
            month_date = (current + timedelta(days=32*i)).replace(day=1)
        
        # Get days in month
        if month_date.month == 12:
            next_month = month_date.replace(year=month_date.year+1, month=1)
        else:
            next_month = month_date.replace(month=month_date.month+1)
        days_in_month = (next_month - month_date).days
        
        # Aggregate month data
        month_credits = {"authnet": 0, "checks": 0, "wires": 0, "total": 0}
        month_debits = {"ops": 0, "total": 0}
        month_special = {"amex": 0, "payroll": 0, "payroll_tax": 0, "rent": 0, "recurring": 0, "distribution": 0}
        month_low, month_high = float('inf'), 0
        
        for j in range(days_in_month):
            d = month_date + timedelta(days=j)
            d_str = d.strftime("%Y-%m-%d")
            detail = get_daily_detail(d, forecast)
            
            # Accumulate credits
            month_credits["authnet"] += detail["credits"]["authnet"]
            month_credits["checks"] += detail["credits"]["checks"]
            month_credits["wires"] += detail["credits"]["wires"]
            month_credits["total"] += detail["credits"]["total"]
            
            # Accumulate debits
            month_debits["ops"] += detail["debits"]["ops"]
            
            # Accumulate special
            for txn in detail["special"]:
                if txn["type"] in month_special:
                    month_special[txn["type"]] += abs(txn["amount"])
            
            # Track daily balance for range
            if d_str in forecast:
                day_bal = forecast[d_str]["balance"]
            else:
                balance = int(balance + detail["net"])
                day_bal = balance
            
            month_low = min(month_low, day_bal)
            month_high = max(month_high, day_bal)
        
        # Add monthly recurring expenses
        month_special["rent"] = MONTHLY_RENT
        month_special["recurring"] = MONTHLY_RECURRING
        
        month_debits["total"] = month_debits["ops"] + sum(month_special.values())
        
        # Update overall tracking
        overall_low = min(overall_low, month_low)
        overall_high = max(overall_high, month_high)
        
        rows.append({
            "date": month_date.strftime("%B %Y"),
            "balance": balance,
            "credits": month_credits,
            "debits": month_debits,
            "special": month_special,
            "range": {"low": month_low, "high": month_high}
        })
    
    return {
        "type": "projection",
        "title": f"{months}-Month Projection",
        "period": "monthly",
        "rows": rows,
        "low": {"value": overall_low, "label": "", "note": ""},
        "high": {"value": overall_high, "label": "", "note": ""}
    }

@app.get("/summary")
async def get_summary(code: str = Query(...)):
    verify_code(code)
    
    # Use the projection to get accurate values (includes today's transactions)
    proj = generate_daily_projection(60)
    today = now_pacific().strftime("%Y-%m-%d")
    
    # Today's balance is end-of-day (first row in projection)
    current_balance = proj["rows"][0]["balance"] if proj["rows"] else get_today_balance()
    
    gross_profit = ROLLING_30_DAY['gross_profit']
    
    # Find important dates (local highs and lows) in next 60 days
    # Handle weekend plateaus by looking for trend changes
    rows = proj["rows"]
    important_dates = []
    
    # First pass: find all turning points (where trend changes direction)
    # Track last non-equal value to handle plateaus
    i = 1
    while i < len(rows) - 1:
        curr_bal = rows[i]["balance"]
        
        # Look backwards for a different value
        prev_bal = None
        for j in range(i-1, -1, -1):
            if rows[j]["balance"] != curr_bal:
                prev_bal = rows[j]["balance"]
                break
        
        # Look forwards for a different value
        next_bal = None
        for j in range(i+1, len(rows)):
            if rows[j]["balance"] != curr_bal:
                next_bal = rows[j]["balance"]
                break
        
        if prev_bal is not None and next_bal is not None:
            # Local minimum (lower than both non-equal neighbors)
            if curr_bal < prev_bal and curr_bal < next_bal:
                # Only add if not already a date with same balance
                if not any(d["balance"] == curr_bal and d["type"] == "LOW" for d in important_dates):
                    important_dates.append({
                        "date": rows[i]["date"],
                        "iso_date": rows[i]["iso_date"],
                        "balance": curr_bal,
                        "type": "LOW",
                        "note": rows[i].get("note", "")
                    })
            # Local maximum (higher than both non-equal neighbors)
            elif curr_bal > prev_bal and curr_bal > next_bal:
                if not any(d["balance"] == curr_bal and d["type"] == "HIGH" for d in important_dates):
                    important_dates.append({
                        "date": rows[i]["date"],
                        "iso_date": rows[i]["iso_date"],
                        "balance": curr_bal,
                        "type": "HIGH",
                        "note": rows[i].get("note", "")
                    })
        i += 1
    
    # Also include absolute min and max if not already there
    min_row = min(rows, key=lambda x: x["balance"])
    max_row = max(rows, key=lambda x: x["balance"])
    
    min_iso = min_row["iso_date"]
    max_iso = max_row["iso_date"]
    
    if not any(d["iso_date"] == min_iso for d in important_dates):
        important_dates.append({
            "date": min_row["date"],
            "iso_date": min_iso,
            "balance": min_row["balance"],
            "type": "LOW",
            "note": min_row.get("note", "")
        })
    if not any(d["iso_date"] == max_iso for d in important_dates):
        important_dates.append({
            "date": max_row["date"],
            "iso_date": max_iso,
            "balance": max_row["balance"],
            "type": "HIGH",
            "note": max_row.get("note", "")
        })
    
    # Sort by ISO date chronologically
    important_dates.sort(key=lambda x: x["iso_date"])
    
    # Post-process: ensure alternating HIGH/LOW pattern
    # If two LOWs are adjacent, find the highest point between them and insert it
    # If two HIGHs are adjacent, find the lowest point between them and insert it
    final_dates = []
    for i, d in enumerate(important_dates):
        if i > 0 and final_dates:
            prev = final_dates[-1]
            # Check if same type as previous
            if d["type"] == prev["type"]:
                # Find the opposite extreme between these two dates
                between_rows = [r for r in rows if prev["iso_date"] < r["iso_date"] < d["iso_date"]]
                if between_rows:
                    if d["type"] == "LOW":
                        # Need to insert a HIGH between two LOWs
                        max_between = max(between_rows, key=lambda x: x["balance"])
                        final_dates.append({
                            "date": max_between["date"],
                            "iso_date": max_between["iso_date"],
                            "balance": max_between["balance"],
                            "type": "HIGH",
                            "note": max_between.get("note", "")
                        })
                    else:
                        # Need to insert a LOW between two HIGHs
                        min_between = min(between_rows, key=lambda x: x["balance"])
                        final_dates.append({
                            "date": min_between["date"],
                            "iso_date": min_between["iso_date"],
                            "balance": min_between["balance"],
                            "type": "LOW",
                            "note": min_between.get("note", "")
                        })
        final_dates.append(d)
    
    # Take first 6 (chronological order preserved)
    important_dates = final_dates[:6]
    
    return {
        "current_balance": current_balance,
        "as_of": today,
        "important_dates": important_dates,
        "profit_30day": gross_profit
    }

@app.get("/forecast")
async def get_forecast(code: str = Query(...)):
    verify_code(code)
    forecast = get_forecast_from_db()
    return forecast

@app.get("/low-point")
async def get_low_point(code: str = Query(...)):
    verify_code(code)
    forecast = get_forecast_from_db()
    
    low_point = min(forecast.values(), key=lambda x: x["balance"])
    low_date = [k for k, v in forecast.items() if v["balance"] == low_point["balance"]][0]
    
    return {
        "date": low_date,
        "balance": low_point["balance"],
        "note": low_point.get("note", "")
    }

@app.get("/balance/{date}")
async def get_balance(date: str, code: str = Query(...)):
    verify_code(code)
    forecast = get_forecast_from_db()
    
    if date in forecast:
        return {"date": date, "balance": forecast[date]["balance"], "note": forecast[date].get("note", "")}
    raise HTTPException(status_code=404, detail=f"No forecast for {date}")

@app.get("/payments")
async def get_payments(code: str = Query(...)):
    """Get upcoming payments with balance impact"""
    verify_code(code)
    from datetime import date
    today = date.today()
    
    # Get all scheduled payments from SPECIAL_TRANSACTIONS
    payments = []
    for date_str, txns in SPECIAL_TRANSACTIONS.items():
        for txn in txns:
            if txn["type"] in ["amex", "payroll", "payroll_tax"]:
                payments.append({
                    "date": date_str,
                    "type": txn["type"],
                    "desc": txn["desc"],
                    "amount": abs(txn["amount"])
                })
    
    # Add Comms & Execs for upcoming months
    for month in range(1, 4):  # Jan, Feb, Mar
        year = 2026
        first_str = f"{year}-{month:02d}-01"
        fifteenth_str = f"{year}-{month:02d}-15"
        
        if first_str >= today.isoformat() and first_str not in [p["date"] for p in payments]:
            payments.append({"date": first_str, "type": "comms_execs", "desc": "Comms & Execs", "amount": BOM_CHECKS})
        if fifteenth_str >= today.isoformat() and fifteenth_str not in [p["date"] for p in payments]:
            payments.append({"date": fifteenth_str, "type": "comms_execs", "desc": "Comms & Execs", "amount": MID_CHECKS})
    
    # Filter to upcoming only and sort
    upcoming = [p for p in payments if p["date"] >= today.isoformat()]
    upcoming.sort(key=lambda x: x["date"])
    
    # Calculate days until and balance impact
    # Generate projection once for all payments
    proj = generate_daily_projection(90)
    
    # Build a lookup by ISO date
    balance_by_date = {}
    start_date = now_pacific().replace(hour=0, minute=0, second=0, microsecond=0)
    for i, row in enumerate(proj["rows"]):
        iso_date = (start_date + timedelta(days=i)).strftime("%Y-%m-%d")
        balance_by_date[iso_date] = row["balance"]
    
    result = []
    for p in upcoming:
        payment_date = datetime.strptime(p["date"], "%Y-%m-%d").date()
        days_until = (payment_date - today).days
        
        # Get balance before and after from lookup
        prev_date = (payment_date - timedelta(days=1)).strftime("%Y-%m-%d")
        balance_before = balance_by_date.get(prev_date)
        balance_after = balance_by_date.get(p["date"])
        
        result.append({
            "date": p["date"],
            "type": p["type"],
            "desc": p["desc"],
            "amount": p["amount"],
            "days_until": days_until,
            "balance_before": balance_before,
            "balance_after": balance_after
        })
    
    return {"payments": result}

@app.get("/ask")
async def ask_question(code: str = Query(...), question: str = Query(...)):
    verify_code(code)
    q = question.lower()
    
    # Projection requests
    if any(word in q for word in ['project', 'forecast', 'show', 'next']):
        if 'day' in q:
            days = 30
            for num in [15, 30, 45, 60, 90]:
                if str(num) in q:
                    days = num
                    break
            return {"projection": generate_daily_projection(days)}
        elif 'week' in q:
            weeks = 8
            for num in [4, 8, 12]:
                if str(num) in q:
                    weeks = num
                    break
            return {"projection": generate_weekly_projection(weeks)}
        elif 'month' in q:
            months = 6
            for num in [6, 9, 12]:
                if str(num) in q:
                    months = num
                    break
            return {"projection": generate_monthly_projection(months)}
    
    # Balance queries
    if 'balance' in q or 'current' in q:
        balance = get_today_balance()
        return {"type": "answer", "text": f"Current balance: ${balance:,.0f}"}
    
    # Low point queries
    if 'low' in q:
        forecast = get_forecast_from_db()
        low_point = min(forecast.values(), key=lambda x: x["balance"])
        low_date = [k for k, v in forecast.items() if v["balance"] == low_point["balance"]][0]
        return {"type": "answer", "text": f"Low point: ${low_point['balance']:,.0f} on {low_date}"}
    
    # High/peak queries
    if 'high' in q or 'peak' in q:
        forecast = get_forecast_from_db()
        high_point = max(forecast.values(), key=lambda x: x["balance"])
        high_date = [k for k, v in forecast.items() if v["balance"] == high_point["balance"]][0]
        return {"type": "answer", "text": f"High point: ${high_point['balance']:,.0f} on {high_date}"}
    
    # Profit queries
    if 'profit' in q:
        return {"type": "answer", "text": f"30-day average profit: ${ROLLING_30_DAY['gross_profit']:,.0f}"}
    
    # Payroll queries
    if 'payroll' in q:
        text = """💰 Payroll Structure:

**Per Pay Period** (twice monthly):
• 5-series checks: ~$60,000
• ADP Tax: ~$22,000
• ADP 401K: ~$3,200
• ADP fees: ~$230
• **Total per cycle: ~$85,000**

**Monthly Total: ~$170,000**

**Blue Shield** (BOM, separate): ~$12-19K

**February 2026 Dates**:
• Feb 2: ADP Tax + 401K + Fees (~$25K)
• Feb 3: Payroll Checks (~$60K)
• Feb 16: ADP Tax + 401K + Fees (~$25K)
• Feb 17: Payroll Checks (~$60K)"""
        return {"type": "answer", "text": text}
    
    # Payment queries
    if 'payment' in q or 'due' in q or 'coming up' in q or 'upcoming' in q:
        from datetime import date
        today = date.today()
        payments = [
            {"date": "2026-01-20", "desc": "AmEx Payment", "amount": 112399},
            {"date": "2026-01-31", "desc": "AmEx Payment", "amount": 130000},
            {"date": "2026-02-02", "desc": "ADP Tax + 401K + Fees", "amount": 25430},
            {"date": "2026-02-03", "desc": "Payroll Checks", "amount": 60000},
            {"date": "2026-02-17", "desc": "AmEx + Payroll", "amount": 190000},
            {"date": "2026-02-28", "desc": "AmEx Payment", "amount": 130000},
            {"date": "2026-03-02", "desc": "ADP Tax + 401K + Fees", "amount": 25430},
            {"date": "2026-03-03", "desc": "Payroll Checks", "amount": 60000},
            {"date": "2026-03-17", "desc": "AmEx + Payroll", "amount": 190000},
            {"date": "2026-03-31", "desc": "AmEx Payment", "amount": 130000},
        ]
        upcoming = [p for p in payments if p["date"] >= today.isoformat()]
        if not upcoming:
            return {"type": "answer", "text": "No upcoming payments scheduled."}
        lines = ["📅 Upcoming Payments:"]
        for p in upcoming[:5]:
            lines.append(f"• {p['date']}: {p['desc']} - ${p['amount']:,.0f}")
        return {"type": "answer", "text": "\n".join(lines)}
    
    # Check deposit / e-deposit estimate queries
    if 'check' in q and ('deposit' in q or 'estimate' in q):
        # Calculate CMS check deposit performance
        conn = get_db()
        cur = conn.cursor()
        cur.execute("""
            SELECT date, credit, description 
            FROM bank_transactions 
            WHERE credit > 0 
            AND (LOWER(description) LIKE '%cms%' OR LOWER(description) LIKE '%cmsrelease%')
            ORDER BY date DESC
            LIMIT 100
        """)
        rows = cur.fetchall()
        conn.close()
        
        if not rows:
            return {"type": "answer", "text": "📊 **Check Deposit Status**\n\nNo check deposit data available. Please import recent bank data to see performance vs estimate.\n\n**Estimate:** $14,059/day (~$70K/week)"}
        
        # Calculate weekly totals
        from collections import defaultdict
        from datetime import datetime as dt_class, timedelta, date as date_class
        weekly = defaultdict(float)
        for row_date, credit, desc in rows:
            try:
                if isinstance(row_date, str):
                    dt = dt_class.strptime(row_date, '%Y-%m-%d').date()
                elif isinstance(row_date, dt_class):
                    dt = row_date.date()
                elif isinstance(row_date, date_class):
                    dt = row_date
                else:
                    continue
                week_start = dt - timedelta(days=dt.weekday())
                weekly[week_start.strftime('%Y-%m-%d')] += float(credit)
            except Exception:
                continue
        
        # Get recent weeks
        sorted_weeks = sorted(weekly.keys(), reverse=True)[:4]
        target_weekly = 70000
        
        lines = ["📊 **Check Deposit Performance**", "", "**Target:** $14,059/day (~$70K/week)", ""]
        
        total_recent = 0
        for week in sorted_weeks:
            total = weekly[week]
            total_recent += total
            pct = (total / target_weekly) * 100
            status = "✅" if pct >= 90 else "⚠️" if pct >= 70 else "❌"
            lines.append(f"• Week of {week}: ${total:,.0f} ({pct:.0f}%) {status}")
        
        avg = total_recent / len(sorted_weeks) if sorted_weeks else 0
        variance = ((avg / target_weekly) - 1) * 100
        
        lines.append("")
        lines.append(f"**4-Week Avg:** ${avg:,.0f}/week")
        lines.append(f"**Variance:** {variance:+.0f}% vs estimate")
        
        return {"type": "answer", "text": "\n".join(lines)}
    
    # Refresh/update requests
    if 'refresh' in q or 'update' in q:
        async with httpx.AsyncClient() as client:
            await client.post(WEBHOOK_URL, json={
                "type": "refresh_request",
                "timestamp": now_pacific().isoformat()
            })
        return {"type": "answer", "text": "Refreshing data from Authorize.Net... Check back in a minute!"}
    
    # Unknown - send to webhook
    async with httpx.AsyncClient() as client:
        await client.post(WEBHOOK_URL, json={
            "type": "unknown_question",
            "question": question,
            "timestamp": now_pacific().isoformat()
        })
    
    return {"type": "answer", "text": "I'll look into that and get back to you!"}

@app.get("/request-update")
async def request_update(code: str = Query(...)):
    verify_code(code)
    async with httpx.AsyncClient() as client:
        await client.post(WEBHOOK_URL, json={
            "type": "update_request",
            "timestamp": now_pacific().isoformat()
        })
    return {"status": "requested", "message": "Update request sent"}

@app.get("/")
async def root():
    return FileResponse("static/index.html")

@app.get("/health")
async def health():
    return {"status": "ok"}

# Mount static files last
app.mount("/static", StaticFiles(directory="static"), name="static")
