from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel
from typing import Optional
import httpx
import os
import re
from datetime import datetime, timedelta
import psycopg2
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

# Initialize on startup
@app.on_event("startup")
async def startup():
    init_db()

def verify_code(code: str):
    if code != ACCESS_CODE:
        raise HTTPException(status_code=401, detail="Invalid access code")

# Rolling 30-day averages (updated when data is submitted)
ROLLING_30_DAY = {
    "cash_in": 285000,
    "cash_out": 199500,
    "gross_profit": 85500,
}

MONTHLY_PAYROLL = 206000

# Fallback forecast if database is empty
DEFAULT_FORECAST = {
    "2026-01-15": {"balance": 237000, "note": "Normal ops"},
    "2026-01-16": {"balance": 225000, "note": "AmEx $106K payment"},
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
    "2026-02-02": {"balance": 245000, "note": "February starts"},
    "2026-02-03": {"balance": 220000, "note": "Payroll starts"},
    "2026-02-04": {"balance": 200000, "note": "Payroll continues"},
    "2026-02-05": {"balance": 175000, "note": "Payroll + taxes"},
    "2026-02-06": {"balance": 190000, "note": "Recovery"},
    "2026-02-09": {"balance": 220000, "note": "Week buildup"},
    "2026-02-10": {"balance": 245000, "note": "Strong week"},
    "2026-02-11": {"balance": 280000, "note": "Approaching peak"},
    "2026-02-12": {"balance": 369000, "note": "PEAK - Best for distribution"},
    "2026-02-13": {"balance": 269000, "note": "After $100K AmEx payment"},
    "2026-02-17": {"balance": 290000, "note": "Pre-payroll"},
    "2026-02-18": {"balance": 265000, "note": "Payroll starts"},
    "2026-02-19": {"balance": 245000, "note": "Payroll continues"},
    "2026-02-20": {"balance": 220000, "note": "Payroll + taxes"},
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
    """Get balance for today from forecast or nearest date"""
    forecast = get_forecast_from_db()
    today = datetime.now().strftime("%Y-%m-%d")
    if today in forecast:
        return forecast[today]["balance"]
    sorted_dates = sorted(forecast.keys())
    for d in sorted_dates:
        if d >= today:
            return forecast[d]["balance"]
    return forecast[sorted_dates[-1]]["balance"] if sorted_dates else 237000

class DataSubmission(BaseModel):
    data: str

def parse_bank_data(raw_data: str) -> list:
    """Parse bank transaction data from various formats including messy web-copied data"""
    lines = raw_data.strip().split('\n')
    transactions = []
    current_date = None
    
    # First try tab-separated format
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        # Try tab-separated format: Date\tDescription\tDebit\tCredit\tBalance
        parts = line.split('\t')
        if len(parts) >= 5:
            try:
                date_str = parts[0].strip()
                desc = parts[1].strip()
                debit = parts[2].strip().replace(',', '').replace('$', '')
                credit = parts[3].strip().replace(',', '').replace('$', '')
                balance = parts[4].strip().replace(',', '').replace('$', '')
                
                # Parse date
                date = None
                for fmt in ['%m/%d/%Y', '%Y-%m-%d', '%m/%d/%y', '%m-%d-%Y']:
                    try:
                        date = datetime.strptime(date_str, fmt)
                        break
                    except:
                        continue
                
                if date:
                    transactions.append({
                        'date': date,
                        'description': desc,
                        'debit': float(debit) if debit else 0,
                        'credit': float(credit) if credit else 0,
                        'balance': float(balance) if balance else 0
                    })
            except Exception as e:
                continue
    
    # If tab-separated worked, return those
    if transactions:
        return transactions
    
    # Try parsing messy web-copied format with date headers like "JAN 13, 2026 (17)"
    # Pattern: date headers followed by transactions
    date_pattern = re.compile(r'(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)\s+(\d{1,2}),?\s+(\d{4})', re.IGNORECASE)
    amount_pattern = re.compile(r'^-?[\d,]+\.\d{2}$')
    
    pending_desc = None
    pending_amount = None
    last_balance = None
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Check for date header
        date_match = date_pattern.search(line)
        if date_match:
            month_str = date_match.group(1).upper()
            day = int(date_match.group(2))
            year = int(date_match.group(3))
            months = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
                     'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}
            current_date = datetime(year, months[month_str], day)
            continue
        
        # Check if this is an amount (e.g., "1,333.00" or "-325.00")
        clean_line = line.replace(',', '').replace('$', '')
        if amount_pattern.match(clean_line) or amount_pattern.match(line.replace(',', '')):
            amount = float(clean_line)
            
            # If we have a pending description, this completes a transaction
            if pending_desc and current_date:
                if pending_amount is not None:
                    # This is the balance
                    transactions.append({
                        'date': current_date,
                        'description': pending_desc,
                        'debit': abs(pending_amount) if pending_amount < 0 else 0,
                        'credit': pending_amount if pending_amount > 0 else 0,
                        'balance': amount
                    })
                    last_balance = amount
                    pending_desc = None
                    pending_amount = None
                else:
                    # This is the amount, next number will be balance
                    pending_amount = amount
            elif amount > 10000:  # Likely a balance
                last_balance = amount
            else:
                pending_amount = amount
            continue
        
        # Check if this looks like a description
        if len(line) > 3 and not line.replace(',', '').replace('.', '').replace('-', '').isdigit():
            # Save any pending transaction first
            if pending_desc and pending_amount is not None and current_date and last_balance:
                transactions.append({
                    'date': current_date,
                    'description': pending_desc,
                    'debit': abs(pending_amount) if pending_amount < 0 else 0,
                    'credit': pending_amount if pending_amount > 0 else 0,
                    'balance': last_balance
                })
            pending_desc = line
            pending_amount = None
    
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
                "timestamp": datetime.now().isoformat()
            })
        return {"status": "queued", "message": "Data sent for processing"}
    
    conn = get_db()
    if not conn:
        return {"status": "error", "message": "Database not available"}
    
    cur = conn.cursor()
    
    # Get existing transactions for deduplication
    min_date = min(tx['date'] for tx in transactions).date()
    existing = get_existing_transactions(conn, min_date)
    
    added_count = 0
    skipped_count = 0
    latest_balance = None
    
    # Insert transactions and update forecast with actual balances
    for tx in transactions:
        tx_date = tx['date'].date() if hasattr(tx['date'], 'date') else tx['date']
        
        # Check for duplicate
        sig = f"{tx_date}|{tx['description'][:30] if tx['description'] else ''}|{tx['debit']}|{tx['credit']}|{tx['balance']}"
        if sig in existing:
            skipped_count += 1
            continue
        
        # Insert transaction
        cur.execute("""
            INSERT INTO bank_transactions (date, description, debit, credit, balance)
            VALUES (%s, %s, %s, %s, %s)
        """, (tx_date, tx['description'], tx['debit'], tx['credit'], tx['balance']))
        
        # Update forecast with actual balance
        note = "Actual" if tx['balance'] else ""
        if tx['debit'] > 50000:
            note = f"Large debit: ${tx['debit']:,.0f}"
        elif tx['credit'] > 50000:
            note = f"Large credit: ${tx['credit']:,.0f}"
        
        cur.execute("""
            INSERT INTO forecast (date, balance, note)
            VALUES (%s, %s, %s)
            ON CONFLICT (date) DO UPDATE SET
                balance = EXCLUDED.balance,
                note = CASE WHEN EXCLUDED.note != '' THEN EXCLUDED.note ELSE forecast.note END,
                updated_at = CURRENT_TIMESTAMP
        """, (tx_date, tx['balance'], note))
        
        existing.add(sig)  # Prevent duplicates within same submission
        added_count += 1
        latest_balance = tx['balance']
    
    conn.commit()
    cur.close()
    conn.close()
    
    return {
        "status": "success",
        "message": f"Added {added_count} new transactions" + (f", skipped {skipped_count} duplicates" if skipped_count else ""),
        "added": added_count,
        "skipped": skipped_count,
        "latest_balance": latest_balance
    }


@app.get("/transactions")
async def get_transactions(code: str = Query(...), limit: int = Query(default=20, le=100)):
    """Get recent bank transactions"""
    verify_code(code)
    
    conn = get_db()
    if not conn:
        return {"transactions": [], "message": "Database not available"}
    
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT date, description, debit, credit, balance, created_at
        FROM bank_transactions
        ORDER BY date DESC, id DESC
        LIMIT %s
    """, (limit,))
    
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    transactions = []
    for row in rows:
        transactions.append({
            "date": row['date'].strftime("%Y-%m-%d") if row['date'] else None,
            "description": row['description'],
            "debit": float(row['debit']) if row['debit'] else 0,
            "credit": float(row['credit']) if row['credit'] else 0,
            "balance": float(row['balance']) if row['balance'] else 0
        })
    
    return {"transactions": transactions}

# Projection generators
def generate_daily_projection(days: int) -> dict:
    forecast = get_forecast_from_db()
    start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    today_str = start_date.strftime("%Y-%m-%d")
    
    if today_str in forecast:
        start_balance = forecast[today_str]["balance"]
    else:
        start_balance = get_today_balance()
    
    daily_net = (ROLLING_30_DAY['cash_in'] - ROLLING_30_DAY['cash_out']) / 30
    
    rows = []
    balance = start_balance
    low_bal, low_date, low_note = float('inf'), None, ""
    high_bal, high_date, high_note = 0, None, ""
    
    for i in range(days):
        date = start_date + timedelta(days=i)
        date_str = date.strftime("%Y-%m-%d")
        
        note = ""
        if date_str in forecast:
            balance = forecast[date_str]["balance"]
            note = forecast[date_str].get("note", "")
        else:
            if date.weekday() < 5:
                balance = int(balance + daily_net)
        
        if balance < low_bal:
            low_bal, low_date, low_note = balance, date.strftime("%b %d"), note
        if balance > high_bal:
            high_bal, high_date, high_note = balance, date.strftime("%b %d"), note
        
        rows.append({
            "date": date.strftime("%a %b %d"),
            "balance": balance,
            "note": note
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
    forecast = get_forecast_from_db()
    start_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    today_str = start_date.strftime("%Y-%m-%d")
    
    if today_str in forecast:
        start_balance = forecast[today_str]["balance"]
    else:
        start_balance = get_today_balance()
    
    weekly_net = ROLLING_30_DAY['cash_in'] - ROLLING_30_DAY['cash_out'] - (MONTHLY_PAYROLL / 4)
    
    rows = []
    balance = start_balance
    low_bal, low_date = float('inf'), None
    high_bal, high_date = 0, None
    
    for i in range(weeks):
        week_start = start_date + timedelta(weeks=i)
        week_end = week_start + timedelta(days=6)
        
        note = ""
        week_balance = balance
        for j in range(7):
            d = week_start + timedelta(days=j)
            d_str = d.strftime("%Y-%m-%d")
            if d_str in forecast:
                week_balance = forecast[d_str]["balance"]
                if forecast[d_str].get("note"):
                    note = forecast[d_str]["note"]
        
        balance = week_balance if week_balance != balance else int(balance + weekly_net)
        
        if balance < low_bal:
            low_bal, low_date = balance, week_start.strftime("%b %d")
        if balance > high_bal:
            high_bal, high_date = balance, week_start.strftime("%b %d")
        
        rows.append({
            "date": f"Week of {week_start.strftime('%b %d')}",
            "balance": balance,
            "note": note
        })
    
    return {
        "type": "projection",
        "title": f"{weeks}-Week Projection",
        "period": "weekly",
        "rows": rows,
        "low": {"value": low_bal, "label": low_date, "note": ""},
        "high": {"value": high_bal, "label": high_date, "note": ""}
    }

def generate_monthly_projection(months: int) -> dict:
    start_balance = get_today_balance()
    monthly_net = (ROLLING_30_DAY['cash_in'] - ROLLING_30_DAY['cash_out']) * 30 / 30 - MONTHLY_PAYROLL
    
    rows = []
    balance = start_balance
    low_bal, low_date = float('inf'), None
    high_bal, high_date = 0, None
    
    current = datetime.now().replace(day=1)
    
    for i in range(months):
        month_date = current + timedelta(days=32*i)
        month_date = month_date.replace(day=1)
        
        note = ""
        if i == 0:
            note = "Current month"
        
        if i > 0:
            balance = int(balance + monthly_net)
        
        if balance < low_bal:
            low_bal, low_date = balance, month_date.strftime("%b %Y")
        if balance > high_bal:
            high_bal, high_date = balance, month_date.strftime("%b %Y")
        
        rows.append({
            "date": month_date.strftime("%B %Y"),
            "balance": balance,
            "note": note
        })
    
    return {
        "type": "projection",
        "title": f"{months}-Month Projection",
        "period": "monthly",
        "rows": rows,
        "low": {"value": low_bal, "label": low_date, "note": ""},
        "high": {"value": high_bal, "label": high_date, "note": ""}
    }

@app.get("/summary")
async def get_summary(code: str = Query(...)):
    verify_code(code)
    forecast = get_forecast_from_db()
    sorted_dates = sorted(forecast.keys())
    
    today = datetime.now().strftime("%Y-%m-%d")
    current_balance = get_today_balance()
    
    low_point = min(forecast.values(), key=lambda x: x["balance"])
    low_date = [k for k, v in forecast.items() if v["balance"] == low_point["balance"]][0]
    
    high_point = max(forecast.values(), key=lambda x: x["balance"])
    high_date = [k for k, v in forecast.items() if v["balance"] == high_point["balance"]][0]
    
    gross_profit = ROLLING_30_DAY['gross_profit']
    net_profit = gross_profit - MONTHLY_PAYROLL
    
    return {
        "current_balance": current_balance,
        "as_of": today,
        "low_point": {
            "balance": low_point["balance"],
            "date": low_date,
            "note": low_point.get("note", "")
        },
        "high_point": {
            "balance": high_point["balance"],
            "date": high_date,
            "note": high_point.get("note", "")
        },
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
    
    # Refresh/update requests
    if 'refresh' in q or 'update' in q:
        async with httpx.AsyncClient() as client:
            await client.post(WEBHOOK_URL, json={
                "type": "refresh_request",
                "timestamp": datetime.now().isoformat()
            })
        return {"type": "answer", "text": "Refreshing data from Authorize.Net... Check back in a minute!"}
    
    # Unknown - send to webhook
    async with httpx.AsyncClient() as client:
        await client.post(WEBHOOK_URL, json={
            "type": "unknown_question",
            "question": question,
            "timestamp": datetime.now().isoformat()
        })
    
    return {"type": "answer", "text": "I'll look into that and get back to you!"}

@app.get("/request-update")
async def request_update(code: str = Query(...)):
    verify_code(code)
    async with httpx.AsyncClient() as client:
        await client.post(WEBHOOK_URL, json={
            "type": "update_request",
            "timestamp": datetime.now().isoformat()
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
