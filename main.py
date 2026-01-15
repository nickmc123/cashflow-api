from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import re
from datetime import datetime
import httpx
import os

app = FastAPI(title="Casablanca Cash Flow API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

ACCESS_CODE = "cflownk"
WEBHOOK_URL = os.environ.get("TASKLET_WEBHOOK_URL", "")

# Core forecast data - updated via webhook
FORECAST_DATA = {
    "current_balance": 245000,
    "current_date": "2026-01-13",
    "low_point": {"amount": 184000, "date": "2026-01-20"},
    "january_end": 224000,
    "february_peak": {"amount": 369000, "date": "2026-02-12"},
    "february_end": 341000,
    "distribution": {"amount": 50000, "best_date": "2026-02-12", "resulting_low": 234000},
    "amex_payments": [
        {"amount": 106000, "date": "2026-01-16", "status": "upcoming"},
        {"amount": 130000, "date": "2026-01-31", "status": "upcoming"},
        {"amount": 100000, "date": "2026-02-13", "status": "upcoming"}
    ],
    "payroll_dates": ["2026-02-03", "2026-02-18"],
    "daily_forecast": [
        {"date": "2026-01-13", "balance": 245000},
        {"date": "2026-01-14", "balance": 237000},
        {"date": "2026-01-15", "balance": 242000},
        {"date": "2026-01-16", "balance": 189000, "note": "$106K AmEx payment"},
        {"date": "2026-01-17", "balance": 197000},
        {"date": "2026-01-20", "balance": 184000, "note": "LOW POINT"},
        {"date": "2026-01-21", "balance": 190000},
        {"date": "2026-01-22", "balance": 207000},
        {"date": "2026-01-23", "balance": 215000},
        {"date": "2026-01-24", "balance": 219000},
        {"date": "2026-01-27", "balance": 205000},
        {"date": "2026-01-28", "balance": 222000},
        {"date": "2026-01-29", "balance": 240000},
        {"date": "2026-01-30", "balance": 248000},
        {"date": "2026-01-31", "balance": 224000, "note": "$130K AmEx payment"},
        {"date": "2026-02-03", "balance": 258000, "note": "Payroll starts"},
        {"date": "2026-02-06", "balance": 275000},
        {"date": "2026-02-10", "balance": 320000},
        {"date": "2026-02-12", "balance": 369000, "note": "PEAK - best for distribution"},
        {"date": "2026-02-13", "balance": 284000, "note": "$100K AmEx payment"},
        {"date": "2026-02-18", "balance": 310000, "note": "Payroll starts"},
        {"date": "2026-02-24", "balance": 341000}
    ],
    "last_updated": "2026-01-13T21:00:00",
    "recent_settlements": [],
    "pasted_data": []
}

def verify_code(code: str):
    if code != ACCESS_CODE:
        raise HTTPException(status_code=401, detail="Invalid access code")

def format_money(amount):
    return f"${amount:,.0f}"

class DataInput(BaseModel):
    data: str
    data_type: Optional[str] = "auto"  # auto, bank, settlements, authorize
    notes: Optional[str] = None

class RefreshRequest(BaseModel):
    source: Optional[str] = "authorize"  # authorize, all

@app.get("/")
async def root():
    return {"status": "ok", "service": "Casablanca Cash Flow API", "version": "2.0"}

@app.get("/summary")
async def get_summary(code: str = Query(...)):
    verify_code(code)
    return {
        "current_balance": f"{format_money(FORECAST_DATA['current_balance'])} as of {FORECAST_DATA['current_date']}",
        "low_point": f"{format_money(FORECAST_DATA['low_point']['amount'])} on {FORECAST_DATA['low_point']['date']}",
        "january_outlook": f"Tight but manageable - lowest at {format_money(FORECAST_DATA['low_point']['amount'])} on Jan 20",
        "february_outlook": f"Strong recovery to {format_money(FORECAST_DATA['february_peak']['amount'])} by Feb 12",
        "distribution_timing": f"Best to take {format_money(FORECAST_DATA['distribution']['amount'])} distribution around {FORECAST_DATA['distribution']['best_date']}",
        "last_updated": FORECAST_DATA['last_updated']
    }

@app.get("/forecast")
async def get_forecast(code: str = Query(...)):
    verify_code(code)
    return FORECAST_DATA

@app.get("/balance/{date}")
async def get_balance(date: str, code: str = Query(...)):
    verify_code(code)
    for day in FORECAST_DATA['daily_forecast']:
        if date.lower() in day['date'].lower() or day['date'].replace('2026-', '').replace('-', '') == date.replace('jan', '01').replace('feb', '02').replace('/', ''):
            return {"date": day['date'], "projected_balance": format_money(day['balance']), "note": day.get('note', '')}
    return {"error": f"No forecast for {date}", "available_dates": [d['date'] for d in FORECAST_DATA['daily_forecast']]}

@app.get("/low-point")
async def get_low_point(code: str = Query(...)):
    verify_code(code)
    lp = FORECAST_DATA['low_point']
    return {
        "date": lp['date'],
        "amount": format_money(lp['amount']),
        "context": "This is after the $106K AmEx payment on Jan 16 and before deposits catch up. Tight but manageable."
    }

@app.get("/ask")
async def ask_question(question: str = Query(...), code: str = Query(...)):
    verify_code(code)
    q = question.lower()
    
    # Check for data update requests
    if any(word in q for word in ['update', 'refresh', 'pull', 'fetch', 'sync', 'new data', 'latest']):
        if WEBHOOK_URL:
            return {
                "question": question,
                "answer": "To update with the latest data, use POST /refresh endpoint. This will pull the latest Authorize.net settlement reports and bank data.",
                "action_available": "POST /refresh?code=cflownk"
            }
        else:
            return {
                "question": question,
                "answer": "Data updates are processed by the main system. Ask Nick to update the forecast, or paste new data using POST /data"
            }
    
    # Balance/cash questions
    if any(word in q for word in ['balance', 'cash', 'how much', 'current', 'now', 'today']):
        answer = f"Current balance is {format_money(FORECAST_DATA['current_balance'])} as of {FORECAST_DATA['current_date']}."
        if 'safe' in q or 'ok' in q or 'worry' in q:
            answer += f" We'll hit a low of {format_money(FORECAST_DATA['low_point']['amount'])} on {FORECAST_DATA['low_point']['date']}, but we'll make it through."
        return {"question": question, "answer": answer}
    
    # Low point questions
    if any(word in q for word in ['low', 'minimum', 'worst', 'tight', 'lowest']):
        lp = FORECAST_DATA['low_point']
        return {
            "question": question,
            "answer": f"The low point is {format_money(lp['amount'])} on {lp['date']} (January 20). This is after the $106K AmEx payment on Jan 16 and before deposits catch up. It's tight but manageable."
        }
    
    # Distribution questions
    if any(word in q for word in ['distribution', 'take out', 'withdraw', 'owner', 'dividend']):
        d = FORECAST_DATA['distribution']
        return {
            "question": question,
            "answer": f"Best time for the {format_money(d['amount'])} distribution is around {d['best_date']} when we hit {format_money(FORECAST_DATA['february_peak']['amount'])}. After the distribution and subsequent AmEx payment, the low point would be {format_money(d['resulting_low'])}."
        }
    
    # AmEx/payment questions
    if any(word in q for word in ['amex', 'american express', 'payment', 'due', 'owe', 'pay']):
        payments = FORECAST_DATA['amex_payments']
        payment_str = "\n".join([f"• {format_money(p['amount'])} on {p['date']}" for p in payments])
        return {
            "question": question,
            "answer": f"AmEx payment schedule:\n{payment_str}\n\nThe Jan 16 payment ($106K) creates our tightest moment. Jan 31 ($130K) is manageable. Feb 13 ($100K) comes right after our peak."
        }
    
    # Payroll questions
    if any(word in q for word in ['payroll', 'salary', 'wages', 'employee']):
        return {
            "question": question,
            "answer": f"Payroll runs twice monthly (~$103K total each time including taxes, 401K, fees). February dates: {', '.join(FORECAST_DATA['payroll_dates'])}. Each payroll spreads over 3 days."
        }
    
    # Specific date questions
    date_match = re.search(r'(jan|feb)\w*\s*(\d{1,2})|\d{1,2}[/-](\d{1,2})', q)
    if date_match:
        for day in FORECAST_DATA['daily_forecast']:
            if any(str(x) in day['date'] for x in date_match.groups() if x):
                note = f" Note: {day['note']}" if day.get('note') else ""
                return {
                    "question": question,
                    "answer": f"On {day['date']}, projected balance is {format_money(day['balance'])}.{note}"
                }
    
    # January questions
    if 'january' in q or 'jan' in q:
        return {
            "question": question,
            "answer": f"January outlook: Starting at {format_money(FORECAST_DATA['current_balance'])}, dropping to {format_money(FORECAST_DATA['low_point']['amount'])} on Jan 20 (tightest point), ending around {format_money(FORECAST_DATA['january_end'])} after the $130K AmEx payment on Jan 31."
        }
    
    # February questions
    if 'february' in q or 'feb' in q:
        return {
            "question": question,
            "answer": f"February outlook: Strong recovery! Peak of {format_money(FORECAST_DATA['february_peak']['amount'])} on Feb 12 (best time for distribution). After $100K AmEx payment on Feb 13 and second payroll on Feb 18, ending around {format_money(FORECAST_DATA['february_end'])}."
        }
    
    # Safety/worry questions
    if any(word in q for word in ['safe', 'ok', 'worry', 'concern', 'risk', 'trouble', 'problem']):
        return {
            "question": question,
            "answer": f"We're tight but safe. The lowest point ({format_money(FORECAST_DATA['low_point']['amount'])} on Jan 20) gives us enough cushion. February looks much healthier with a peak of {format_money(FORECAST_DATA['february_peak']['amount'])}. No immediate concerns."
        }
    
    # How's it looking / general
    if any(word in q for word in ['how', 'looking', 'overview', 'summary', 'status', 'situation']):
        return {
            "question": question,
            "answer": f"Cash flow summary:\n• Current: {format_money(FORECAST_DATA['current_balance'])} ({FORECAST_DATA['current_date']})\n• January low: {format_money(FORECAST_DATA['low_point']['amount'])} ({FORECAST_DATA['low_point']['date']}) - tight but OK\n• January end: ~{format_money(FORECAST_DATA['january_end'])} (after $130K AmEx)\n• February peak: {format_money(FORECAST_DATA['february_peak']['amount'])} ({FORECAST_DATA['february_peak']['date']}) - best time for {format_money(FORECAST_DATA['distribution']['amount'])} distribution\n• February end: ~{format_money(FORECAST_DATA['february_end'])}\n\nWe'll make it through the tight January stretch. February looks much healthier."
        }
    
    # Recent data questions
    if any(word in q for word in ['recent', 'settlement', 'deposit', 'authorize']):
        if FORECAST_DATA['recent_settlements']:
            settlements = "\n".join([f"• {s['date']}: {format_money(s['amount'])}" for s in FORECAST_DATA['recent_settlements'][-7:]])
            return {"question": question, "answer": f"Recent settlements:\n{settlements}"}
        return {"question": question, "answer": "No recent settlement data loaded. Use POST /data to paste Authorize.net reports or POST /refresh to pull latest."}
    
    # Default
    return {
        "question": question,
        "answer": f"Current balance: {format_money(FORECAST_DATA['current_balance'])}. Low point: {format_money(FORECAST_DATA['low_point']['amount'])} on {FORECAST_DATA['low_point']['date']}. Ask about: balance, low point, AmEx payments, payroll, distribution timing, January outlook, February outlook, or specific dates."
    }

@app.post("/data")
async def submit_data(data_input: DataInput, code: str = Query(...)):
    """Submit pasted data (bank statements, Authorize.net reports, etc.)"""
    verify_code(code)
    
    # Store the pasted data
    entry = {
        "submitted_at": datetime.now().isoformat(),
        "data_type": data_input.data_type,
        "notes": data_input.notes,
        "data": data_input.data,
        "line_count": len(data_input.data.split('\n'))
    }
    FORECAST_DATA['pasted_data'].append(entry)
    
    # If webhook is configured, forward to Tasklet for processing
    if WEBHOOK_URL:
        try:
            async with httpx.AsyncClient() as client:
                await client.post(WEBHOOK_URL, json={
                    "action": "process_data",
                    "data_type": data_input.data_type,
                    "data": data_input.data,
                    "notes": data_input.notes
                }, timeout=10)
            return {
                "status": "received",
                "message": "Data received and sent to processing. Forecast will be updated shortly.",
                "lines_received": entry['line_count'],
                "data_type": data_input.data_type
            }
        except:
            pass
    
    return {
        "status": "stored",
        "message": "Data received and stored. Note: Automatic processing not configured - data stored for manual review.",
        "lines_received": entry['line_count'],
        "data_type": data_input.data_type
    }

@app.post("/refresh")
async def refresh_data(code: str = Query(...), source: str = Query("authorize")):
    """Trigger a data refresh from Authorize.net or other sources"""
    verify_code(code)
    
    if not WEBHOOK_URL:
        return {
            "status": "not_configured",
            "message": "Automatic refresh not configured. Ask Nick to pull the latest Authorize.net reports and update the forecast."
        }
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(WEBHOOK_URL, json={
                "action": "refresh",
                "source": source,
                "requested_at": datetime.now().isoformat()
            }, timeout=10)
        return {
            "status": "triggered",
            "message": f"Refresh triggered for {source}. The forecast will be updated with latest data shortly.",
            "webhook_status": response.status_code
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Could not trigger refresh: {str(e)}. Ask Nick to manually update."
        }

@app.get("/data")
async def get_pasted_data(code: str = Query(...)):
    """View previously pasted data"""
    verify_code(code)
    return {
        "pasted_entries": len(FORECAST_DATA['pasted_data']),
        "recent": FORECAST_DATA['pasted_data'][-5:] if FORECAST_DATA['pasted_data'] else [],
        "last_updated": FORECAST_DATA['last_updated']
    }
