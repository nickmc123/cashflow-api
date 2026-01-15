from fastapi import FastAPI, HTTPException, Query, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

app = FastAPI(title="Casablanca Cash Flow API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Access code for authentication
ACCESS_CODE = "cflownk"

def verify_access(access_code: str = Query(None, alias="code"), x_access_code: str = Header(None)):
    """Verify access code from query param or header"""
    code = access_code or x_access_code
    if code != ACCESS_CODE:
        raise HTTPException(status_code=401, detail="Invalid or missing access code. Use ?code=YOUR_CODE or X-Access-Code header.")
    return True

# Current forecast data
FORECAST = {
    "last_updated": "2026-01-14",
    "starting_balance": 245000,
    "starting_date": "2026-01-13",
    "low_point": {"date": "2026-01-20", "balance": 184000, "note": "After daily ops, before deposits"},
    "jan30_balance": 224000,
    "feb12_peak": 369000,
    "feb24_projection": 341000,
    "key_payments": [
        {"date": "2026-01-16", "amount": 106000, "payee": "AmEx"},
        {"date": "2026-01-31", "amount": 130000, "payee": "AmEx"},
        {"date": "2026-02-13", "amount": 100000, "payee": "AmEx"},
    ],
    "payroll_dates": ["2026-02-03", "2026-02-18"],
    "daily_ops_estimate": "15000-18000",
    "distribution_window": {"best_date": "2026-02-12", "amount": 50000, "balance_after": 234000}
}

class Question(BaseModel):
    question: str

@app.get("/")
def health():
    return {"status": "ok", "service": "Casablanca Cash Flow API", "auth_required": True}

@app.get("/forecast")
def get_forecast(code: str = Query(..., description="Access code")):
    verify_access(code)
    return FORECAST

@app.get("/balance/{date}")
def get_balance(date: str, code: str = Query(..., description="Access code")):
    verify_access(code)
    balances = {
        "jan13": {"date": "2026-01-13", "balance": 245000, "type": "actual"},
        "jan16": {"date": "2026-01-16", "balance": 195000, "type": "projected", "note": "After $106K AmEx"},
        "jan20": {"date": "2026-01-20", "balance": 184000, "type": "projected", "note": "Low point"},
        "jan30": {"date": "2026-01-30", "balance": 224000, "type": "projected"},
        "feb12": {"date": "2026-02-12", "balance": 369000, "type": "projected", "note": "Peak before AmEx"},
        "feb24": {"date": "2026-02-24", "balance": 341000, "type": "projected"},
    }
    key = date.lower().replace("-", "").replace("2026", "")
    if key in balances:
        return balances[key]
    return {"error": f"No projection for {date}", "available": list(balances.keys())}

@app.get("/low-point")
def get_low_point(code: str = Query(..., description="Access code")):
    verify_access(code)
    return FORECAST["low_point"]

@app.get("/summary")
def get_summary(code: str = Query(..., description="Access code")):
    verify_access(code)
    return {
        "current_balance": f"${FORECAST['starting_balance']:,} as of {FORECAST['starting_date']}",
        "low_point": f"${FORECAST['low_point']['balance']:,} on {FORECAST['low_point']['date']}",
        "january_outlook": "Tight but manageable - lowest at $184K on Jan 20",
        "february_outlook": "Strong recovery to $369K by Feb 12",
        "distribution_timing": "Best to take $50K distribution around Feb 12"
    }

@app.post("/ask")
def ask_question(q: Question, code: str = Query(..., description="Access code")):
    verify_access(code)
    question = q.question.lower()
    
    if "balance" in question or "current" in question:
        return {"answer": f"Current balance is ${FORECAST['starting_balance']:,} as of {FORECAST['starting_date']}"}
    elif "low" in question:
        return {"answer": f"Low point will be ${FORECAST['low_point']['balance']:,} on {FORECAST['low_point']['date']}"}
    elif "amex" in question or "payment" in question:
        return {"answer": "AmEx payments: $106K on Jan 16, $130K on Jan 31, $100K mid-Feb"}
    elif "distribution" in question:
        return {"answer": "Best time for $50K distribution is Feb 12 when balance peaks at $369K"}
    elif "payroll" in question:
        return {"answer": "Payroll dates: Feb 3 and Feb 18 (~$103K total each time)"}
    else:
        return {"answer": "Try asking about: balance, low point, amex payments, distribution, or payroll"}
