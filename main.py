from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os

app = FastAPI(title="Casablanca Cash Flow API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Cash flow data - updated Jan 14, 2026
FORECAST = {
    "current_balance": 245000,
    "low_point": {"date": "Jan 20", "balance": 184000},
    "jan_31": 224000,
    "feb_20": 289000,
    "feb_24": 341000,
    "major_payments": [
        {"date": "Jan 16", "desc": "AmEx", "amount": 106000},
        {"date": "Jan 20-22", "desc": "Payroll #1", "amount": 103000},
        {"date": "Jan 30", "desc": "AmEx", "amount": 130000},
        {"date": "Feb 3-5", "desc": "Payroll #2", "amount": 103000},
        {"date": "Feb 13", "desc": "AmEx", "amount": 100000},
        {"date": "Feb 18-20", "desc": "Payroll #3", "amount": 103000},
    ],
    "daily_income": {
        "e_deposits": {"mon": 10000, "tue": 26000, "wed": 26000, "thu": 16000, "fri": 20000},
        "cc_revenue": 20000,
        "wires": 3000,
    },
    "daily_ops": 15000,
}

class Question(BaseModel):
    question: str

@app.get("/")
def root():
    return {"status": "ok", "message": "Casablanca Cash Flow API"}

@app.get("/forecast")
def get_forecast():
    return FORECAST

@app.get("/balance/{date}")
def get_balance(date: str):
    """Get projected balance for a specific date (format: jan20, feb15, etc)"""
    date_balances = {
        "jan14": 279000, "jan15": 278000, "jan16": 200000, "jan20": 184000,
        "jan21": 189000, "jan22": 188000, "jan23": 216000, "jan30": 224000,
        "jan31": 224000, "feb3": 226000, "feb13": 297000, "feb20": 289000,
        "feb24": 341000,
    }
    key = date.lower().replace(" ", "").replace("-", "")
    if key in date_balances:
        return {"date": date, "projected_balance": date_balances[key]}
    raise HTTPException(status_code=404, detail=f"No projection for {date}")

@app.get("/low-point")
def get_low_point():
    return FORECAST["low_point"]

@app.post("/ask")
def ask_question(q: Question):
    """Simple Q&A about cash flow"""
    question = q.question.lower()
    
    if "low" in question or "minimum" in question:
        return {"answer": f"Low point is ${FORECAST['low_point']['balance']:,} on {FORECAST['low_point']['date']}"}
    
    if "current" in question or "today" in question or "now" in question:
        return {"answer": f"Current balance is ${FORECAST['current_balance']:,}"}
    
    if "feb 24" in question or "february 24" in question:
        return {"answer": f"Projected balance on Feb 24 is ${FORECAST['feb_24']:,}"}
    
    if "payroll" in question:
        payrolls = [p for p in FORECAST['major_payments'] if 'Payroll' in p['desc']]
        return {"answer": f"Upcoming payrolls: " + ", ".join([f"{p['date']} (${p['amount']:,})" for p in payrolls])}
    
    if "amex" in question or "american express" in question:
        amex = [p for p in FORECAST['major_payments'] if 'AmEx' in p['desc']]
        return {"answer": f"AmEx payments: " + ", ".join([f"{p['date']} (${p['amount']:,})" for p in amex])}
    
    return {"answer": "Try asking about: current balance, low point, Feb 24 projection, payroll dates, or AmEx payments"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
