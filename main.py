from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import re

app = FastAPI(title="Casablanca Cash Flow API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

ACCESS_CODE = "cflownk"

def check_code(code: str):
    if code != ACCESS_CODE:
        raise HTTPException(status_code=401, detail="Invalid access code")

# Current forecast data
FORECAST = {
    "last_updated": "2026-01-14",
    "starting_balance": 245000,
    "starting_date": "2026-01-13",
    "daily_balances": {
        "2026-01-13": 245000,
        "2026-01-14": 239000,
        "2026-01-15": 232000,
        "2026-01-16": 189000,  # After $106K AmEx
        "2026-01-17": 217000,
        "2026-01-20": 184000,  # LOW POINT
        "2026-01-21": 196000,
        "2026-01-22": 222000,
        "2026-01-23": 238000,
        "2026-01-24": 254000,
        "2026-01-27": 239000,
        "2026-01-28": 251000,
        "2026-01-29": 277000,
        "2026-01-30": 224000,  # After $130K AmEx
        "2026-01-31": 252000,
        "2026-02-03": 237000,  # Payroll starts
        "2026-02-04": 222000,
        "2026-02-05": 182000,  # Payroll + taxes
        "2026-02-06": 210000,
        "2026-02-07": 226000,
        "2026-02-10": 211000,
        "2026-02-11": 337000,
        "2026-02-12": 369000,  # PEAK
        "2026-02-13": 285000,  # After $100K AmEx
        "2026-02-17": 270000,
        "2026-02-18": 255000,  # Payroll starts
        "2026-02-19": 240000,
        "2026-02-20": 200000,  # Payroll + taxes
        "2026-02-21": 228000,
        "2026-02-24": 341000,
    },
    "low_point": {"date": "2026-01-20", "balance": 184000},
    "peak": {"date": "2026-02-12", "balance": 369000},
    "key_payments": [
        {"date": "2026-01-16", "amount": 106000, "payee": "AmEx"},
        {"date": "2026-01-31", "amount": 130000, "payee": "AmEx"},
        {"date": "2026-02-13", "amount": 100000, "payee": "AmEx"},
    ],
    "payroll": {
        "dates": ["2026-02-03", "2026-02-18"],
        "amount_per_period": 103430,
        "breakdown": "~$75K salary over 3 days + ~$25K taxes + ~$3.2K 401K + ~$230 ADP"
    },
    "distribution": {
        "recommended_date": "2026-02-12",
        "amount": 50000,
        "balance_after": 319000,
        "subsequent_low": 234000
    }
}

def answer_question(q: str) -> str:
    q = q.lower().strip()
    
    # Current balance questions
    if any(w in q for w in ["current balance", "balance now", "how much do we have", "what's the balance", "whats the balance", "cash on hand", "current cash"]):
        return f"Current balance is ${FORECAST['starting_balance']:,} as of {FORECAST['starting_date']}."
    
    # Low point questions
    if any(w in q for w in ["low point", "lowest", "minimum", "tightest", "most tight", "danger", "risk", "worried", "concern"]):
        lp = FORECAST['low_point']
        return f"The low point is ${lp['balance']:,} on {lp['date']} (January 20). This is after the $106K AmEx payment on Jan 16 and before deposits catch up. It's tight but manageable."
    
    # Peak / high point
    if any(w in q for w in ["peak", "highest", "maximum", "most cash", "best"]):
        pk = FORECAST['peak']
        return f"Peak balance will be ${pk['balance']:,} on {pk['date']} (February 12). This is the best time for a distribution - right before the $100K AmEx payment on Feb 13."
    
    # Distribution questions
    if any(w in q for w in ["distribution", "take money", "withdraw", "owner draw", "pull out"]):
        d = FORECAST['distribution']
        return f"Best time for the $50K distribution is around {d['recommended_date']} when we hit ${FORECAST['peak']['balance']:,}. After the distribution and subsequent AmEx payment, the low point would be ${d['subsequent_low']:,}."
    
    # AmEx / payments
    if any(w in q for w in ["amex", "american express", "payment due", "payments", "what do we owe", "upcoming bills"]):
        payments = FORECAST['key_payments']
        lines = ["Upcoming AmEx payments:"]
        for p in payments:
            lines.append(f"  • {p['date']}: ${p['amount']:,}")
        return "\n".join(lines)
    
    # Payroll
    if any(w in q for w in ["payroll", "salary", "wages", "employee pay"]):
        pr = FORECAST['payroll']
        return f"Payroll runs twice monthly: {', '.join(pr['dates'])}. Each period is ~${pr['amount_per_period']:,} total ({pr['breakdown']})."
    
    # Specific date balance
    date_match = re.search(r'(jan|feb)(?:uary)?\s*(\d{1,2})', q)
    if date_match or "balance on" in q or "what about" in q:
        if date_match:
            month = "01" if "jan" in date_match.group(1) else "02"
            day = date_match.group(2).zfill(2)
            date_key = f"2026-{month}-{day}"
            if date_key in FORECAST['daily_balances']:
                bal = FORECAST['daily_balances'][date_key]
                return f"Projected balance on {date_key}: ${bal:,}"
            else:
                # Find closest date
                return f"I don't have an exact projection for {date_key}, but check /forecast for the full daily breakdown."
    
    # January outlook
    if "january" in q or "jan" in q:
        return f"January is tight. We start at ${FORECAST['starting_balance']:,}, hit a low of ${FORECAST['low_point']['balance']:,} on Jan 20, then recover to ~$252K by month end (after $130K AmEx on Jan 31)."
    
    # February outlook  
    if "february" in q or "feb" in q:
        return f"February looks strong. We build up to ${FORECAST['peak']['balance']:,} by Feb 12, then pay $100K AmEx on Feb 13. Payroll on Feb 3 and Feb 18. End of Feb projection: ~$341K."
    
    # General outlook / summary
    if any(w in q for w in ["outlook", "summary", "overview", "how are we", "how's it", "hows it", "status", "situation", "looking"]):
        return f"""Cash flow summary:
• Current: ${FORECAST['starting_balance']:,} (Jan 13)
• January low: ${FORECAST['low_point']['balance']:,} (Jan 20) - tight but OK
• January end: ~$224K (after $130K AmEx)
• February peak: ${FORECAST['peak']['balance']:,} (Feb 12) - best time for $50K distribution
• February end: ~$341K

We'll make it through the tight January stretch. February looks much healthier."""
    
    # Safe / OK questions
    if any(w in q for w in ["safe", "ok", "okay", "fine", "make it", "survive"]):
        return f"Yes, we'll be OK. The tightest point is ${FORECAST['low_point']['balance']:,} on Jan 20 - not comfortable but manageable. Things improve significantly in February."
    
    # Fallback
    return f"""I can answer questions about:
• Current balance and projections
• Low points and peak balances  
• AmEx payment schedule
• Payroll dates
• Distribution timing
• January/February outlook

Try asking: "What's the current balance?" or "When is the low point?" or "How's cash flow looking?"""

class Question(BaseModel):
    question: str

@app.get("/")
async def root():
    return {"status": "ok", "service": "Casablanca Cash Flow API", "hint": "Add ?code=ACCESS_CODE to endpoints"}

@app.get("/summary")
async def summary(code: str = Query(...)):
    check_code(code)
    return {
        "current_balance": f"${FORECAST['starting_balance']:,} as of {FORECAST['starting_date']}",
        "low_point": f"${FORECAST['low_point']['balance']:,} on {FORECAST['low_point']['date']}",
        "january_outlook": "Tight but manageable - lowest at $184K on Jan 20",
        "february_outlook": f"Strong recovery to ${FORECAST['peak']['balance']:,} by Feb 12",
        "distribution_timing": "Best to take $50K distribution around Feb 12"
    }

@app.get("/forecast")
async def forecast(code: str = Query(...)):
    check_code(code)
    return FORECAST

@app.get("/balance/{date}")
async def get_balance(date: str, code: str = Query(...)):
    check_code(code)
    # Try to find the date
    if date in FORECAST['daily_balances']:
        return {"date": date, "projected_balance": FORECAST['daily_balances'][date]}
    # Try common formats
    for key in FORECAST['daily_balances']:
        if date.lower().replace("-", "").replace("/", "") in key.replace("-", ""):
            return {"date": key, "projected_balance": FORECAST['daily_balances'][key]}
    raise HTTPException(status_code=404, detail=f"No projection for date: {date}")

@app.get("/low-point")
async def low_point(code: str = Query(...)):
    check_code(code)
    return FORECAST['low_point']

@app.post("/ask")
async def ask(q: Question, code: str = Query(...)):
    check_code(code)
    answer = answer_question(q.question)
    return {"question": q.question, "answer": answer}

@app.get("/ask")
async def ask_get(question: str = Query(...), code: str = Query(...)):
    check_code(code)
    answer = answer_question(question)
    return {"question": question, "answer": answer}
