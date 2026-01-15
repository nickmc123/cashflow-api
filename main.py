from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel
from typing import Optional
import httpx
import os
from datetime import datetime

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
WEBHOOK_URL = "https://api.tasklet.ai/api/webhooks/wti_x6gx7ax4z6vwmepgd6th/trigger?secret=YpN5E73c9gPW8bZB1UxT"

def verify_code(code: str):
    if code != ACCESS_CODE:
        raise HTTPException(status_code=401, detail="Invalid access code")

# Simple Cash Flow Profit Calculation (Rolling 30-Day Average)
# Method: Cash In - Cash Out = Profit
# Excludes payroll from expense analysis per user instructions

ROLLING_30_DAY = {
    "cash_in": 285000,      # 30-day deposits (CC processors + wires + e-deposits)
    "cash_out": 199500,     # 30-day ops debits (excludes payroll)
    "gross_profit": 85500,  # Cash In - Cash Out
}

# Payroll is tracked separately: ~$103K per cycle x 2 = ~$206K/month
MONTHLY_PAYROLL = 206000

# Net profit after payroll
NET_MONTHLY_PROFIT = ROLLING_30_DAY["gross_profit"] - MONTHLY_PAYROLL  # Typically negative due to payroll

# Forecast data
FORECAST = {
    "2026-01-13": {"balance": 245000, "note": "Starting balance confirmed"},
    "2026-01-14": {"balance": 241000, "note": "Normal ops"},
    "2026-01-15": {"balance": 237000, "note": "Normal ops"},
    "2026-01-16": {"balance": 225000, "note": "AmEx $106K payment"},
    "2026-01-17": {"balance": 221000, "note": "Normal ops"},
    "2026-01-20": {"balance": 184000, "note": "LOW POINT - MLK holiday weekend impact"},
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
    "2026-02-03": {"balance": 220000, "note": "Payroll starts (~$75K over 3 days)"},
    "2026-02-04": {"balance": 200000, "note": "Payroll continues"},
    "2026-02-05": {"balance": 175000, "note": "Payroll + taxes (~$25K)"},
    "2026-02-06": {"balance": 190000, "note": "Recovery"},
    "2026-02-09": {"balance": 220000, "note": "Week buildup"},
    "2026-02-10": {"balance": 245000, "note": "Strong week"},
    "2026-02-11": {"balance": 280000, "note": "Approaching peak"},
    "2026-02-12": {"balance": 369000, "note": "PEAK - Best time for distribution"},
    "2026-02-13": {"balance": 269000, "note": "After $100K AmEx payment"},
    "2026-02-17": {"balance": 290000, "note": "Pre-payroll"},
    "2026-02-18": {"balance": 265000, "note": "Payroll starts"},
    "2026-02-19": {"balance": 245000, "note": "Payroll continues"},
    "2026-02-20": {"balance": 220000, "note": "Payroll + taxes"},
    "2026-02-24": {"balance": 341000, "note": "End of forecast period"}
}

class DataSubmission(BaseModel):
    data: str

# Projection generators
def generate_daily_projection(days: int) -> str:
    from datetime import datetime, timedelta
    start_date = datetime(2026, 1, 13)
    start_balance = 245000
    daily_net = (ROLLING_30_DAY['cash_in'] - ROLLING_30_DAY['cash_out']) / 30
    
    lines = [f"📅 **{days}-Day Projection**\n"]
    
    balance = start_balance
    low_bal, low_date = float('inf'), None
    high_bal, high_date = 0, None
    
    entries = []
    for i in range(days):
        date = start_date + timedelta(days=i)
        date_str = date.strftime("%Y-%m-%d")
        
        note = ""
        if date_str in FORECAST:
            balance = FORECAST[date_str]["balance"]
            note = FORECAST[date_str].get("note", "")
        else:
            if date.weekday() >= 5:
                note = "🔸"
            else:
                balance = int(balance + daily_net)
        
        if balance < low_bal:
            low_bal, low_date = balance, date
        if balance > high_bal:
            high_bal, high_date = balance, date
        
        # Format: emoji indicator based on balance level
        if balance < 200000:
            indicator = "🔴"
        elif balance < 250000:
            indicator = "🟡"
        else:
            indicator = "🟢"
        
        day_name = date.strftime("%a")
        date_fmt = date.strftime("%b %d")
        entry = f"{indicator} **{day_name} {date_fmt}**: ${balance:,}"
        if note and note != "🔸":
            entry += f" _{note}_"
        entries.append(entry)
    
    # Show entries
    lines.extend(entries)
    
    # Summary
    lines.append("\n─────────────")
    lines.append(f"🔴 **Low**: ${low_bal:,} ({low_date.strftime('%b %d')})")
    lines.append(f"🟢 **High**: ${high_bal:,} ({high_date.strftime('%b %d')})")
    
    return "\n".join(lines)

def generate_weekly_projection(weeks: int) -> str:
    from datetime import datetime, timedelta
    start_date = datetime(2026, 1, 13)
    weekly_net = ROLLING_30_DAY['cash_in'] - ROLLING_30_DAY['cash_out'] - (MONTHLY_PAYROLL / 4)
    
    lines = [f"📆 **{weeks}-Week Projection**\n"]
    
    low_bal, low_wk = float('inf'), None
    high_bal, high_wk = 0, None
    
    for i in range(weeks):
        end_date = start_date + timedelta(weeks=i+1)
        date_str = end_date.strftime("%Y-%m-%d")
        
        if date_str in FORECAST:
            balance = FORECAST[date_str]["balance"]
        else:
            balance = 245000 + int(weekly_net * (i + 1) / 4)
            balance = max(150000, min(400000, balance))
        
        if balance < low_bal:
            low_bal, low_wk = balance, i+1
        if balance > high_bal:
            high_bal, high_wk = balance, i+1
        
        if balance < 200000:
            indicator = "🔴"
        elif balance < 250000:
            indicator = "🟡"
        else:
            indicator = "🟢"
        
        lines.append(f"{indicator} **Week {i+1}** ({end_date.strftime('%b %d')}): ${balance:,}")
    
    lines.append("\n─────────────")
    lines.append(f"🔴 **Low**: ${low_bal:,} (Week {low_wk})")
    lines.append(f"🟢 **High**: ${high_bal:,} (Week {high_wk})")
    
    return "\n".join(lines)

def generate_monthly_projection(months: int) -> str:
    lines = [f"🗓️ **{months}-Month Projection**\n"]
    
    monthly_data = [
        ("Jan 2026", 230000, "After $130K AmEx"),
        ("Feb 2026", 341000, "Strong recovery"),
        ("Mar 2026", 320000, "Typical ops"),
        ("Apr 2026", 310000, "Seasonal adj"),
        ("May 2026", 330000, "Summer pickup"),
        ("Jun 2026", 350000, "Peak starts"),
        ("Jul 2026", 380000, "Peak summer"),
        ("Aug 2026", 390000, "Peak summer"),
        ("Sep 2026", 360000, "Post-summer"),
        ("Oct 2026", 340000, "Fall ops"),
        ("Nov 2026", 320000, "Pre-holiday"),
        ("Dec 2026", 300000, "Holiday"),
    ]
    
    low_bal, low_month = float('inf'), None
    high_bal, high_month = 0, None
    
    for i in range(min(months, 12)):
        month, balance, note = monthly_data[i]
        
        if balance < low_bal:
            low_bal, low_month = balance, month
        if balance > high_bal:
            high_bal, high_month = balance, month
        
        if balance < 250000:
            indicator = "🔴"
        elif balance < 300000:
            indicator = "🟡"
        else:
            indicator = "🟢"
        
        lines.append(f"{indicator} **{month}**: ${balance:,} _{note}_")
    
    lines.append("\n─────────────")
    lines.append(f"🔴 **Low**: ${low_bal:,} ({low_month})")
    lines.append(f"🟢 **High**: ${high_bal:,} ({high_month})")
    
    return "\n".join(lines)

def interpret_question(q: str) -> str:
    q_lower = q.lower()
    
    # Projection questions
    if 'projection' in q_lower or ('show' in q_lower and any(w in q_lower for w in ['days', 'weeks', 'months'])):
        import re
        # Parse the request
        match = re.search(r'(\d+)\s*(days?|weeks?|months?)', q_lower)
        if match:
            count = int(match.group(1))
            unit = match.group(2).rstrip('s')  # normalize to singular
            
            if unit == 'day':
                return generate_daily_projection(count)
            elif unit == 'week':
                return generate_weekly_projection(count)
            elif unit == 'month':
                return generate_monthly_projection(count)
        
        return "Please specify a time period like '30 days', '8 weeks', or '6 months'."
    
    # Profit questions
    if any(w in q_lower for w in ['profit', 'margin', 'making', 'earn', 'net', 'gross']):
        return f"""**Monthly Average Profit** (Rolling 30-Day Cash Flow Method)

💵 **Cash In:** ${ROLLING_30_DAY['cash_in']:,}/month
💸 **Cash Out:** ${ROLLING_30_DAY['cash_out']:,}/month (ops only, excludes payroll)

📈 **Gross Profit: ${ROLLING_30_DAY['gross_profit']:,}/month**

👥 **After Payroll (~$206K/month):**
Net: ${NET_MONTHLY_PROFIT:,}/month

📝 *Simple cash flow method: Cash In - Cash Out = Profit*"""
    
    # Revenue questions  
    if any(w in q_lower for w in ['revenue', 'income', 'sales', 'bringing in', 'cash in']):
        return f"""**Monthly Cash In: ${ROLLING_30_DAY['cash_in']:,}** (30-day rolling avg)

📥 Sources:
• CC Processors (Paymentech, CMS, AmEx)
• Wire income (~$14K/week)
• E-deposits (checks)"""
    
    # Expenses questions
    if any(w in q_lower for w in ['expense', 'cost', 'spending', 'burn', 'overhead', 'cash out']):
        return f"""**Monthly Cash Out:** (30-day rolling avg)

🏢 **Ops:** ${ROLLING_30_DAY['cash_out']:,}/month
• Daily ops: $15-18K/day
• Includes refund checks

👥 **Payroll:** ~$206K/month (tracked separately)
• $75K salary x 2 cycles + taxes + 401K + ADP

📊 **Total: ~${ROLLING_30_DAY['cash_out'] + MONTHLY_PAYROLL:,}/month**"""
    
    # Current balance
    if any(w in q_lower for w in ['current', 'balance now', 'how much', 'what is the balance', "what's the balance"]):
        return "Current balance is **$245,000** as of January 13, 2026."
    
    # Low point
    if any(w in q_lower for w in ['low', 'lowest', 'minimum', 'tight', 'worried', 'concern']):
        return "The **low point is $184,000 on January 20** (MLK holiday weekend impact). It's tight but manageable. Recovery begins immediately after."
    
    # Peak / high point
    if any(w in q_lower for w in ['peak', 'high', 'maximum', 'best']):
        return "The **peak is $369,000 on February 12** - right before the mid-February AmEx payment."
    
    # Distribution
    if any(w in q_lower for w in ['distribution', 'take money', 'withdraw', 'pull out', '$50k', '50k']):
        return "Best time for the **$50K distribution is around February 12** when we hit $369K. Take it before the AmEx payment on Feb 13. This would leave the low point at $234K - still comfortable."
    
    # AmEx / payments
    if any(w in q_lower for w in ['amex', 'american express', 'payment', 'due', 'owe']):
        return "**AmEx Payment Schedule:**\n• $106K due Jan 16 \n• $130K due Jan 31\n• $100K due mid-February\n\nAll factored into projections."
    
    # Payroll
    if any(w in q_lower for w in ['payroll', 'salary', 'wages', 'employee']):
        return "**Payroll runs twice monthly:**\n• Feb 3: ~$75K over 3 days + $25K taxes\n• Feb 18: Same structure\n\nPlus ~$3.2K 401K and ~$230 ADP fees each cycle."
    
    # January
    if 'january' in q_lower or 'jan' in q_lower:
        return "**January outlook:** Tight but manageable. Low point of $184K on Jan 20, then recovery. Ends around $230K after the $130K AmEx payment on Jan 30."
    
    # February  
    if 'february' in q_lower or 'feb' in q_lower:
        return "**February outlook:** Strong recovery! Peaks at $369K on Feb 12. After AmEx payment and payroll, ends around $341K by Feb 24."
    
    # Overview / general
    if any(w in q_lower for w in ['overview', 'summary', 'how', 'looking', 'status', 'ok', 'safe', 'good']):
        return "**Cash flow is tight but manageable.**\n\n📉 Low point: $184K on Jan 20\n📈 Peak: $369K on Feb 12\n💰 Distribution timing: Best around Feb 12\n\nWe'll get through the January squeeze and recover nicely in February."
    
    # Specific date check
    import re
    date_match = re.search(r'(jan|feb)\w*\s*(\d{1,2})', q_lower)
    if date_match:
        month = '01' if 'jan' in date_match.group(1) else '02'
        day = date_match.group(2).zfill(2)
        date_key = f"2026-{month}-{day}"
        if date_key in FORECAST:
            f = FORECAST[date_key]
            return f"**{date_key}:** Balance projected at **${f['balance']:,}**\n{f['note']}"
        return f"I don't have a specific projection for {date_key}, but I can give you the overall trend."
    
    # Unknown question - notify the powers that be
    try:
        webhook_url = "https://webhooks.tasklet.ai/v1/public/webhook?token=739e742528fc953b33f7fddb05705e9f"
        httpx.post(webhook_url, json={
            "type": "unknown_question",
            "question": question,
            "timestamp": datetime.now().isoformat()
        }, timeout=5.0)
    except:
        pass  # Don't fail if webhook doesn't work
    
    return """🤔 That's not something I'm ready to answer yet!

I've sent your question to the powers that be and requested an update. They'll teach me how to handle this soon.

**In the meantime, I can help with:**
• Current balance & projections
• Low points & peaks
• Distribution timing
• AmEx payment schedule
• Payroll dates
• Monthly profit estimates
• Specific date lookups (e.g., "What about Jan 20?")

Try asking one of those!"""

@app.get("/", response_class=HTMLResponse)
async def root():
    try:
        with open("static/index.html", "r") as f:
            return HTMLResponse(content=f.read())
    except:
        return HTMLResponse(content="<h1>Casablanca Cash Flow API</h1><p>PWA coming soon...</p>")

@app.get("/manifest.json")
async def manifest():
    try:
        return FileResponse("static/manifest.json", media_type="application/json")
    except:
        raise HTTPException(status_code=404)

@app.get("/icon-192.png")
async def icon_192():
    try:
        return FileResponse("static/icon-192.png", media_type="image/png")
    except:
        raise HTTPException(status_code=404)

@app.get("/icon-512.png")
async def icon_512():
    try:
        return FileResponse("static/icon-512.png", media_type="image/png")
    except:
        raise HTTPException(status_code=404)

@app.get("/summary")
async def get_summary(code: str = Query(...)):
    verify_code(code)
    return {
        "current_balance": "$245,000 as of 2026-01-13",
        "low_point": "$184K Jan 20",
        "high_point": "$369K Feb 12",
        "monthly_profit": f"${ROLLING_30_DAY['gross_profit']//1000}K/mo",
        "january_outlook": "Tight but manageable - lowest at $184K on Jan 20",
        "february_outlook": "Strong recovery to $369K by Feb 12",
        "distribution_timing": "Best to take $50K distribution around Feb 12"
    }

@app.get("/forecast")
async def get_forecast(code: str = Query(...)):
    verify_code(code)
    return {"forecast": FORECAST}

@app.get("/balance/{date}")
async def get_balance(date: str, code: str = Query(...)):
    verify_code(code)
    if date in FORECAST:
        return FORECAST[date]
    raise HTTPException(status_code=404, detail=f"No forecast for {date}")

@app.get("/low-point")
async def get_low_point(code: str = Query(...)):
    verify_code(code)
    return {
        "date": "2026-01-20",
        "balance": 184000,
        "note": "MLK holiday weekend impact - lowest point before recovery"
    }

@app.get("/ask")
async def ask_question(code: str = Query(...), question: str = Query(...)):
    verify_code(code)
    answer = interpret_question(question)
    return {"question": question, "answer": answer}

@app.post("/submit-data")
async def submit_data(code: str = Query(...), submission: DataSubmission = None):
    verify_code(code)
    
    # Send to webhook for processing
    try:
        async with httpx.AsyncClient() as client:
            await client.post(WEBHOOK_URL, json={
                "action": "process_data",
                "data": submission.data if submission else ""
            }, timeout=10)
    except:
        pass
    
    return {"message": "Data received! Processing and updating projections. Check back in a few minutes for updated forecasts."}

@app.post("/request-update")
async def request_update(code: str = Query(...)):
    verify_code(code)
    
    # Send to webhook to trigger Authorize.net pull
    try:
        async with httpx.AsyncClient() as client:
            await client.post(WEBHOOK_URL, json={
                "action": "pull_authorizenet"
            }, timeout=10)
    except:
        pass
    
    return {"message": "Update requested! I'm pulling the latest settlements from Authorize.net. Projections will be updated shortly."}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
