# Project Documentation

> **Note**: Sensitive tokens and passwords have been redacted from this public version. Get actual credentials from the agent's conversation history or environment variables.: Casablanca Cash Flow & Project-R

## Overview

This documentation covers two cash flow management applications:

1. **Casablanca Cash Flow** - A single-tenant PWA for Casablanca Express travel agency
2. **Project-R** - A multi-tenant universal cash flow app for any business

---

## CASABLANCA CASH FLOW APP

### Purpose
Monitor cash flow and ensure sufficient liquidity for Casablanca Express, a travel agency business.

### Live URL
- **App**: https://web-production-a76db.up.railway.app
- **Access Code**: `cflownk`
- **Login**: Username `casa`, Password `6300`

### GitHub Repository
- **Repo**: https://github.com/nickmc123/cashflow-api
- **Branch**: main

### Railway Project
- **Project Name**: cashflow-api
- **Service**: web

### Tech Stack
- **Backend**: FastAPI (Python)
- **Frontend**: Vanilla HTML/CSS/JavaScript (PWA)
- **Database**: PostgreSQL on Railway
- **Deployment**: Railway (auto-deploys from GitHub)

### Key Features

#### Dashboard
- Current balance (clickable to update)
- Low point with date
- High point with date
- 30-day average profit
- Status indicator (HEALTHY/OK/TIGHT)
- Casablanca Express branding (orange gradient #FFA726 to #FF8A65)

#### Cash Projection
- Single button opens modal with 9 options:
  - Daily: 15, 30, 45 days
  - Weekly: 4, 8, 12 weeks
  - Monthly: 6, 9, 12 months
- Full-screen projection page with categorized cards
- Color-coded amounts (green credits, dark red debits)

#### Payments Screen
- Upcoming scheduled payments
- Color-coded cards by type (AmEx, Payroll, Comms & Execs)

#### Transaction Management
- Paginated transaction history (10 per page)
- Search functionality
- Bulk delete with checkboxes
- Smart data import with duplicate detection

#### Ask a Question
- Natural language chat interface
- Queries answered by AI

### Business Rules

#### Daily Operations ($9,044/day weighted)
- Refund checks under $1,500 only
- Excludes: AmEx payments, Comms & Execs checks, 5-series payroll, ALL ACH

#### Payroll (~$170K/month)
- Twice per month (not bi-weekly), spread over 3 days
- ADP Tax + 401K + Fees: First business day after 1st and 15th (~$25.4K)
- Base payroll (5-series checks): Next business day (~$60K)

#### Recurring Expenses
- Blue Shield (health insurance): ~$12-19K/month, beginning of month
- Comms & Execs: $51K on 1st, $46K on 15th (~$97K/month)
- AmEx: $130K twice per month (~$260K/month total)

#### Deposit Patterns (weighted calculations)
- CC Processors (Auth.net): $15,836/day (~$79K/week)
- E-Deposits (checks): $14,059/day (~$70K/week)
- Wire income: $1,907/day (~$9.5K/week)

### Database Schema (PostgreSQL)
```sql
-- forecast table: stores projected balances
-- bank_transactions table: stores imported transactions
```

### API Endpoints
- `POST /auth/login` - Authentication
- `GET /forecast` - Get cash projections
- `GET /summary` - Dashboard summary data
- `GET /payments` - Scheduled payments
- `GET /transactions` - Transaction history
- `POST /import-data` - Import bank data
- `POST /update-balance` - Update current balance
- `POST /ask` - Natural language questions

### Webhook Trigger
- **ID**: wti_x6gx7ax4z6vwmepgd6th
- **URL**: https://webhooks.tasklet.ai/v1/public/webhook?token=[WEBHOOK_TOKEN_1]
- Receives notifications for data updates and questions

---

## PROJECT-R (UNIVERSAL CASH FLOW APP)

### Purpose
A multi-tenant cash flow management platform for any business, with intelligent categorization, trend analysis, and what-if scenarios.

### Live URL
- **App**: https://web-production-8d237.up.railway.app
- **Demo Account**: `demo@projectr.app` / `demo123`

### GitHub Repository
- **Repo**: https://github.com/nickmc123/project-r
- **Branch**: main
- **IMPORTANT**: Railway serves from root `/static/` directory, NOT `/app/static/`

### Railway Project
- **Project Name**: project-r
- **Service**: web
- **Database**: PostgreSQL (shinkansen.proxy.rlwy.net:35334)
- **DB Credentials**: User `postgres`, Password `[DB_PASSWORD]`, Database `railway`

### Tech Stack
- **Backend**: FastAPI (Python)
- **Frontend**: Vanilla HTML/CSS/JavaScript (PWA)
- **Database**: PostgreSQL on Railway
- **Deployment**: Railway (auto-deploys from GitHub)

### Repository Structure
```
project-r/
├── static/              # ROOT - Railway deployment directory
│   └── index.html       # Main PWA file (UPDATE THIS FOR DEPLOYMENT)
├── app/                 # Backend application
│   ├── main.py          # FastAPI application
│   ├── models.py        # SQLAlchemy models
│   └── services/
│       └── forecast.py  # Forecast calculation logic
├── api/                 # Development copy (NOT used by Railway)
│   └── app/
│       ├── main.py
│       ├── models.py
│       └── services/
│           └── forecast.py
└── requirements.txt
```

**CRITICAL**: To deploy changes:
1. Update files in `/app/` directory (Railway reads from here for backend)
2. Update `/static/index.html` for frontend changes
3. Push to GitHub - Railway auto-deploys

### Key Features

#### User Authentication
- Signup with email/password
- Login with session tokens
- Demo account for testing
- Password reset flow (tokens generated, email not yet configured)

#### Onboarding Flow
1. Company setup (name, website)
2. Auto-fetch branding from website (logo, theme color, business type)
3. Data upload (90 days banking or QuickBooks)
4. Smart parsing with interactive fallback
5. Transaction categorization (auto-grouping by patterns)
6. User review and adjustment
7. Cash flow model generation

#### Website Analyzer
- Logo extraction (apple-touch-icon, OG image, favicons)
- Theme color from meta tags
- Business type detection (Travel, SaaS, Restaurant, Retail, Healthcare, etc.)
- Industry-specific category suggestions

#### Transaction Groups
- Categorize transactions into groups
- Types: Revenue, Expense
- Frequencies: Daily, Weekly, Semi-monthly, Monthly, Quarterly, Uncommon
- Support for offset transactions (chargebacks in revenue groups, rebates in expense groups)

#### Trend Management
- Calculated trends based on historical data
- Adjustable lookback period (weeks or months)
- Set custom trend direction (up/down/flat) and percentage
- Correlations between categories
- Format: "When [category A] goes [up/down], [category B] goes [up/down] by [X%] over [N] [days/weeks/months]"

#### Cash Projections
- Same 9-option modal as Casablanca (Daily 15/30/45, Weekly 4/8/12, Monthly 6/9/12)
- **Calculated vs Adjusted toggle**
- Full-screen projection page
- Categorized transaction cards (Credits, Expenses)
- Low/High point summary boxes
- Color-coded amounts

#### What-If Scenarios
- Natural language questions in chat
- "What if I did another $5K/week in revenue?"
- Shows impact on 30-day cash change and low point

### Database Schema

```sql
-- Users table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR UNIQUE NOT NULL,
    password_hash VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Companies table
CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    name VARCHAR NOT NULL,
    logo_url VARCHAR,
    theme_color VARCHAR,
    business_type VARCHAR,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Transactions table
CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    date DATE NOT NULL,
    amount DECIMAL NOT NULL,
    description VARCHAR,
    category VARCHAR,
    group_id INTEGER REFERENCES transaction_groups(id),
    is_starred BOOLEAN DEFAULT FALSE,
    is_recurring BOOLEAN DEFAULT FALSE,
    offset_for_group_id INTEGER,
    star_note VARCHAR,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Transaction Groups table
CREATE TABLE transaction_groups (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    name VARCHAR NOT NULL,
    type VARCHAR NOT NULL,  -- 'revenue' or 'expense'
    frequency VARCHAR,
    description VARCHAR,
    trend VARCHAR,  -- calculated trend
    trend_percent DECIMAL,
    adjusted_trend_percent DECIMAL,  -- user-set adjustment
    trend_lookback_value INTEGER,
    trend_lookback_unit VARCHAR,  -- 'weeks' or 'months'
    created_at TIMESTAMP DEFAULT NOW()
);

-- Group Correlations table
CREATE TABLE group_correlations (
    id SERIAL PRIMARY KEY,
    source_group_id INTEGER REFERENCES transaction_groups(id),
    target_group_id INTEGER REFERENCES transaction_groups(id),
    source_direction VARCHAR,  -- 'up' or 'down'
    target_direction VARCHAR,
    target_percent DECIMAL,
    delay_value INTEGER,
    delay_unit VARCHAR,  -- 'days', 'weeks', 'months'
    created_at TIMESTAMP DEFAULT NOW()
);

-- Forecasts table
CREATE TABLE forecasts (
    id SERIAL PRIMARY KEY,
    company_id INTEGER REFERENCES companies(id),
    current_balance DECIMAL,
    created_at TIMESTAMP DEFAULT NOW()
);
```

### API Endpoints

#### Authentication
- `POST /auth/signup` - Create new account
- `POST /auth/login` - Login
- `POST /auth/demo-login` - Demo account login
- `POST /auth/forgot-password` - Request password reset
- `POST /auth/reset-password` - Reset password with token

#### Company
- `GET /company` - Get company info
- `PUT /company` - Update company info
- `POST /analyze-website` - Analyze website for branding

#### Transactions
- `GET /transactions` - List transactions
- `POST /transactions` - Create transaction
- `PUT /transactions/{id}` - Update transaction
- `DELETE /transactions/{id}` - Delete transaction
- `POST /transactions/bulk-move` - Move transactions between groups
- `POST /parse-data` - Parse uploaded data

#### Transaction Groups
- `GET /groups` - List groups
- `POST /groups` - Create group
- `PUT /groups/{id}` - Update group
- `DELETE /groups/{id}` - Delete group
- `POST /groups/{id}/correlations` - Add correlation
- `DELETE /correlations/{id}` - Remove correlation

#### Forecasts
- `GET /forecast` - Get projections (params: period=daily/weekly/monthly, count=N, adjusted=true/false)
- `GET /summary` - Dashboard summary
- `POST /update-balance` - Update current balance
- `POST /calculate-trend` - Calculate trend for a group

#### Chat
- `POST /chat` - Natural language questions and what-if scenarios

### Demo Account Data (Acme Coffee Co.)
- 82 transactions over 60 days
- Daily Square deposits (~$2,100-$2,800)
- Weekly wholesale orders (~$7,500-$9,500)
- Semi-monthly payroll (~$12K)
- Monthly rent ($4,500)
- Weekly inventory (~$3,200)
- Monthly utilities (~$850)
- Starting balance: $47,850
- Onboarding already complete

### Webhook Trigger (New Signups)
- **ID**: wti_axkvzgrhbp0rhf7rbp2h
- **URL**: https://webhooks.tasklet.ai/v1/public/webhook?token=[WEBHOOK_TOKEN_2]
- Sends email to nickmc123@gmail.com on new signups

---

## WISHLIST APP (VAPE CARTRIDGE SHOP)

### Live URL
- **App**: https://wishlist-app-production-0e51.up.railway.app
- **Demo Account**: `demo@wishlist.app` / `demo123`

### GitHub Repository
- **Repo**: https://github.com/nickmc123/wishlist-app
- **Branch**: main

### Railway Project
- **Project Name**: illustrious-nature

### Features
- Vape cartridge product catalog
- Categories: 510 Thread, Pod System, Premium
- Wishlist and Favorites
- Premium membership ($9.99/mo)
- Purple theme (#6B46C1)
- PWA installable

---

## EXTERNAL CONNECTIONS

### Gmail (conn_30151wbdacg8fmn2xr4p)
- Email: nick@casablancaexpress.com, NICK@casablancaexpress.com
- Contains Authorize.Net settlement report emails
- **Skill file**: /agent/skills/connections/conn_30151wbdacg8fmn2xr4p/SKILL.md

### Google Drive (conn_dy5j9pd81jftftfrn83d)
- Spreadsheet: "Casablanca Cash Flow Jan-Feb 2026"
- Sheets: Daily Forecast, Weekly Summary

### GitHub API (conn_63bfgk3z4yfqa0mvr30t)
- Direct API connection
- Token: [GITHUB_TOKEN]
- Repositories: cashflow-api, project-r, wishlist-app

### Computer Use (conn_1a2qz61nmtm708yk41qb)
- "Nick Personal" computer
- Used for Railway deployments and manual operations
- Timezone: Pacific (Los Angeles)
- **Skill file**: /agent/skills/connections/conn_1a2qz61nmtm708yk41qb/SKILL.md

---

## DEPLOYMENT WORKFLOW

### For Casablanca (cashflow-api)
1. Edit files in `/agent/home/cashflow-api/`
2. Push to GitHub using API
3. Railway auto-deploys

### For Project-R
1. Edit backend files in `/agent/home/project-r/app/` (main.py, models.py, services/)
2. Edit frontend in `/agent/home/project-r/static/index.html`
3. Push both to GitHub using API
4. Railway auto-deploys from root directory

### GitHub Push Script
```python
import base64
import httpx

token = "[GITHUB_TOKEN]"

# Read local file
with open('/path/to/file', 'r') as f:
    content = f.read()

# Get current SHA
resp = httpx.get(
    f"https://api.github.com/repos/nickmc123/{repo}/contents/{path}",
    headers={"User-Agent": "Tasklet", "Authorization": f"token {token}"}
)
sha = resp.json().get('sha')

# Push update
httpx.put(
    f"https://api.github.com/repos/nickmc123/{repo}/contents/{path}",
    headers={"User-Agent": "Tasklet", "Authorization": f"token {token}"},
    json={
        "message": "Update description",
        "content": base64.b64encode(content.encode()).decode(),
        "sha": sha
    }
)
```

---

## LOCAL FILE LOCATIONS

- `/agent/home/cashflow-api/` - Casablanca API source
- `/agent/home/project-r/` - Project-R source
- `/agent/home/project-r/api/app/` - Project-R backend (development copy)
- `/agent/home/project-r/app/` - Project-R backend (Railway deployment)
- `/agent/home/project-r/static/` - Project-R frontend (Railway deployment)
- `/agent/home/wishlist-app/` - Wishlist app source
- `/agent/home/bank_data.csv` - Bank statement data
- `/agent/home/uploads/` - Uploaded data files

---

## IMPORTANT NOTES

1. **Railway serves Project-R from root directory** - always update `/static/index.html` and `/app/` files
2. **There are TWO copies of backend files** - `/api/app/` (dev) and `/app/` (production). Railway uses `/app/`
3. **Server timezone is Pacific** - all date calculations should account for this
4. **Casablanca first-of-month spikes should be excluded from projections**
5. **Demo accounts have full premium access** for testing purposes
6. **GitHub token** is used for API pushes: [GITHUB_TOKEN]

---

*Last Updated: January 19, 2026*
