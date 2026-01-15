# Casablanca Cash Flow API

Simple API for querying cash flow projections.

## Endpoints

- `GET /` - Health check
- `GET /forecast` - Full forecast data
- `GET /balance/{date}` - Get projected balance for a date (e.g., `/balance/jan20`)
- `GET /low-point` - Get the low point projection
- `POST /ask` - Ask a question about cash flow

## Example Questions

```bash
curl -X POST https://your-app.railway.app/ask \
  -H "Content-Type: application/json" \
  -d '{"question": "What is the current balance?"}'
```

## Deploy to Railway

[![Deploy on Railway](https://railway.app/button.svg)](https://railway.app/template)

1. Connect this repo to Railway
2. Railway auto-detects Python and deploys
