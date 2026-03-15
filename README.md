# Fidelity Research Automation

Automated daily fetching, processing, and summarization of Argus Research and Zacks Investment Research reports from Fidelity's research portal.

## Sources

### Argus Research
- **Market Digest** (daily) — Analyst Notes, rating changes, price target updates
- **Market Watch** (daily) — Market commentary + stock idea
- **Market Movers** (daily) — Technical picks for next month
- **Market Update** (daily, midday) — Updated Analyst Notes
- **Portfolio Selector** (monthly) — Focus List + sector recommendations
- **Special Situations** (periodic) — Investment themes
- **Sector Watch** (monthly) — 11 sector ratings
- **Weekly Staff Report** (weekly) — Market/economic/technical forecasts
- **Weekly Options Watch** (weekly) — Options strategies
- **Viewpoint** (monthly) — U.S. economy forecasts
- **Economy at a Glance** (bi-weekly) — Chart-based trends
- **Fixed Income Strategy** (monthly) — Interest rate forecasts

### Zacks Investment Research
- **Economic Outlook** (monthly) — Macro forecast by John Blank PhD
- **Market Strategy** (monthly) — Asset class forecasts
- **Focus List** (weekly) — Model portfolio changes
- **Bull & Bear of the Daily** (daily) — 2 stock picks
- **Industry Outlook** (daily) — Industry commentary + stock highlights
- **Stock Research Reports** (ad-hoc) — Individual stock deep-dives

## Architecture

```
Browser (Chrome relay, logged into Fidelity)
    ↓ fetch PDFs via JavaScript
downloads/{source}/YYYY-MM-DD/*.pdf
    ↓ PyMuPDF extraction
reports/{source}/YYYY-MM-DD/*.md
    ↓ AI extraction (Gemini CLI)
data/research.db (SQLite — structured stock actions, ratings, sectors)
data/chroma/ (vector DB — semantic search)
    ↓ AI summary (Gemini CLI)
Telegram message to Todd
```

## Setup

```bash
pip install pymupdf chromadb
```

## Usage

### Manual run
```bash
python scripts/orchestrator.py --source argus --date today
python scripts/orchestrator.py --source zacks --date today
```

### Full pipeline (called by cron)
```bash
python scripts/orchestrator.py --full-pipeline
```

### Query the database
```bash
python scripts/query.py --upgrades --week
python scripts/query.py --ticker NVDA --days 30
python scripts/query.py --sector Technology --month
```

## Database Schema

See `scripts/db.py` for full schema. Key tables:
- `reports` — Report metadata + content hashes
- `stock_actions` — Individual stock ratings, upgrades/downgrades, price targets
- `sector_commentary` — Sector-level outlook and themes
- `bull_bear_daily` — Zacks daily Bull & Bear picks
- `focus_list_changes` — Zacks Focus List additions/removals
- `industry_outlook` — Zacks industry commentary
- `market_sentiment` — Daily overall market sentiment

## File Retention
- Raw PDFs: 7 days
- Markdown: indefinitely
- SQLite + ChromaDB: indefinitely

## Cron Schedule
- 6:30 AM PT: First download attempt
- 7:00 AM PT: Retry for late reports
- 7:30 AM PT: Final retry + generate summary + send Telegram message
- Monthly reports: checked daily, deduped by content hash
