#!/usr/bin/env python3
"""Extract structured data from markdown reports using Gemini CLI via stdin."""
import json, os, re, subprocess, sys
from datetime import date
from db import get_db

REPORTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'reports')

ARGUS_EXTRACTION = [
    ('market_digest.md', 'market_digest'),
    ('market_watch.md', 'market_watch'),
    ('market_movers.md', 'market_movers'),
]
ZACKS_EXTRACTION = [
    ('economic_outlook.md', 'economic_outlook'),
    ('focus_list.md', 'focus_list'),
    ('industry_outlook.md', 'industry_outlook'),
]

def extract_with_gemini(markdown_text, source, report_type, report_date):
    """Extract structured data using Gemini CLI via stdin."""
    truncated = markdown_text[:30000]
    prompt = f"""You are a financial research analyst. Extract ALL structured data from this research report.

Report Source: {source}
Report Type: {report_type}
Date: {report_date}

---BEGIN REPORT---
{truncated}
---END REPORT---

Extract stock actions, sector commentary, market sentiment, and key themes.
- Extract EVERY stock with a rating, price target, or action
- Use null for unknown fields
- If no stock actions, return empty array
- Tickers uppercase, 1-5 letters
- Respond with ONLY valid JSON:
{{"report_metadata": {{"source":"...","report_type":"...","date":"...","title":"...","overall_sentiment":"bullish|bearish|neutral|cautiously_bullish|cautiously_bearish","market_summary":"..."}}, "stock_actions": [{{"ticker":"...","company_name":"...","action":"upgrade|downgrade|initiate|reiterate|maintain|new_coverage","rating":"strong_buy|buy|hold|sell|strong_sell","previous_rating":null,"price_target":null,"previous_price_target":null,"sector":"...","summary":"..."}}], "sector_commentary": [{{"sector":"...","outlook":"overweight|underweight|neutral|market_weight","summary":"..."}}], "key_themes": ["..."]}}"""
    try:
        result = subprocess.run(['gemini', '-p', 'Extract structured JSON from the research report I will paste. Respond ONLY with valid JSON.'],
            input=prompt, capture_output=True, text=True, timeout=180)
        if result.returncode != 0:
            print(f"  ✗ Gemini CLI error: {result.stderr[:200]}")
            return None
        text = result.stdout.strip()
        if text.startswith('```'):
            text = re.sub(r'^```(?:json)?\s*\n?', '', text)
            text = re.sub(r'\n?```\s*$', '', text)
        return json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r'\{[\s\S]*\}', result.stdout)
        if match:
            try: return json.loads(match.group())
            except: pass
        print(f"  ✗ JSON parse error")
        return None
    except Exception as e:
        print(f"  ✗ Gemini error: {e}")
        return None

def extract_report(md_path, source, report_type, report_date):
    with open(md_path, 'r') as f:
        md_text = f.read()
    print(f"  Extracting from {os.path.basename(md_path)} ({len(md_text):,} chars)...")
    return extract_with_gemini(md_text, source, report_type, report_date)

def store_extraction(conn, data, source, report_date):
    import uuid
    meta = data.get('report_metadata', {})
    report_id = str(uuid.uuid4())
    conn.execute("INSERT INTO reports (id,source,date,report_type,title,filename,content_hash) VALUES (?,?,?,?,?,?,?)",
        (report_id, source, report_date, meta.get('report_type','unknown'), meta.get('title',''), '', ''))
    for a in data.get('stock_actions', []):
        conn.execute("INSERT INTO stock_actions (id,source,report_id,date,ticker,company_name,action,rating,previous_rating,price_target,previous_price_target,sector,summary) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (str(uuid.uuid4()), source, report_id, report_date, a.get('ticker',''), a.get('company_name',''), a.get('action',''), a.get('rating',''), a.get('previous_rating'), a.get('price_target'), a.get('previous_price_target'), a.get('sector',''), a.get('summary','')))
    for s in data.get('sector_commentary', []):
        conn.execute("INSERT INTO sector_commentary (id,report_id,date,source,sector,outlook,summary) VALUES (?,?,?,?,?,?,?)",
            (str(uuid.uuid4()), report_id, report_date, source, s.get('sector',''), s.get('outlook',''), s.get('summary','')))
    sid = f"{source}-{report_date}-sentiment"
    conn.execute("INSERT OR REPLACE INTO market_sentiment (id,date,source,overall_sentiment,summary,key_themes) VALUES (?,?,?,?,?,?)",
        (sid, report_date, source, meta.get('overall_sentiment','neutral'), meta.get('market_summary',''), json.dumps(data.get('key_themes',[]))))
    conn.commit()

def run_extraction(report_date=None):
    if report_date is None: report_date = date.today().strftime("%Y-%m-%d")
    conn = get_db(); total = 0
    for source, rdir, configs in [('argus', os.path.join(REPORTS_DIR,'argus'), ARGUS_EXTRACTION), ('zacks', os.path.join(REPORTS_DIR,'zacks'), ZACKS_EXTRACTION)]:
        print(f"=== {source.upper()} EXTRACTION ===")
        for fn, rt in configs:
            p = os.path.join(rdir, fn)
            if os.path.exists(p):
                d = extract_report(p, source, rt, report_date)
                if d: store_extraction(conn, d, source, report_date); total += len(d.get('stock_actions',[])); print(f"  ✓ {len(d.get('stock_actions',[]))} actions")
            else: print(f"  ⚠ {fn} not found")
    print(f"\n=== DONE: {total} total actions ===")
    conn.close()

if __name__ == "__main__":
    run_extraction()
