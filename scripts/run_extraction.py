#!/usr/bin/env python3
"""
Extract structured data from markdown reports using Gemini CLI.
Stores results in SQLite database.
"""
import json
import os
import re
import subprocess
import sys
import sqlite3
from datetime import date

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from db import get_db
from extractor import build_extraction_prompt, parse_extraction_result, validate_extraction

REPORTS_DIR = os.path.join(SCRIPT_DIR, '..', 'reports')

EXTRACTION_PROMPT = """You are a financial research analyst. Extract ALL structured data from this research report.

Report Source: {source}
Report Type: {report_type}
Date: {report_date}

---BEGIN REPORT---
{report_text}
---END REPORT---

Extract the following and respond ONLY with valid JSON (no markdown, no explanation):

{{
    "report_metadata": {{
        "source": "{source}",
        "report_type": "{report_type}",
        "date": "{report_date}",
        "title": "<report title>",
        "overall_sentiment": "<bullish|bearish|neutral|cautiously_bullish|cautiously_bearish>",
        "market_summary": "<2-3 sentence summary>"
    }},
    "stock_actions": [
        {{
            "ticker": "<stock ticker>",
            "company_name": "<full company name>",
            "action": "<upgrade|downgrade|initiate|reiterate|maintain|new_coverage>",
            "rating": "<strong_buy|buy|hold|sell|strong_sell>",
            "previous_rating": "<if mentioned, otherwise null>",
            "price_target": <number or null>,
            "previous_price_target": <number or null>,
            "sector": "<sector if mentioned>",
            "summary": "<1-2 sentence reason>"
        }}
    ],
    "sector_commentary": [
        {{
            "sector": "<sector name>",
            "outlook": "<overweight|underweight|neutral|market_weight>",
            "summary": "<brief outlook>"
        }}
    ],
    "key_themes": ["<theme1>", "<theme2>"]
}}

Rules:
- Extract EVERY stock with a rating, price target, or action
- Use null for unknown fields
- Tickers uppercase, 1-5 letters
- Price targets are numbers only
- If no stock actions, return empty array"""


def _truncate_at_sentence(text, max_chars):
    """Truncate text at a sentence boundary near max_chars."""
    if len(text) <= max_chars:
        return text
    truncated = text[:max_chars]
    last_period = max(truncated.rfind('. '), truncated.rfind('.\n'))
    last_excl = max(truncated.rfind('! '), truncated.rfind('!\n'))
    last_q = max(truncated.rfind('? '), truncated.rfind('?\n'))
    cut_point = max(last_period, last_excl, last_q)
    if cut_point > max_chars * 0.7:
        return truncated[:cut_point + 1]
    last_para = truncated.rfind('\n\n')
    if last_para > max_chars * 0.7:
        return truncated[:last_para]
    return truncated


def extract_with_gemini(markdown_text, source, report_type, report_date):
    """Call Gemini CLI to extract structured data from markdown."""
    # Use sentence-aware truncation instead of hard limit
    truncated = _truncate_at_sentence(markdown_text, 50000)
    
    prompt = EXTRACTION_PROMPT.format(
        source=source,
        report_type=report_type,
        report_date=report_date,
        report_text=truncated
    )
    
    # Use gemini CLI
    result = subprocess.run(
        ['gemini', '-p', prompt],
        capture_output=True, text=True, timeout=120
    )
    
    if result.returncode != 0:
        print(f"  ✗ Gemini CLI error: {result.stderr[:200]}")
        return None
    
    return parse_extraction_result(result.stdout)


def extract_report(md_path, source, report_type, report_date):
    """Extract structured data from a single markdown report."""
    with open(md_path, 'r') as f:
        md_text = f.read()
    
    print(f"  Extracting from {os.path.basename(md_path)} ({len(md_text):,} chars)...")
    
    data = extract_with_gemini(md_text, source, report_type, report_date)
    
    if data is None:
        print(f"  ✗ Failed to parse extraction result")
        return None
    
    issues = validate_extraction(data)
    if issues:
        print(f"  ⚠ Validation issues: {', '.join(issues)}")
    
    return data


def store_extraction(conn, data, source, report_date):
    """Store extracted data in SQLite."""
    meta = data.get('report_metadata', {})
    report_type = meta.get('report_type', 'unknown')
    report_title = meta.get('title', '')
    
    # Generate report_id
    import uuid
    report_id = str(uuid.uuid4())
    
    # Store report
    conn.execute("""
        INSERT INTO reports (id, source, date, report_type, title, filename, content_hash)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (report_id, source, report_date, report_type, report_title, '', ''))
    
    # Store stock actions
    for action in data.get('stock_actions', []):
        action_id = str(uuid.uuid4())
        conn.execute("""
            INSERT INTO stock_actions 
            (id, source, report_id, date, ticker, company_name, action, rating, 
             previous_rating, price_target, previous_price_target, sector, summary)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            action_id, source, report_id, report_date,
            action.get('ticker', ''),
            action.get('company_name', ''),
            action.get('action', ''),
            action.get('rating', ''),
            action.get('previous_rating'),
            action.get('price_target'),
            action.get('previous_price_target'),
            action.get('sector', ''),
            action.get('summary', '')
        ))
    
    # Store sector commentary
    for sector in data.get('sector_commentary', []):
        sector_id = str(uuid.uuid4())
        conn.execute("""
            INSERT INTO sector_commentary
            (id, report_id, date, source, sector, outlook, summary)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            sector_id, report_id, report_date, source,
            sector.get('sector', ''),
            sector.get('outlook', ''),
            sector.get('summary', '')
        ))
    
    # Store sentiment (one per source per day)
    sentiment_id = f"{source}-{report_date}-sentiment"
    themes = json.dumps(data.get('key_themes', []))
    conn.execute("""
        INSERT OR REPLACE INTO market_sentiment
        (id, date, source, overall_sentiment, summary, key_themes)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        sentiment_id, report_date, source,
        meta.get('overall_sentiment', 'neutral'),
        meta.get('market_summary', ''),
        themes
    ))
    
    conn.commit()
    return report_id


def main():
    """Run extraction on all downloaded reports."""
    report_date = date.today().strftime("%Y-%m-%d")
    
    # Initialize database
    conn = get_db()
    
    # Argus reports
    argus_dir = os.path.join(REPORTS_DIR, 'argus')
    argus_reports = [
        ('market_digest.md', 'market_digest'),
        ('market_watch.md', 'market_watch'),
        ('market_movers.md', 'market_movers'),
    ]
    
    print("=== ARGUS EXTRACTION ===")
    for filename, report_type in argus_reports:
        md_path = os.path.join(argus_dir, filename)
        if os.path.exists(md_path):
            data = extract_report(md_path, 'argus', report_type, report_date)
            if data:
                store_extraction(conn, data, 'argus', report_date)
                actions = len(data.get('stock_actions', []))
                print(f"  ✓ Extracted {actions} stock actions")
        else:
            print(f"  ⚠ {filename} not found")
    
    # Zacks reports
    zacks_dir = os.path.join(REPORTS_DIR, 'zacks')
    zacks_reports = [
        ('economic_outlook.md', 'economic_outlook'),
        ('focus_list.md', 'focus_list'),
        ('industry_outlook.md', 'industry_outlook'),
    ]
    
    print("\n=== ZACKS EXTRACTION ===")
    for filename, report_type in zacks_reports:
        md_path = os.path.join(zacks_dir, filename)
        if os.path.exists(md_path):
            data = extract_report(md_path, 'zacks', report_type, report_date)
            if data:
                store_extraction(conn, data, 'zacks', report_date)
                actions = len(data.get('stock_actions', []))
                print(f"  ✓ Extracted {actions} stock actions")
        else:
            print(f"  ⚠ {filename} not found")
    
    # Show summary stats
    print("\n=== DATABASE SUMMARY ===")
    total_actions = conn.execute("SELECT COUNT(*) FROM stock_actions WHERE date = ?", (report_date,)).fetchone()[0]
    total_sectors = conn.execute("SELECT COUNT(*) FROM sector_commentary WHERE date = ?", (report_date,)).fetchone()[0]
    total_sentiment = conn.execute("SELECT COUNT(*) FROM market_sentiment WHERE date = ?", (report_date,)).fetchone()[0]
    
    print(f"  Stock actions: {total_actions}")
    print(f"  Sector commentaries: {total_sectors}")
    print(f"  Sentiment records: {total_sentiment}")
    
    # Show all extracted stocks
    stocks = conn.execute("""
        SELECT ticker, company_name, action, rating, price_target, source
        FROM stock_actions WHERE date = ?
        ORDER BY source, ticker
    """, (report_date,)).fetchall()
    
    if stocks:
        print(f"\n=== EXTRACTED STOCKS ===")
        for s in stocks:
            pt = f"${s['price_target']:.0f}" if s['price_target'] else "N/A"
            print(f"  {s['ticker']:6s} | {s['rating'] or 'N/A':12s} | PT: {pt:8s} | {s['source']} | {s['company_name']}")
    
    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
