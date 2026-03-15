"""
Fidelity Research Database — Schema and helpers.

Uses SQLite for structured data (stock actions, ratings, sectors).
"""

import sqlite3
import json
import os
from datetime import datetime, date
from pathlib import Path

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.join(SCRIPT_DIR, '..')
CONFIG_PATH = os.path.join(PROJECT_DIR, 'config.json')


def load_config():
    """Load project configuration from config.json."""
    with open(CONFIG_PATH) as f:
        return json.load(f)


SCHEMA_SQL = """
-- Schema versioning
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TEXT DEFAULT (datetime('now')),
    description TEXT
);

-- Report metadata
CREATE TABLE IF NOT EXISTS reports (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    date TEXT NOT NULL,
    report_type TEXT NOT NULL,
    title TEXT,
    filename TEXT,
    content_hash TEXT NOT NULL,
    pages INTEGER,
    fetched_at TEXT DEFAULT (datetime('now')),
    metadata TEXT,
    UNIQUE(source, date, report_type)
);

-- Stock actions (upgrades, downgrades, price target changes)
CREATE TABLE IF NOT EXISTS stock_actions (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    report_id TEXT REFERENCES reports(id),
    date TEXT NOT NULL,
    ticker TEXT NOT NULL,
    company_name TEXT,
    action TEXT,
    rating TEXT,
    previous_rating TEXT,
    price_target REAL,
    previous_price_target REAL,
    sector TEXT,
    industry TEXT,
    summary TEXT,
    zacks_rank INTEGER,
    previous_zacks_rank INTEGER,
    zacks_recommendation TEXT,
    style_score_value TEXT,
    style_score_growth TEXT,
    style_score_momentum TEXT,
    analyst_name TEXT,
    conviction TEXT,
    metadata TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_actions_ticker ON stock_actions(ticker);
CREATE INDEX IF NOT EXISTS idx_actions_date ON stock_actions(date);
CREATE INDEX IF NOT EXISTS idx_actions_source ON stock_actions(source);
CREATE INDEX IF NOT EXISTS idx_actions_rating ON stock_actions(rating);
CREATE INDEX IF NOT EXISTS idx_actions_zacks_rank ON stock_actions(zacks_rank);

-- Sector commentary
CREATE TABLE IF NOT EXISTS sector_commentary (
    id TEXT PRIMARY KEY,
    report_id TEXT REFERENCES reports(id),
    date TEXT NOT NULL,
    source TEXT NOT NULL,
    sector TEXT NOT NULL,
    outlook TEXT,
    summary TEXT,
    key_themes TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Zacks Bull & Bear daily picks
CREATE TABLE IF NOT EXISTS bull_bear_daily (
    id TEXT PRIMARY KEY,
    date TEXT NOT NULL UNIQUE,
    bull_ticker TEXT,
    bull_company TEXT,
    bull_zacks_rank INTEGER,
    bull_summary TEXT,
    bear_ticker TEXT,
    bear_company TEXT,
    bear_zacks_rank INTEGER,
    bear_summary TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Zacks Focus List tracking
CREATE TABLE IF NOT EXISTS focus_list_changes (
    id TEXT PRIMARY KEY,
    date TEXT NOT NULL,
    action TEXT NOT NULL,
    ticker TEXT NOT NULL,
    company_name TEXT,
    zacks_rank INTEGER,
    summary TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS focus_list_current (
    ticker TEXT PRIMARY KEY,
    company_name TEXT,
    zacks_rank INTEGER,
    added_date TEXT,
    last_confirmed TEXT,
    metadata TEXT
);

-- Industry outlook
CREATE TABLE IF NOT EXISTS industry_outlook (
    id TEXT PRIMARY KEY,
    report_id TEXT REFERENCES reports(id),
    date TEXT NOT NULL,
    source TEXT NOT NULL,
    industry TEXT NOT NULL,
    outlook TEXT,
    summary TEXT,
    tickers_mentioned TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Market sentiment
CREATE TABLE IF NOT EXISTS market_sentiment (
    id TEXT PRIMARY KEY,
    date TEXT NOT NULL,
    source TEXT NOT NULL,
    overall_sentiment TEXT,
    summary TEXT,
    key_themes TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(date, source)
);

-- Download log (for dedup and retry tracking)
CREATE TABLE IF NOT EXISTS download_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    report_type TEXT NOT NULL,
    date TEXT NOT NULL,
    url TEXT,
    content_hash TEXT,
    status TEXT NOT NULL,
    error TEXT,
    downloaded_at TEXT DEFAULT (datetime('now'))
);
"""


def get_db(db_path=None):
    """Get a database connection, creating schema if needed."""
    if db_path is None:
        try:
            config = load_config()
            db_rel = config.get('paths', {}).get('db', 'data/research.db')
            db_path = os.path.join(PROJECT_DIR, db_rel)
        except (FileNotFoundError, json.JSONDecodeError):
            db_path = os.path.join(PROJECT_DIR, 'data', 'research.db')
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    # Check schema version
    try:
        version = conn.execute(
            "SELECT MAX(version) FROM schema_version"
        ).fetchone()[0]
    except sqlite3.OperationalError:
        version = None

    if version is None:
        conn.executescript(SCHEMA_SQL)
        conn.execute(
            "INSERT INTO schema_version (version, description) VALUES (?, ?)",
            (1, "Initial schema — Argus + Zacks unified")
        )
        conn.commit()

    return conn


def store_report(conn, source, date_str, report_type, title, filename,
                 content_hash, pages=None, metadata=None):
    """Store a report record. Returns report ID."""
    report_id = f"{source}-{date_str}-{report_type}"
    conn.execute("""
        INSERT OR REPLACE INTO reports
            (id, source, date, report_type, title, filename, content_hash, pages, metadata)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (report_id, source, date_str, report_type, title, filename,
          content_hash, pages, json.dumps(metadata) if metadata else None))
    conn.commit()
    return report_id


def store_stock_action(conn, source, report_id, date_str, ticker, **kwargs):
    """Store a stock action (upgrade, downgrade, etc.)."""
    action_id = f"{source}-{date_str}-{ticker}-{kwargs.get('action', 'unknown')}-{report_id}"
    cols = ['company_name', 'action', 'rating', 'previous_rating',
            'price_target', 'previous_price_target', 'sector', 'industry',
            'summary', 'zacks_rank', 'previous_zacks_rank',
            'zacks_recommendation', 'style_score_value', 'style_score_growth',
            'style_score_momentum', 'analyst_name', 'conviction', 'metadata']
    values = [action_id, source, report_id, date_str, ticker]
    placeholders = "?, ?, ?, ?, ?"

    for col in cols:
        val = kwargs.get(col)
        if col == 'metadata' and val is not None:
            val = json.dumps(val)
        placeholders += ", ?"
        values.append(val)

    conn.execute(f"""
        INSERT OR REPLACE INTO stock_actions
            (id, source, report_id, date, ticker, {', '.join(cols)})
        VALUES ({placeholders})
    """, values)
    conn.commit()
    return action_id


def log_download(conn, source, report_type, date_str, url, content_hash,
                 status, error=None):
    """Log a download attempt."""
    conn.execute("""
        INSERT INTO download_log (source, report_type, date, url, content_hash, status, error)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (source, report_type, date_str, url, content_hash, status, error))
    conn.commit()


def is_already_downloaded(conn, source, report_type, date_str, content_hash=None):
    """Check if a report was already successfully downloaded."""
    if content_hash:
        row = conn.execute("""
            SELECT 1 FROM reports
            WHERE source = ? AND report_type = ? AND date = ? AND content_hash = ?
        """, (source, report_type, date_str, content_hash)).fetchone()
    else:
        row = conn.execute("""
            SELECT 1 FROM reports
            WHERE source = ? AND report_type = ? AND date = ?
        """, (source, report_type, date_str)).fetchone()
    return row is not None


def get_content_hash_for_type(conn, source, report_type, date_str):
    """Get the last known content hash for a report type (for dedup)."""
    row = conn.execute("""
        SELECT content_hash FROM reports
        WHERE source = ? AND report_type = ?
        ORDER BY date DESC LIMIT 1
    """, (source, report_type)).fetchone()
    return row['content_hash'] if row else None
