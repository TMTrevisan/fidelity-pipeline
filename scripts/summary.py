import logging
from typing import Dict, List, Any, Optional
"""
Daily summary generator — creates Telegram-friendly briefing from extracted data.

Reads from SQLite database, formats a concise summary message.
"""

import sqlite3
import json
from datetime import date, timedelta


SUMMARY_TEMPLATE = """📈 *{source} Daily Briefing* — {date}

{stock_section}

{sector_section}

{sentiment_section}

{monthly_section}"""


def generate_daily_summary(conn: sqlite3.Connection, source: str, date_str: Optional[str] = None) -> str:
    """
    Generate a daily summary from the database.

    Returns formatted string ready for Telegram.
    """
    if date_str is None:
        date_str = date.today().strftime("%Y-%m-%d")

    # Get stock actions for today
    actions = conn.execute("""
        SELECT ticker, company_name, action, rating, previous_rating,
               price_target, previous_price_target, sector, summary
        FROM stock_actions
        WHERE source = ? AND date = ?
        ORDER BY
            CASE action
                WHEN 'upgrade' THEN 1
                WHEN 'downgrade' THEN 2
                WHEN 'initiate' THEN 3
                WHEN 'new_coverage' THEN 4
                ELSE 5
            END,
            ticker
    """, (source, date_str)).fetchall()

    # Get sector commentary
    sectors = conn.execute("""
        SELECT sector, outlook, summary
        FROM sector_commentary
        WHERE source = ? AND date = ?
        ORDER BY sector
    """, (source, date_str)).fetchall()

    # Get market sentiment from reports.metadata
    sentiment_row = conn.execute("""
        SELECT metadata
        FROM reports
        WHERE source = ? AND date = ? AND metadata IS NOT NULL
        LIMIT 1
    """, (source, date_str)).fetchone()
    
    sentiment = None
    if sentiment_row and sentiment_row['metadata']:
        try:
            import json
            meta = json.loads(sentiment_row['metadata'])
            sentiment = {
                'overall_sentiment': meta.get('overall_sentiment'),
                'summary': meta.get('market_summary'),
                'key_themes': json.dumps(meta.get('key_themes', []))
            }
        except (json.JSONDecodeError, TypeError):
            sentiment = None

    # Get today's new reports
    reports = conn.execute("""
        SELECT report_type, title FROM reports
        WHERE source = ? AND date = ?
        ORDER BY report_type
    """, (source, date_str)).fetchall()

    # Build sections
    stock_section = _format_stock_actions(actions)
    sector_section = _format_sector_commentary(sectors)
    sentiment_section = _format_sentiment(sentiment)
    monthly_section = _format_monthly_updates(conn, source, date_str)

    source_name = "Argus" if source == "argus" else "Zacks"

    return SUMMARY_TEMPLATE.format(
        source=source_name,
        date=date_str,
        stock_section=stock_section,
        sector_section=sector_section,
        sentiment_section=sentiment_section,
        monthly_section=monthly_section
    ).strip()


def _format_stock_actions(actions: List[Dict[str, Any]]) -> str:
    """Format stock actions into Telegram-friendly sections."""
    if not actions:
        return "📋 *No stock actions today*"

    upgrades = []
    downgrades = []
    initiations = []
    other = []

    for a in actions:
        ticker = a['ticker']
        rating = (a['rating'] or '').replace('_', ' ').title()
        prev = (a['previous_rating'] or '').replace('_', ' ').title() if a['previous_rating'] else ''
        pt = f"PT ${a['price_target']:.0f}" if a['price_target'] else ''
        prev_pt = f"(was ${a['previous_price_target']:.0f})" if a['previous_price_target'] else ''
        summary = a['summary'] or ''
        # Truncate summary
        if len(summary) > 120:
            summary = summary[:117] + "..."

        line = f"• *{ticker}*"
        if a['company_name']:
            line += f" ({a['company_name']})"
        line += f" → {rating}"
        if prev:
            line += f" (from {prev})"
        if pt:
            line += f" | {pt}"
        if prev_pt:
            line += f" {prev_pt}"
        if summary:
            line += f"\n  _{summary}_"

        action = (a['action'] or '').lower()
        if action == 'upgrade':
            upgrades.append(line)
        elif action == 'downgrade':
            downgrades.append(line)
        elif action in ('initiate', 'new_coverage'):
            initiations.append(line)
        else:
            other.append(line)

    sections = []
    if upgrades:
        sections.append("🟢 *UPGRADES*\n" + "\n".join(upgrades))
    if downgrades:
        sections.append("🔴 *DOWNGRADES*\n" + "\n".join(downgrades))
    if initiations:
        sections.append("🟡 *NEW COVERAGE*\n" + "\n".join(initiations))
    if other:
        sections.append("📝 *OTHER ACTIONS*\n" + "\n".join(other))

    return "\n\n".join(sections)


def _format_sector_commentary(sectors: List[Dict[str, Any]]) -> str:
    """Format sector outlook."""
    if not sectors:
        return ""

    lines = ["📊 *SECTOR OUTLOOK*"]
    for s in sectors:
        outlook = (s['outlook'] or 'neutral').replace('_', ' ').title()
        emoji = {"Overweight": "🟢", "Underweight": "🔴", "Neutral": "🟡",
                 "Market Weight": "🟡"}.get(outlook, "⚪")
        summary = s['summary'] or ''
        if len(summary) > 100:
            summary = summary[:97] + "..."
        lines.append(f"{emoji} *{s['sector']}*: {outlook}")
        if summary:
            lines.append(f"  _{summary}_")

    return "\n".join(lines)


def _format_sentiment(sentiment: Optional[Dict[str, Any]]) -> str:
    """Format market sentiment."""
    if not sentiment:
        return ""

    s = sentiment['overall_sentiment'] or 'neutral'
    summary = sentiment['summary'] or ''

    emoji = {
        'bullish': '🟢',
        'bearish': '🔴',
        'neutral': '🟡',
        'cautiously_bullish': '🟢',
        'cautiously_bearish': '🔴'
    }.get(s.lower(), '⚪')

    result = f"📰 *MARKET SENTIMENT*: {emoji} {s.replace('_', ' ').title()}"
    if summary:
        result += f"\n_{summary}_"

    if sentiment['key_themes']:
        try:
            themes = json.loads(sentiment['key_themes'])
            if themes:
                result += f"\n🔑 Themes: {', '.join(themes)}"
        except (json.JSONDecodeError, TypeError):
            pass

    return result


def _format_monthly_updates(conn: sqlite3.Connection, source: str, date_str: str) -> str:
    """Check if any monthly/weekly reports dropped today."""
    monthly_types = {
        'argus': ['portfolio_selector', 'sector_watch', 'special_situations',
                  'viewpoint', 'fixed_income_strategy', 'economy_at_a_glance'],
        'zacks': ['economic_outlook', 'market_strategy']
    }
    
    types = monthly_types.get(source, [])
    if not types:
        return ""

    monthly = conn.execute("""
        SELECT report_type, title FROM reports
        WHERE source = ? AND date = ? AND report_type IN ({})
    """.format(','.join('?' * len(types))),
        (source, date_str, *types)).fetchall()

    if not monthly:
        return ""

    lines = ["📋 *MONTHLY/WEEKLY UPDATES*"]
    for m in monthly:
        title = m['title'] or m['report_type'].replace('_', ' ').title()
        lines.append(f"• {title}")

    return "\n".join(lines)


def generate_combined_summary(conn: sqlite3.Connection, date_str: Optional[str] = None) -> str:
    """Generate a combined Argus + Zacks summary with agreement analysis."""
    if date_str is None:
        date_str = date.today().strftime("%Y-%m-%d")

    # Get actions from both sources
    argus_actions = conn.execute("""
        SELECT ticker, action, rating, price_target, summary
        FROM stock_actions WHERE source = 'argus' AND date = ?
    """, (date_str,)).fetchall()

    zacks_actions = conn.execute("""
        SELECT ticker, action, rating, zacks_rank, price_target, summary
        FROM stock_actions WHERE source = 'zacks' AND date = ?
    """, (date_str,)).fetchall()

    argus_tickers = {a['ticker']: a for a in argus_actions}
    zacks_tickers = {z['ticker']: z for z in zacks_actions}

    # Find agreements and disagreements
    common = set(argus_tickers.keys()) & set(zacks_tickers.keys())

    agreements = []
    disagreements = []

    for ticker in common:
        a = argus_tickers[ticker]
        z = zacks_tickers[ticker]

        a_rating = (a['rating'] or '').lower()
        z_rating = (z['rating'] or '').lower()

        # Simplify ratings for comparison
        a_bullish = a_rating in ('buy', 'strong_buy')
        z_bullish = z_rating in ('buy', 'strong_buy') or (z['zacks_rank'] and z['zacks_rank'] <= 2)

        if a_bullish == z_bullish:
            agreements.append(f"• *{ticker}*: Argus {a_rating.replace('_', ' ').title()} + Zacks {z_rating.replace('_', ' ').title()}")
        else:
            disagreements.append(f"• *{ticker}*: Argus {a_rating.replace('_', ' ').title()} vs Zacks {z_rating.replace('_', ' ').title()}")

    parts = []
    if agreements:
        parts.append("✅ *AGREEMENTS*\n" + "\n".join(agreements))
    if disagreements:
        parts.append("🔀 *DISAGREEMENTS*\n" + "\n".join(disagreements))

    return "\n\n".join(parts) if parts else ""


if __name__ == "__main__":
    import sys
    sys.path.insert(0, '.')
    from db import get_db

    conn = get_db()
    date_str = sys.argv[1] if len(sys.argv) > 1 else None

    logging.info("=== ARGUS ===")
    logging.info(generate_daily_summary(conn, "argus", date_str))
    logging.info("\n=== ZACKS ===")
    logging.info(generate_daily_summary(conn, "zacks", date_str))
    logging.info("\n=== COMBINED ===")
    logging.info(generate_combined_summary(conn, date_str))
