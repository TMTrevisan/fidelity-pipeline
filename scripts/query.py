"""
Query tool — Search and analyze stored research data.

Usage:
    python scripts/query.py --upgrades --week
    python scripts/query.py --ticker NVDA --days 30
    python scripts/query.py --sector Technology --month
    python scripts/query.py --bull-bear --week
    python scripts/query.py --focus-list
    python scripts/query.py --sentiment --week
    python scripts/query.py --compare NVDA AAPL MSFT
"""

import sys
import json
import argparse
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from db import get_db

DATE_RANGES = {
    'today': 0,
    'week': 7,
    'month': 30,
    'quarter': 90
}


def get_date_range(period, days=None):
    if days:
        start = date.today() - timedelta(days=days)
    elif period in DATE_RANGES:
        start = date.today() - timedelta(days=DATE_RANGES[period])
    else:
        start = date.today() - timedelta(days=7)
    return start.strftime("%Y-%m-%d")


def query_upgrades(conn, period='week', days=None):
    """Show upgrades in date range."""
    start = get_date_range(period, days)
    rows = conn.execute("""
        SELECT date, source, ticker, company_name, rating, previous_rating,
               price_target, previous_price_target, summary
        FROM stock_actions
        WHERE action = 'upgrade' AND date >= ?
        ORDER BY date DESC, ticker
    """, (start,)).fetchall()

    if not rows:
        print("No upgrades found.")
        return

    print(f"\n📈 UPGRADES since {start}:\n")
    for r in rows:
        pt = f"PT ${r['price_target']:.0f}" if r['price_target'] else ''
        prev_pt = f"(was ${r['previous_price_target']:.0f})" if r['previous_price_target'] else ''
        print(f"  {r['date']} [{r['source']}] {r['ticker']}: "
              f"{(r['previous_rating'] or '?').replace('_', ' ')} → {(r['rating'] or '?').replace('_', ' ')}"
              f" {pt} {prev_pt}")
        if r['summary']:
            print(f"    {r['summary'][:120]}")


def query_downgrades(conn, period='week', days=None):
    """Show downgrades in date range."""
    start = get_date_range(period, days)
    rows = conn.execute("""
        SELECT date, source, ticker, company_name, rating, previous_rating,
               price_target, summary
        FROM stock_actions
        WHERE action = 'downgrade' AND date >= ?
        ORDER BY date DESC, ticker
    """, (start,)).fetchall()

    if not rows:
        print("No downgrades found.")
        return

    print(f"\n📉 DOWNGRADES since {start}:\n")
    for r in rows:
        pt = f"PT ${r['price_target']:.0f}" if r['price_target'] else ''
        print(f"  {r['date']} [{r['source']}] {r['ticker']}: "
              f"{(r['previous_rating'] or '?').replace('_', ' ')} → {(r['rating'] or '?').replace('_', ' ')} {pt}")
        if r['summary']:
            print(f"    {r['summary'][:120]}")


def query_ticker(conn, ticker, days=30):
    """Show all actions for a ticker."""
    start = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = conn.execute("""
        SELECT date, source, action, rating, previous_rating,
               price_target, previous_price_target, summary
        FROM stock_actions
        WHERE ticker = ? AND date >= ?
        ORDER BY date DESC
    """, (ticker.upper(), start)).fetchall()

    if not rows:
        print(f"No data found for {ticker.upper()} in the last {days} days.")
        return

    print(f"\n📊 {ticker.upper()} — last {days} days:\n")
    for r in rows:
        action = (r['action'] or '').upper()
        rating = (r['rating'] or '').replace('_', ' ').title()
        pt = f"PT ${r['price_target']:.0f}" if r['price_target'] else ''
        print(f"  {r['date']} [{r['source']}] {action}: {rating} {pt}")
        if r['summary']:
            print(f"    {r['summary'][:150]}")


def query_sector(conn, sector, period='month', days=None):
    """Show sector commentary."""
    start = get_date_range(period, days)
    rows = conn.execute("""
        SELECT date, source, outlook, summary
        FROM sector_commentary
        WHERE sector LIKE ? AND date >= ?
        ORDER BY date DESC
    """, (f"%{sector}%", start)).fetchall()

    if not rows:
        print(f"No sector data found for '{sector}'.")
        return

    print(f"\n🏭 Sector: {sector} — since {start}:\n")
    for r in rows:
        print(f"  {r['date']} [{r['source']}] {r['outlook']}")
        if r['summary']:
            print(f"    {r['summary'][:150]}")


def query_bull_bear(conn, period='week', days=None):
    """Show Zacks Bull & Bear picks."""
    start = get_date_range(period, days)
    rows = conn.execute("""
        SELECT date, bull_ticker, bull_company, bull_zacks_rank, bull_summary,
               bear_ticker, bear_company, bear_zacks_rank, bear_summary
        FROM bull_bear_daily
        WHERE date >= ?
        ORDER BY date DESC
    """, (start,)).fetchall()

    if not rows:
        print("No Bull & Bear data found.")
        return

    print(f"\n🐂🐻 Zacks Bull & Bear since {start}:\n")
    for r in rows:
        print(f"  {r['date']}:")
        print(f"    🐂 Bull: {r['bull_ticker']} ({r['bull_company']}) — Rank #{r['bull_zacks_rank']}")
        if r['bull_summary']:
            print(f"       {r['bull_summary'][:100]}")
        print(f"    🐻 Bear: {r['bear_ticker']} ({r['bear_company']}) — Rank #{r['bear_zacks_rank']}")
        if r['bear_summary']:
            print(f"       {r['bear_summary'][:100]}")


def query_focus_list(conn):
    """Show current Zacks Focus List."""
    rows = conn.execute("""
        SELECT ticker, company_name, zacks_rank, added_date
        FROM focus_list_current
        ORDER BY ticker
    """).fetchall()

    if not rows:
        print("No Focus List data.")
        return

    print(f"\n📋 Zacks Focus List ({len(rows)} stocks):\n")
    for r in rows:
        rank = f"Rank #{r['zacks_rank']}" if r['zacks_rank'] else ''
        added = f"(added {r['added_date']})" if r['added_date'] else ''
        print(f"  {r['ticker']:6} {r['company_name'] or '':30} {rank} {added}")


def query_sentiment(conn, period='week', days=None):
    """Show market sentiment trend."""
    start = get_date_range(period, days)
    rows = conn.execute("""
        SELECT date, source, overall_sentiment, summary
        FROM market_sentiment
        WHERE date >= ?
        ORDER BY date DESC
    """, (start,)).fetchall()

    if not rows:
        print("No sentiment data found.")
        return

    print(f"\n📰 Market Sentiment since {start}:\n")
    for r in rows:
        print(f"  {r['date']} [{r['source']}] {r['overall_sentiment']}")
        if r['summary']:
            print(f"    {r['summary'][:150]}")


def query_compare(conn, tickers):
    """Compare multiple tickers side by side."""
    placeholders = ','.join('?' * len(tickers))
    rows = conn.execute(f"""
        SELECT ticker, date, source, action, rating, price_target, summary
        FROM stock_actions
        WHERE ticker IN ({placeholders})
        ORDER BY ticker, date DESC
    """, [t.upper() for t in tickers]).fetchall()

    if not rows:
        print("No data found for those tickers.")
        return

    print(f"\n🔍 Comparison: {', '.join(t.upper() for t in tickers)}\n")
    current_ticker = None
    for r in rows:
        if r['ticker'] != current_ticker:
            current_ticker = r['ticker']
            print(f"\n  {current_ticker}:")
        rating = (r['rating'] or '').replace('_', ' ').title()
        pt = f"PT ${r['price_target']:.0f}" if r['price_target'] else ''
        print(f"    {r['date']} [{r['source']}] {rating} {pt}")


def main():
    parser = argparse.ArgumentParser(description="Query Fidelity Research Database")

    parser.add_argument('--upgrades', action='store_true', help='Show upgrades')
    parser.add_argument('--downgrades', action='store_true', help='Show downgrades')
    parser.add_argument('--ticker', '-t', help='Filter by ticker')
    parser.add_argument('--sector', '-s', help='Filter by sector')
    parser.add_argument('--bull-bear', action='store_true', help='Show Zacks Bull & Bear')
    parser.add_argument('--focus-list', action='store_true', help='Show Zacks Focus List')
    parser.add_argument('--sentiment', action='store_true', help='Show market sentiment')
    parser.add_argument('--compare', nargs='+', help='Compare tickers')

    parser.add_argument('--today', action='store_true')
    parser.add_argument('--week', action='store_true')
    parser.add_argument('--month', action='store_true')
    parser.add_argument('--quarter', action='store_true')
    parser.add_argument('--days', type=int, help='Custom day range')

    args = parser.parse_args()

    period = 'week'
    if args.today:
        period = 'today'
    elif args.month:
        period = 'month'
    elif args.quarter:
        period = 'quarter'
    elif args.week:
        period = 'week'

    conn = get_db()

    if args.upgrades:
        query_upgrades(conn, period, args.days)
    elif args.downgrades:
        query_downgrades(conn, period, args.days)
    elif args.ticker:
        query_ticker(conn, args.ticker, args.days or DATE_RANGES[period])
    elif args.sector:
        query_sector(conn, args.sector, period, args.days)
    elif args.bull_bear:
        query_bull_bear(conn, period, args.days)
    elif args.focus_list:
        query_focus_list(conn)
    elif args.sentiment:
        query_sentiment(conn, period, args.days)
    elif args.compare:
        query_compare(conn, args.compare)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
