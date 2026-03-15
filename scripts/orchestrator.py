import logging
from typing import Dict, List, Any, Optional, Tuple
"""
Orchestrator — Main pipeline for Fidelity Research automation.

Coordinates: download → convert → extract → store → summarize

Designed to be called by OpenClaw cron via Gemini CLI agent.
The agent uses the browser tool for downloads, then runs the
rest of the pipeline via Python subprocess calls.
"""

import json
import os
import sys
import hashlib
import base64
import argparse
import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler('data/pipeline.log'),
        logging.StreamHandler()
    ]
)

from datetime import date, datetime
from pathlib import Path

# Add scripts dir to path
sys.path.insert(0, str(Path(__file__).parent))

from db import get_db, load_config, store_report, store_stock_action, log_download, is_already_downloaded
from downloader import (get_argus_urls, get_zacks_page_url,
                         build_browser_download_script, save_pdf,
                         compute_file_hash, cleanup_old_downloads)
from pdf_converter import convert_directory, pdf_to_markdown
from extractor import build_extraction_prompt, build_bull_bear_prompt, parse_extraction_result, validate_extraction
from summary import generate_daily_summary, generate_combined_summary


def get_downloads_dir() -> Path:
    """Get downloads directory from config."""
    try:
        config = load_config()
        dl_rel = config.get('paths', {}).get('downloads', 'downloads')
        return BASE_DIR / dl_rel
    except (FileNotFoundError, json.JSONDecodeError):
        return BASE_DIR / "downloads"


def get_reports_dir() -> Path:
    """Get reports directory from config."""
    try:
        config = load_config()
        rp_rel = config.get('paths', {}).get('reports', 'reports')
        return BASE_DIR / rp_rel
    except (FileNotFoundError, json.JSONDecodeError):
        return BASE_DIR / "reports"

BASE_DIR = Path(__file__).resolve().parent.parent



def get_today_str() -> str:
    return date.today().strftime("%Y-%m-%d")


def get_today_compact() -> str:
    return date.today().strftime("%Y%m%d")


# ============================================================
# BROWSER DOWNLOAD PHASE
# ============================================================

def generate_argus_download_script(report_types: Optional[List[str]] = None) -> Tuple[str, List[str]]:
    """
    Generate JavaScript to download Argus PDFs via browser fetch().
    Returns the JS code to execute in the browser.
    """
    config = load_config()
    urls = get_argus_urls(config, report_types)
    output_dir = str(get_downloads_dir() / "argus" / get_today_str())
    return build_browser_download_script(urls, output_dir), urls


def generate_zacks_scrape_script() -> str:
    """Generate JavaScript to extract Zacks PDF links from the page."""
    from downloader import build_zacks_link_scraper_js
    return build_zacks_link_scraper_js()


def process_downloaded_pdfs(source: str, pdf_results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Process results from browser PDF downloads.

    Args:
        source: 'argus' or 'zacks'
        pdf_results: list of {key, name, status, data (base64), error, size}

    Returns list of saved file paths.
    """
    output_dir = get_downloads_dir() / source / get_today_str()
    saved = []

    for result in pdf_results:
        if result.get('error') or not result.get('data'):
            logging.warning(f" {result.get('name', result.get('key'))}: {result.get('error', 'no data')}")
            continue

        filename = f"{result['key']}_{get_today_str()}.pdf"
        filepath = save_pdf(result['data'], str(output_dir), filename)
        saved.append({
            'key': result['key'],
            'name': result['name'],
            'path': filepath,
            'size': result.get('size', 0),
            'hash': compute_file_hash(filepath)
        })
        logging.info(f" Saved: {filename} ({result.get('size', 0):,} bytes)")

    return saved


def process_zacks_links(zacks_links: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Process scraped Zacks PDF links. Downloads are done via browser fetch.

    Returns list of URLs to download with metadata.
    """
    today = get_today_compact()
    downloads = []

    for link in zacks_links:
        doc_tag = link.get('docTag', '')
        version_tag = link.get('versionTag', '')
        feed_id = link.get('feedId', '')

        # Determine report type from docTag
        if 'Economic_Outlook' in doc_tag:
            key = 'economic_outlook'
        elif 'Strategy' in doc_tag:
            key = 'market_strategy'
        elif 'Focus_List' in doc_tag or 'Model_Portfolio' in doc_tag:
            key = 'focus_list'
        elif 'Industry' in doc_tag:
            key = 'industry_outlook'
        else:
            key = doc_tag.split('_')[0].lower() if doc_tag else 'unknown'

        downloads.append({
            'key': key,
            'name': link['text'],
            'url': link['href'],
            'doc_tag': doc_tag,
            'version_tag': version_tag,
            'feed_id': feed_id
        })

    return downloads


# ============================================================
# CONVERSION PHASE
# ============================================================

def convert_pdfs_to_markdown(source: str, date_str: Optional[str] = None) -> List[Tuple[str, str, int]]:
    """Convert all PDFs for a source/date to markdown."""
    if date_str is None:
        date_str = get_today_str()

    logging.info(f"\nConverting {source} PDFs for {date_str}...")
    results = convert_directory(source, date_str)
    logging.info(f"  Converted {len(results)} files")
    return results


# ============================================================
# EXTRACTION PHASE (AI)
# ============================================================

def generate_extraction_prompts(source: str, date_str: Optional[str] = None) -> List[Dict[str, str]]:
    """
    Generate AI extraction prompts for all reports of a source/date.
    These prompts are designed to be sent to Gemini CLI.
    """
    if date_str is None:
        date_str = get_today_str()

    md_dir = get_reports_dir() / source / date_str
    if not md_dir.exists():
        logging.info(f"No markdown files found: {md_dir}")
        return []

    prompts = []
    for md_file in sorted(md_dir.glob("*.md")):
        with open(md_file, 'r') as f:
            text = f.read()

        # Determine report type from filename
        report_type = md_file.stem.rsplit('_', 1)[0]  # Remove date suffix

        prompt = build_extraction_prompt(text, source, report_type, date_str)
        prompts.append({
            'file': str(md_file),
            'report_type': report_type,
            'prompt': prompt
        })

    return prompts


def store_extraction_results(conn, source: str, date_str: str, extraction_data: Dict[str, Any], report_type: str, file_hash: str = "pending"):
    """Store AI extraction results in the database."""
    if not extraction_data:
        return

    meta = extraction_data.get('report_metadata', {})
    report_id = f"{source}-{date_str}-{report_type}"

    # Store report
    store_report(
        conn, source, date_str, report_type,
        title=meta.get('title', report_type.replace('_', ' ').title()),
        filename=f"{report_type}_{date_str}.pdf",
        content_hash=file_hash,
        metadata={'overall_sentiment': meta.get('overall_sentiment'),
                  'market_summary': meta.get('market_summary'),
                  'key_themes': extraction_data.get('key_themes')}
    )

    # Store stock actions
    for action in extraction_data.get('stock_actions', []):
        store_stock_action(
            conn, source, report_id, date_str,
            ticker=action.get('ticker', ''),
            company_name=action.get('company_name'),
            action=action.get('action'),
            rating=action.get('rating'),
            previous_rating=action.get('previous_rating'),
            price_target=action.get('price_target'),
            previous_price_target=action.get('previous_price_target'),
            sector=action.get('sector'),
            summary=action.get('summary'),
            zacks_rank=action.get('zacks_rank'),
            previous_zacks_rank=action.get('previous_zacks_rank'),
            zacks_recommendation=action.get('zacks_recommendation'),
            style_score_value=action.get('style_score_value'),
            style_score_growth=action.get('style_score_growth'),
            style_score_momentum=action.get('style_score_momentum')
        )

    # Store sector commentary
    for sector in extraction_data.get('sector_commentary', []):
        sector_id = f"{source}-{date_str}-{sector.get('sector', 'unknown')}"
        conn.execute("""
            INSERT OR REPLACE INTO sector_commentary
                (id, report_id, date, source, sector, outlook, summary)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (sector_id, report_id, date_str, source,
              sector.get('sector'), sector.get('outlook'), sector.get('summary')))

    conn.commit()


# ============================================================
# SUMMARY PHASE
# ============================================================

def generate_summary_message(source: str, date_str: Optional[str] = None) -> str:
    """Generate the Telegram summary message."""
    if date_str is None:
        date_str = get_today_str()

    conn = get_db()
    summary = generate_daily_summary(conn, source, date_str)
    combined = generate_combined_summary(conn, date_str)

    if combined:
        summary += "\n\n" + combined

    return summary


# ============================================================
# CLEANUP PHASE
# ============================================================

def cleanup() -> None:
    """Clean up old files."""
    config = load_config()
    days = config['retention']['pdf_days']
    if days > 0:
        cleanup_old_downloads(days)


# ============================================================
# CLI INTERFACE
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Fidelity Research Orchestrator")
    subparsers = parser.add_subparsers(dest='command')

    # Download phase
    dl = subparsers.add_parser('download-script', help='Generate browser download script')
    dl.add_argument('--source', choices=['argus', 'zacks'], required=True)

    # Scrape Zacks links
    zl = subparsers.add_parser('zacks-links', help='Generate Zacks link scraper script')

    # Process downloaded PDFs
    pp = subparsers.add_parser('process-pdfs', help='Process downloaded PDF results (JSON on stdin)')
    pp.add_argument('--source', choices=['argus', 'zacks'], required=True)

    # Convert phase
    cv = subparsers.add_parser('convert', help='Convert PDFs to markdown')
    cv.add_argument('--source', choices=['argus', 'zacks'], required=True)
    cv.add_argument('--date', help='Date (YYYY-MM-DD)')

    # Extraction prompts
    ep = subparsers.add_parser('extract-prompts', help='Generate AI extraction prompts')
    ep.add_argument('--source', choices=['argus', 'zacks'], required=True)
    ep.add_argument('--date', help='Date (YYYY-MM-DD)')

    # Store extraction results
    se = subparsers.add_parser('store', help='Store extraction results')
    se.add_argument('--source', choices=['argus', 'zacks'], required=True)
    se.add_argument('--date', help='Date (YYYY-MM-DD)')
    se.add_argument('--report-type', required=True)
    se.add_argument('--input', help='JSON file with extraction results (default: stdin)')
    se.add_argument('--hash', default='pending', help='File hash')

    # Summary
    sm = subparsers.add_parser('summary', help='Generate daily summary')
    sm.add_argument('--source', choices=['argus', 'zacks', 'combined'])
    sm.add_argument('--date', help='Date (YYYY-MM-DD)')

    # Cleanup
    subparsers.add_parser('cleanup', help='Clean up old downloads')

    # Status
    subparsers.add_parser('status', help='Show pipeline status')

    args = parser.parse_args()

    if args.command == 'download-script':
        if args.source == 'argus':
            js, urls = generate_argus_download_script()
            logging.info(js)
        else:
            logging.info(generate_zacks_scrape_script())

    elif args.command == 'zacks-links':
        logging.info(generate_zacks_scrape_script())

    elif args.command == 'process-pdfs':
        data = json.load(sys.stdin)
        saved = process_downloaded_pdfs(args.source, data)
        logging.info(json.dumps(saved, indent=2))

    elif args.command == 'convert':
        convert_pdfs_to_markdown(args.source, args.date)

    elif args.command == 'extract-prompts':
        prompts = generate_extraction_prompts(args.source, args.date)
        for p in prompts:
            logging.info(f"\n{'='*60}")
            logging.info(f"Report: {p['report_type']}")
            logging.info(f"{'='*60}")
            logging.info(p['prompt'])

    elif args.command == 'store':
        conn = get_db()
        if args.input:
            with open(args.input) as f:
                data = json.load(f)
        else:
            data = json.load(sys.stdin)
        store_extraction_results(conn, args.source, args.date or get_today_str(),
                                 data, args.report_type, args.hash)
        logging.info("Stored successfully")

    elif args.command == 'summary':
        if args.source and args.source != 'combined':
            logging.info(generate_summary_message(args.source, args.date))
        else:
            conn = get_db()
            logging.info(generate_combined_summary(conn, args.date or get_today_str()))

    elif args.command == 'cleanup':
        cleanup()

    elif args.command == 'status':
        conn = get_db()
        today = get_today_str()
        for source in ['argus', 'zacks']:
            reports = conn.execute(
                "SELECT COUNT(*) as c FROM reports WHERE source = ? AND date = ?",
                (source, today)
            ).fetchone()
            actions = conn.execute(
                "SELECT COUNT(*) as c FROM stock_actions WHERE source = ? AND date = ?",
                (source, today)
            ).fetchone()
            logging.info(f"{source}: {reports['c']} reports, {actions['c']} stock actions")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
