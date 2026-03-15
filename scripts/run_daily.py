#!/usr/bin/env python3
"""
Daily Fidelity Research Pipeline Runner.
Downloads → Converts (pymupdf4llm) → Extracts (Gemini CLI stdin) → Stores → Summarizes → Telegram.
Usage: python run_daily.py [--dry-run] [--skip-download]
"""
import asyncio, json, os, sys
from datetime import date, datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.join(SCRIPT_DIR, '..')
sys.path.insert(0, SCRIPT_DIR)

from db import get_db
from convert_pdfs_v2 import convert_all
from extract_sdk import extract_report, store_extraction, ARGUS_EXTRACTION, ZACKS_EXTRACTION
from summary import generate_daily_summary, generate_combined_summary

ARGUS_DL = os.path.join(PROJECT_DIR, 'downloads', 'argus')
ZACKS_DL = os.path.join(PROJECT_DIR, 'downloads', 'zacks')
ARGUS_RPT = os.path.join(PROJECT_DIR, 'reports', 'argus')
ZACKS_RPT = os.path.join(PROJECT_DIR, 'reports', 'zacks')

def log(msg, level="INFO"):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [{level}] {msg}")

async def run_pipeline(dry_run=False, skip_download=False):
    report_date = date.today().strftime("%Y-%m-%d")
    conn = get_db()
    log(f"=== Fidelity Research Pipeline — {report_date} ===")

    # Step 0: Discover Chrome tabs
    log("Step 0: Discovering Chrome tabs...")
    try:
        from discover_tabs import get_tabs_async
        tabs = await get_tabs_async()
        argus_tab, zacks_tab = tabs.get('argus'), tabs.get('zacks')
        if argus_tab and zacks_tab:
            log(f"  Found: argus={argus_tab[:8]}..., zacks={zacks_tab[:8]}...")
        else:
            log(f"  Partial: argus={argus_tab}, zacks={zacks_tab}", "WARN")
    except Exception as e:
        log(f"  Tab discovery failed: {e}", "WARN")
        argus_tab = zacks_tab = None

    # Step 1: Download PDFs
    if not skip_download:
        if not argus_tab or not zacks_tab:
            log("Cannot download without Chrome tabs — use --skip-download", "ERROR")
            return 0
        log("Step 1: Downloading PDFs...")
        try:
            from downloaders import download_argus, download_zacks
            r = await download_argus(argus_tab, ARGUS_DL)
            log(f"  Argus: {sum(1 for v in r.values() if v.get('status')=='ok')}/{len(r)} downloaded")
            r = await download_zacks(zacks_tab, ZACKS_DL)
            log(f"  Zacks: {sum(1 for v in r.values() if v.get('status')=='ok')}/{len(r)} downloaded")
        except Exception as e:
            log(f"  Download error: {e}", "ERROR")
    else:
        log("Step 1: Skipping download")

    # Step 2: Convert PDFs to markdown (pymupdf4llm)
    log("Step 2: Converting PDFs (pymupdf4llm)...")
    convert_all(ARGUS_DL, ARGUS_RPT)
    convert_all(ZACKS_DL, ZACKS_RPT)

    # Step 3: Extract structured data (Gemini CLI via stdin)
    log("Step 3: Extracting data (Gemini CLI)...")
    total_actions = 0
    for source, rpt_dir, configs in [('argus', ARGUS_RPT, ARGUS_EXTRACTION), ('zacks', ZACKS_RPT, ZACKS_EXTRACTION)]:
        for fn, rt in configs:
            md_path = os.path.join(rpt_dir, fn)
            if os.path.exists(md_path):
                data = extract_report(md_path, source, rt, report_date)
                if data:
                    if not dry_run:
                        store_extraction(conn, data, source, report_date)
                    n = len(data.get('stock_actions', []))
                    total_actions += n
                    log(f"  {source}/{fn}: {n} actions")
            else:
                log(f"  {fn}: not found", "WARN")

    # Step 4: Generate summary
    log("Step 4: Generating summary...")
    argus_s = generate_daily_summary(conn, "argus", report_date) if not dry_run and total_actions > 0 else ""
    zacks_s = generate_daily_summary(conn, "zacks", report_date) if not dry_run and total_actions > 0 else ""
    combined = generate_combined_summary(conn, report_date) if not dry_run and total_actions > 0 else ""
    total_stored = conn.execute("SELECT COUNT(*) FROM stock_actions WHERE date=?", (report_date,)).fetchone()[0] if not dry_run else total_actions

    msg = f"""📈 *Fidelity Research Daily* — {report_date}

✅ {total_stored} stock actions extracted

{argus_s}

{zacks_s}

{combined}

---
_Automated pipeline — {datetime.now().strftime('%H:%M PDT')}_"""

    os.makedirs(os.path.join(PROJECT_DIR, 'data'), exist_ok=True)
    with open(os.path.join(PROJECT_DIR, 'data', 'latest_summary.txt'), 'w') as f:
        f.write(msg)
    print("\n" + "="*60 + "\n" + msg + "\n" + "="*60)
    conn.close()
    log("=== Pipeline complete ===")
    return total_stored

def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--skip-download', action='store_true')
    args = p.parse_args()
    asyncio.run(run_pipeline(dry_run=args.dry_run, skip_download=args.skip_download))

if __name__ == "__main__":
    main()
