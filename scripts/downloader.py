"""
PDF Downloader — Downloads Argus and Zacks PDFs via browser JavaScript.

Since Fidelity uses Akamai bot protection, direct HTTP requests fail.
Instead, we execute fetch() calls in the logged-in Chrome browser session
and save the resulting blobs to disk.

This script is meant to be called by the orchestrator, which handles
the browser automation via OpenClaw's browser tool.
"""

import json
import os
import sys
import hashlib
import subprocess
from datetime import date, datetime
from pathlib import Path

# Add scripts dir to path for shared config
sys.path.insert(0, str(Path(__file__).parent))
from db import load_config, CONFIG_PATH

# Paths (config-driven with fallbacks)
BASE_DIR = Path(__file__).resolve().parent.parent
DOWNLOADS_DIR = BASE_DIR / "downloads"


def get_today_str():
    return date.today().strftime("%Y-%m-%d")


def get_date_str(d=None):
    return (d or date.today()).strftime("%Y%m%d")


def content_hash(data):
    """SHA-256 hash of bytes."""
    return hashlib.sha256(data).hexdigest()


def get_argus_urls(config, report_types=None):
    """Generate Argus report download URLs."""
    argus = config['sources']['argus']
    base_url = argus['base_url']
    urls = []

    for key, report in argus['reports'].items():
        if report_types and key not in report_types:
            continue
        url = f"{base_url}/pi/report/GetReport?type={report['type_code']}"
        urls.append({
            'key': key,
            'name': report['name'],
            'url': url,
            'frequency': report['frequency'],
            'priority': report['priority']
        })

    return urls


def get_zacks_page_url(config):
    """Get Zacks research page URL for scraping PDF links."""
    return config['sources']['zacks']['page_url']


def build_browser_download_script(urls, output_dir):
    """
    Build a JavaScript snippet that downloads PDFs via fetch() in the browser
    and returns them as base64-encoded strings.

    This is executed via OpenClaw's browser evaluate action.
    """
    os.makedirs(output_dir, exist_ok=True)

    # JavaScript that fetches each URL and returns base64 data
    js = f"""
    async () => {{
        const results = [];
        const urls = {json.dumps([u['url'] for u in urls])};
        const keys = {json.dumps([u['key'] for u in urls])};
        const names = {json.dumps([u['name'] for u in urls])};

        for (let i = 0; i < urls.length; i++) {{
            try {{
                const resp = await fetch(urls[i], {{ credentials: 'include' }});
                if (!resp.ok) {{
                    results.push({{
                        key: keys[i],
                        name: names[i],
                        url: urls[i],
                        status: resp.status,
                        error: 'HTTP ' + resp.status,
                        data: null
                    }});
                    continue;
                }}

                const contentType = resp.headers.get('content-type') || '';
                if (!contentType.includes('pdf')) {{
                    results.push({{
                        key: keys[i],
                        name: names[i],
                        url: urls[i],
                        status: resp.status,
                        error: 'Not a PDF: ' + contentType,
                        data: null
                    }});
                    continue;
                }}

                const blob = await resp.blob();
                const buffer = await blob.arrayBuffer();
                const bytes = new Uint8Array(buffer);

                // Convert to base64 in chunks to avoid stack overflow
                let binary = '';
                const chunkSize = 8192;
                for (let j = 0; j < bytes.length; j += chunkSize) {{
                    const chunk = bytes.subarray(j, Math.min(j + chunkSize, bytes.length));
                    binary += String.fromCharCode.apply(null, chunk);
                }}
                const base64 = btoa(binary);

                results.push({{
                    key: keys[i],
                    name: names[i],
                    url: urls[i],
                    status: resp.status,
                    error: null,
                    size: bytes.length,
                    data: base64
                }});
            }} catch (e) {{
                results.push({{
                    key: keys[i],
                    name: names[i],
                    url: urls[i],
                    status: 0,
                    error: e.message,
                    data: null
                }});
            }}
        }}

        return results;
    }}
    """

    return js


def build_zacks_link_scraper_js():
    """
    Build JavaScript to extract PDF links from the Zacks research page.
    Returns list of {text, href, doc_tag} for each PDF link.
    """
    return """
    () => {
        const links = Array.from(document.querySelectorAll('a[href]'));
        return links
            .filter(a => a.href.includes('pdf.asp') && a.textContent.includes('PDF'))
            .map(a => {
                const href = a.href;
                const url = new URL(href);
                const docTag = url.searchParams.get('docTag') || '';
                const feedId = url.searchParams.get('feedId') || '';
                const versionTag = url.searchParams.get('versionTag') || '';
                return {
                    text: a.textContent.trim(),
                    href: href,
                    docTag: docTag,
                    feedId: feedId,
                    versionTag: versionTag,
                    date: docTag.match(/(\\d{8})/)?.[1] || ''
                };
            });
    }
    """


def save_pdf(data_b64, output_dir, filename):
    """Save base64-encoded PDF data to disk."""
    import base64
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, filename)
    with open(filepath, 'wb') as f:
        f.write(base64.b64decode(data_b64))
    return filepath


def compute_file_hash(filepath):
    """Compute SHA-256 hash of a file."""
    h = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


def get_downloads_dir():
    """Get downloads directory from config."""
    try:
        config = load_config()
        dl_rel = config.get('paths', {}).get('downloads', 'downloads')
        return BASE_DIR / dl_rel
    except (FileNotFoundError, json.JSONDecodeError):
        return BASE_DIR / "downloads"


def cleanup_old_downloads(days=7):
    """Remove PDF downloads older than N days."""
    dl_dir = get_downloads_dir()
    cutoff = datetime.now().timestamp() - (days * 86400)
    for source_dir in (dl_dir / "argus", dl_dir / "zacks"):
        if not source_dir.exists():
            continue
        for date_dir in source_dir.iterdir():
            if date_dir.is_dir():
                try:
                    dir_date = datetime.strptime(date_dir.name, "%Y-%m-%d")
                    if dir_date.timestamp() < cutoff:
                        import shutil
                        shutil.rmtree(date_dir)
                        print(f"Cleaned up: {date_dir}")
                except ValueError:
                    pass


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fidelity PDF Downloader")
    parser.add_argument("--source", choices=["argus", "zacks", "all"], default="all")
    parser.add_argument("--list-urls", action="store_true", help="List download URLs")
    parser.add_argument("--cleanup", action="store_true", help="Clean up old downloads")
    args = parser.parse_args()

    config = load_config()

    if args.cleanup:
        cleanup_old_downloads(config['retention']['pdf_days'])
        sys.exit(0)

    if args.list_urls:
        if args.source in ("argus", "all"):
            print("\n=== Argus Report URLs ===")
            for url_info in get_argus_urls(config):
                print(f"  [{url_info['priority']}] {url_info['name']} ({url_info['frequency']})")
                print(f"    {url_info['url']}")
        if args.source in ("zacks", "all"):
            print(f"\n=== Zacks Page URL ===")
            print(f"  {get_zacks_page_url(config)}")
            print(f"  (PDF links extracted from page)")
        sys.exit(0)

    print("Use orchestrator.py for full pipeline execution.")
