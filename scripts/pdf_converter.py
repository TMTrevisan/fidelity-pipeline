import logging
from typing import Dict, List, Any, Optional, Tuple
"""
PDF to Markdown converter using pymupdf4llm for table-aware extraction.

Replaces basic fitz.get_text('text') with semantic markdown output
that preserves tables, headers, and document structure.
"""

import os
import re
import hashlib
from pathlib import Path
from datetime import date

try:
    import pymupdf4llm
    import pymupdf
except ImportError:
    logging.info("pymupdf4llm not installed. Run: pip install pymupdf4llm")
    import sys
    sys.exit(1)


BASE_DIR = Path(__file__).resolve().parent.parent
REPORTS_DIR = BASE_DIR / "reports"


def pdf_to_markdown(pdf_path: str, output_path: Optional[str] = None) -> Tuple[str, int]:
    """
    Extract text from a PDF and save as markdown using pymupdf4llm.

    pymupdf4llm produces proper markdown tables and better structure
    than basic fitz.get_text('text').

    Returns (markdown_text, page_count).
    """
    # pymupdf4llm.to_markdown produces table-aware markdown
    markdown = pymupdf4llm.to_markdown(pdf_path)

    # Get page count
    doc = pymupdf.open(pdf_path)
    page_count = len(doc)
    doc.close()

    # Clean up common PDF artifacts
    markdown = clean_extracted_text(markdown)

    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(markdown)

    return markdown, page_count


def clean_extracted_text(text: str) -> str:
    """Clean up PDF extraction artifacts."""
    # Fix common ligatures
    text = text.replace('\ufb01', 'fi').replace('\ufb02', 'fl')
    text = text.replace('\ufb00', 'ff').replace('\ufb03', 'ffi').replace('\ufb04', 'ffl')

    # Fix multiple spaces (but preserve intentional indentation)
    text = re.sub(r' {3,}', '  ', text)

    # Fix multiple newlines
    text = re.sub(r'\n{4,}', '\n\n\n', text)

    # Remove page numbers that are alone on a line
    text = re.sub(r'\n\s*\d+\s*\n', '\n', text)

    # Fix broken hyphenation across lines
    text = re.sub(r'(\w)-\n(\w)', r'\1\2', text)

    return text


def extract_report_sections(markdown_text: str, source: str = "argus") -> Dict[str, str]:
    """
    Attempt to split a report into logical sections based on headers.

    Returns dict of {section_name: section_text}.
    """
    sections = {}
    current_section = "header"
    current_text = []

    for line in markdown_text.split('\n'):
        stripped = line.strip()

        # Argus-style headers
        if source == "argus":
            if (stripped.isupper() and len(stripped) > 3 and len(stripped) < 80) or \
               (stripped.endswith(':') and len(stripped) < 60 and stripped == stripped.title()):
                if current_text:
                    sections[current_section] = '\n'.join(current_text)
                current_section = stripped.rstrip(':').lower().replace(' ', '_')
                current_text = []
                continue

        # Zacks-style headers
        elif source == "zacks":
            if re.match(r'^\d[\.\)]\s+[A-Z]', stripped):
                if current_text:
                    sections[current_section] = '\n'.join(current_text)
                current_section = stripped.lower().replace(' ', '_')
                current_text = []
                continue

        current_text.append(line)

    if current_text:
        sections[current_section] = '\n'.join(current_text)

    return sections


def convert_directory(source: str, date_str: Optional[str] = None) -> List[Tuple[str, Optional[str], int]]:
    """
    Convert all PDFs in a date directory to markdown.

    Args:
        source: 'argus' or 'zacks'
        date_str: YYYY-MM-DD format, defaults to today

    Returns list of (pdf_path, md_path, page_count) tuples.
    """
    if date_str is None:
        date_str = date.today().strftime("%Y-%m-%d")

    pdf_dir = BASE_DIR / "downloads" / source / date_str
    md_dir = REPORTS_DIR / source / date_str

    if not pdf_dir.exists():
        logging.info(f"No downloads found: {pdf_dir}")
        return []

    results = []
    for pdf_file in sorted(pdf_dir.glob("*.pdf")):
        md_file = md_dir / (pdf_file.stem + ".md")
        try:
            markdown, pages = pdf_to_markdown(str(pdf_file), str(md_file))
            results.append((str(pdf_file), str(md_file), pages))
            logging.info(f"  Converted: {pdf_file.name} ({pages} pages, {len(markdown):,} chars)")
        except Exception as e:
            logging.info(f"  Error converting {pdf_file.name}: {e}")
            results.append((str(pdf_file), None, 0))

    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Convert PDFs to Markdown (pymupdf4llm)")
    parser.add_argument("pdf", nargs="?", help="PDF file to convert")
    parser.add_argument("--source", choices=["argus", "zacks"], help="Convert all PDFs for source")
    parser.add_argument("--date", help="Date (YYYY-MM-DD), default today")
    parser.add_argument("--output", "-o", help="Output markdown file")
    args = parser.parse_args()

    if args.pdf:
        md, pages = pdf_to_markdown(args.pdf, args.output)
        logging.info(f"Converted {pages} pages ({len(md):,} chars)")
        if not args.output:
            logging.info(md[:500])
    elif args.source:
        results = convert_directory(args.source, args.date)
        logging.info(f"\nConverted {len(results)} files")
    else:
        parser.print_help()
