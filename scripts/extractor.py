import logging
from typing import Dict, List, Any, Optional
"""
Structured data extraction from research reports.

This script generates the extraction prompts and parses AI output.
The actual AI call is made by the orchestrator (via Gemini CLI).

Output format: JSON with stock actions, sector commentary, sentiment.
"""

import json
import re
from datetime import date


EXTRACTION_PROMPT_TEMPLATE = """You are a financial research analyst. Extract ALL structured data from this research report.

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
        "market_summary": "<2-3 sentence summary of overall market outlook>"
    }},
    "stock_actions": [
        {{
            "ticker": "<stock ticker symbol>",
            "company_name": "<full company name>",
            "action": "<upgrade|downgrade|initiate|reiterate|maintain|new_coverage>",
            "rating": "<strong_buy|buy|hold|sell|strong_sell>",
            "previous_rating": "<if mentioned, otherwise null>",
            "price_target": <number or null>,
            "previous_price_target": <number or null>,
            "sector": "<GICS sector if mentioned>",
            "summary": "<1-2 sentence reason for the rating>"
        }}
    ],
    "sector_commentary": [
        {{
            "sector": "<sector name>",
            "outlook": "<overweight|underweight|neutral|market_weight>",
            "summary": "<brief sector outlook>"
        }}
    ],
    "key_themes": ["<theme1>", "<theme2>"],
    "notable_calls": [
        {{
            "ticker": "<ticker>",
            "note": "<what makes this notable or contrarian>"
        }}
    ]
}}

Rules:
- Extract EVERY stock mentioned with a rating, price target, or action
- If a stock is mentioned but no rating/action, do NOT include it in stock_actions
- Use null for unknown fields, not empty strings
- Ticker symbols should be uppercase, 1-5 letters
- Price targets are numbers only (no $ sign)
- If the report has NO stock actions, return an empty array for stock_actions
"""

ZACKS_BULL_BEAR_PROMPT = """Extract the Bull and Bear of the Day from this Zacks report.

Report text:
{report_text}

Respond ONLY with JSON:
{{
    "date": "{report_date}",
    "bull": {{
        "ticker": "<ticker>",
        "company": "<company name>",
        "zacks_rank": <1-5>,
        "rank_label": "<Strong Buy|Buy|Hold|Sell|Strong Sell>",
        "summary": "<why it's the bull>"
    }},
    "bear": {{
        "ticker": "<ticker>",
        "company": "<company name>",
        "zacks_rank": <1-5>,
        "rank_label": "<Strong Buy|Buy|Hold|Sell|Strong Sell>",
        "summary": "<why it's the bear>"
    }}
}}
"""


def _truncate_at_sentence(text: str, max_chars: int) -> str:
    """Truncate text at a sentence boundary near max_chars."""
    if len(text) <= max_chars:
        return text
    
    truncated = text[:max_chars]
    
    # Find the last sentence-ending punctuation
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


def build_extraction_prompt(markdown_text: str, source: str, report_type: str, report_date: Optional[str] = None, config: Optional[Dict[str, Any]] = None) -> str:
    """Build the AI extraction prompt for a report."""
    if report_date is None:
        report_date = date.today().strftime("%Y-%m-%d")

    if config is None:
        config = {}

    max_chars = config.get('extraction', {}).get('max_chars_per_report', 50000)
    truncate_sentence = config.get('extraction', {}).get('truncate_at_sentence', True)

    if truncate_sentence:
        report_text = _truncate_at_sentence(markdown_text, max_chars)
    else:
        report_text = markdown_text[:max_chars]

    return EXTRACTION_PROMPT_TEMPLATE.format(
        source=source,
        report_type=report_type,
        report_date=report_date,
        report_text=report_text
    )


def build_bull_bear_prompt(markdown_text: str, report_date: Optional[str] = None, config: Optional[Dict[str, Any]] = None) -> str:
    """Build extraction prompt for Zacks Bull & Bear."""
    if report_date is None:
        report_date = date.today().strftime("%Y-%m-%d")

    if config is None:
        config = {}
        
    max_chars = config.get('extraction', {}).get('max_chars_per_report', 50000)

    bull_bear_max = min(max_chars, 10000)  # Bull & Bear reports are shorter
    report_text = _truncate_at_sentence(markdown_text, bull_bear_max)

    return ZACKS_BULL_BEAR_PROMPT.format(
        report_text=report_text,
        report_date=report_date
    )


def parse_extraction_result(ai_output: str) -> Optional[Dict[str, Any]]:
    """
    Parse the AI extraction output, handling common formatting issues.

    Returns dict or None if parsing fails.
    """
    # Strip markdown code blocks if present
    text = ai_output.strip()
    if text.startswith('```'):
        # Remove ```json or ``` prefix and trailing ```
        text = re.sub(r'^```(?:json)?\s*\n?', '', text)
        text = re.sub(r'\n?```\s*$', '', text)

    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        # Try to find JSON in the response
        json_match = re.search(r'\{[\s\S]*\}', text)
        if json_match:
            try:
                return json.loads(json_match.group())
            except json.JSONDecodeError:
                pass
        logging.error(f"Failed to parse AI output: {e}")
        logging.error(f"Output was: {text[:500]}")
        return None


def validate_extraction(data: Any) -> List[str]:
    """
    Validate extracted data and return list of issues.
    """
    issues = []

    if not isinstance(data, dict):
        return ["Top-level output is not a JSON object"]

    required_keys = ['report_metadata', 'stock_actions']
    for key in required_keys:
        if key not in data:
            issues.append(f"Missing required key: {key}")

    if 'stock_actions' in data:
        if not isinstance(data['stock_actions'], list):
            issues.append("stock_actions must be an array")
        else:
            for i, action in enumerate(data['stock_actions']):
                if 'ticker' not in action:
                    issues.append(f"stock_actions[{i}] missing ticker")
                if 'rating' not in action and 'action' not in action:
                    issues.append(f"stock_actions[{i}] missing both rating and action")

    return issues


def extract_with_retry(markdown_text: str, source: str, report_type: str, report_date: str, llm_caller: Any, config: Optional[Dict[str, Any]] = None, max_retries: int = 1) -> Optional[Dict[str, Any]]:
    """
    Extract data using an LLM, and retry if validation fails.
    llm_caller must be a function taking (prompt: str) -> str
    """
    prompt = build_extraction_prompt(markdown_text, source, report_type, report_date, config)
    
    for attempt in range(max_retries + 1):
        raw_output = llm_caller(prompt)
        if not raw_output:
            logging.error("No output from LLM.")
            return None
            
        data = parse_extraction_result(raw_output)
        if not data:
            logging.error("Failed to parse LLM output as JSON.")
            if attempt < max_retries:
                prompt += "\n\nPREVIOUS ATTEMPT FAILED TO PRODUCE VALID JSON. PLEASE RETURN ONLY VALID JSON."
                continue
            return None
            
        issues = validate_extraction(data)
        if not issues:
            return data
            
        logging.error(f"Validation issues found: {issues}")
        if attempt < max_retries:
            issues_str = "\n- ".join(issues)
            prompt += f"\n\nYOUR PREVIOUS OUTPUT HAD THE FOLLOWING VALIDATION ERRORS:\n- {issues_str}\n\nPLEASE CORRECT THEM AND RETURN THE FULL JSON."
            continue
            
    return data
