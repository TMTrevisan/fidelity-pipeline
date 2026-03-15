# Fidelity Research Pipeline - Code Review

## 1. Bugs (with file:line references)

*   **`scripts/orchestrator.py` & `scripts/summary.py` (Missing Data)**: 
    *   *Lines:* `orchestrator.py` (~line 223), `summary.py` (~line 46).
    *   *Issue:* The orchestrator stores `overall_sentiment` and `market_summary` inside the `reports.metadata` JSON column, but `summary.py` queries a completely separate, never-populated table called `market_sentiment`. Consequently, the market sentiment section in the generated Telegram summary will always be missing. Furthermore, the `key_themes` array extracted by the AI is completely discarded in `orchestrator.py` and never saved.
*   **`scripts/summary.py` (SQLite Syntax Error Risk)**: 
    *   *Lines:* `summary.py` (~line 182).
    *   *Issue:* In `_format_monthly_updates`, the code builds an `IN (...)` SQL clause dynamically using `.format(",".join("?" * len(...)))`. If `monthly_types.get(source, [])` is empty (e.g., if an unexpected source is passed), it generates `report_type IN ()`, which throws an `OperationalError` in SQLite.
*   **`scripts/orchestrator.py` (Stale Content Hash)**: 
    *   *Lines:* `orchestrator.py` (~line 221).
    *   *Issue:* In `store_extraction_results`, `store_report` is invoked with a hardcoded `content_hash="pending"`. This is never updated later in the script to reflect the actual file hash, breaking the deduplication logic in `db.py` (`is_already_downloaded`) for subsequent runs.
*   **`scripts/downloader.py` (Max Call Stack Exceeded Risk)**:
    *   *Lines:* `downloader.py` (~line 104).
    *   *Issue:* The chunking logic in the injected JavaScript (`String.fromCharCode.apply(null, chunk)`) can throw a "Maximum call stack size exceeded" error in the browser if `chunkSize` interacts poorly with the engine limits on large PDFs, causing the download step to fail silently or crash the injected page context.

## 2. Tactical Fixes Needed

*   **Fix Sentiment Retrieval**: Update `summary.py` to extract sentiment data directly from `reports.metadata` using Python's `json.loads()` on the metadata column, rather than querying the unused `market_sentiment` table. Also, ensure `key_themes` is included in the metadata JSON payload in `orchestrator.py`.
*   **Fix SQLite Query Issue**: Add an early return guard in `summary.py`'s `_format_monthly_updates`:
    ```python
    types = monthly_types.get(source, [])
    if not types:
        return ""
    ```
*   **Pass Actual File Hash**: Modify `store_extraction_results` in `orchestrator.py` to accept the actual `content_hash` of the processed PDF (which is already calculated in `process_downloaded_pdfs`) instead of hardcoding `"pending"`.
*   **Improve PDF Ligature Cleanup**: The ligature cleanup in `pdf_converter.py` only handles `fi` and `fl`. Add the rest of the standard ligatures (`ff`, `ffi`, `ffl`) or rely entirely on `pymupdf4llm`'s native processing. Otherwise, tickers and company names can be corrupted during extraction (e.g., "Affirm" becoming "Airm").
*   **Add Extraction Validation Retry**: In `extractor.py`, if `validate_extraction` finds missing keys, the pipeline simply continues with malformed data. For a scheduled job, passing the validation errors back to the LLM for a single correction loop will drastically improve reliability.

## 3. Enhancement Opportunities

*   **Implement Proper Logging**: The entire pipeline relies on standard `print()` statements. Since this is designed to be a cron job, replace prints with the standard Python `logging` module to allow proper timestamping, log levels, and log file rotation.
*   **Optimize Configuration Loading**: `load_config()` is called repeatedly and redundantly across different modules and functions (e.g., opening `config.json` multiple times per run). Load the configuration once at the orchestrator level and pass it down as a dependency to avoid repeated disk I/O.
*   **Add Type Hinting**: Adding Python type hints (e.g., `-> dict`, `: List[str]`) would significantly improve code maintainability and catch structural mismatches (like the `market_sentiment` table mismatch) earlier via static analysis tools like `mypy`.
*   **Avoid Base64 Transfer for PDFs**: In `downloader.py`, PDFs are converted to Base64 in JavaScript and passed back via CDP/OpenClaw. For large reports, this is highly inefficient and increases memory overhead. A better approach is using `Page.setDownloadBehavior` in CDP to let the browser save the PDF directly to disk without loading the binary into the JavaScript context.

## 4. Architecture Concerns

*   **Brittle Browser Orchestration**: The system relies on stringified JavaScript strings (`build_browser_download_script`) passed to OpenClaw to bypass Akamai bot protection. This creates a severe disconnect between the Python logic and the actual execution environment. A much more robust pattern is to integrate an automation framework directly into Python (like Playwright with the `playwright-stealth` plugin) to handle the bypass and downloads natively.
*   **Lossy Truncation for LLMs**: In `extractor.py`, `_truncate_at_sentence` uses a hard character limit (`max_chars`). If a report is extremely long, any stock actions mentioned at the end of the document will be silently dropped without warning. The architecture should shift from naive truncation to a MapReduce pattern: chunk the document, run the extraction prompt on each chunk independently, and combine the JSON results.
*   **Database Schema & Statefulness**:
    *   **Primary Keys**: Using string concatenation for primary keys (e.g., `f"{source}-{date_str}-{report_type}"`) is fragile and can lead to silent collisions or parsing bugs if the data contains hyphens. Auto-incrementing integers or UUIDs are standard and safer.
    *   **Migrations**: The schema includes a `schema_version` table but lacks any application logic to apply future migrations if the schema changes.
    *   **Stateful Storage**: Storing PDFs and Markdown files locally in the `downloads/` and `reports/` directories makes the application stateful. Moving these artifacts to a cloud object store (like S3) would make the pipeline truly container-friendly and scalable.
