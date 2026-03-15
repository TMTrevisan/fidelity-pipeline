# Fidelity Research Pipeline - Fixes Applied

1. **Fix sentiment data flow**: Updated `summary.py` to query `reports.metadata` instead of the empty `market_sentiment` table.
2. **Fix empty IN() SQL crash**: Added guard `if not types: return ""` to `_format_monthly_updates` in `summary.py`.
3. **Fix stale content hash**: Added `file_hash` argument to `store_extraction_results` in `orchestrator.py` and passed it from CLI args so dedup works.
4. **Add ligature cleanup**: Updated `pdf_converter.py` to replace `ff`, `ffi`, `ffl` ligatures to prevent corruption.
5. **Add LLM validation retry**: Added an `extract_with_retry` wrapper in `extractor.py` that checks `validate_extraction` results and provides validation errors back in the retry prompt.
6. **Replace sys.exit with ImportError**: Replaced `sys.exit(1)` with `raise ImportError(...)` in `pdf_converter.py` to allow graceful handling.
7. **Fix config loading**: Updated `build_extraction_prompt` in `extractor.py` to accept `config` as a parameter instead of re-importing the DB locally.
8. **Add key_themes to storage**: Updated `store_extraction_results` in `orchestrator.py` to capture and store `key_themes` within `reports.metadata`.
9. **Implement Python logging**: Replaced all `print()` statements with `logging.info()`, `logging.warning()`, etc., across the modified scripts. Added basic logging configuration in `orchestrator.py`.
10. **Add type hints**: Added comprehensive type hints to all function signatures in the modified scripts for better maintainability.
