"""
Job to analyze land trade data.

Analyzes downloaded land trade data and extracts failed records
to a separate file for review.

NOTE: Analysis algorithms are to be implemented. Currently this is a skeleton.
"""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Any


def analyze_record(record: dict[str, Any]) -> tuple[bool, str | None]:
    """
    Analyze a single trade record.

    Parameters
    ----------
    record:
        Trade record to analyze.

    Returns
    -------
    tuple[bool, str | None]
        (success, error_message) - True if valid, False with reason if invalid.

    TODO
    ----
    Implement actual analysis logic:
    - Data validation (required fields, format checks)
    - Outlier detection (price anomalies, area anomalies)
    - Business rule validation (reasonable price ranges, valid dates)
    - Cross-reference validation (region codes, building types)
    """
    # TODO: Implement analysis algorithm
    # Current implementation: pass all records as valid
    #
    # Example checks to implement:
    # 1. Required fields present
    # required_fields = ["거래금액", "법정동", "지번"]
    # for field in required_fields:
    #     if not record.get(field):
    #         return False, f"Missing required field: {field}"
    #
    # 2. Price validation
    # price = record.get("거래금액", "").replace(",", "")
    # if price and int(price) <= 0:
    #     return False, "Invalid price: must be positive"
    #
    # 3. Date validation
    # deal_year = record.get("년")
    # deal_month = record.get("월")
    # if not deal_year or not deal_month:
    #     return False, "Missing deal date"

    return True, None


def run(
    *,
    manager: Any,
    input_dir: str = "output/land_trade",
    failed_output_dir: str = "output/failed_records",
    input_pattern: str = "land_trade_all_*.csv",
) -> dict[str, Any]:
    """
    Analyze trade data and extract failed records.

    Parameters
    ----------
    manager:
        IngestionManager instance.
    input_dir:
        Directory containing input CSV files.
    failed_output_dir:
        Directory to save failed records.
    input_pattern:
        Glob pattern to match input files.

    Returns
    -------
    dict[str, Any]
        Analysis results summary.
    """
    job_name = "analyze_trade_data"

    manager.logger.info("Starting trade data analysis")

    input_path = Path(input_dir)
    failed_path = Path(failed_output_dir)
    failed_path.mkdir(parents=True, exist_ok=True)

    # Find the latest input file
    files = sorted(input_path.glob(input_pattern), reverse=True)
    if not files:
        manager.logger.warning(
            "No input files found matching %s in %s",
            input_pattern, input_dir
        )
        return {
            "total_count": 0,
            "success_count": 0,
            "failed_count": 0,
            "status": "no_input",
        }

    input_file = files[0]
    manager.logger.info("Analyzing file: %s", input_file)

    # Process records
    success_count = 0
    failed_records: list[dict[str, Any]] = []
    fieldnames: list[str] = []

    try:
        with open(input_file, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            fieldnames = list(reader.fieldnames or [])

            for row_num, record in enumerate(reader, start=2):  # Start at 2 (header is 1)
                success, error_msg = analyze_record(record)

                if success:
                    success_count += 1
                else:
                    # Add metadata to failed record
                    record["_error_message"] = error_msg
                    record["_row_number"] = row_num
                    record["_source_file"] = str(input_file)
                    failed_records.append(record)

    except Exception as exc:
        manager.logger.error("Failed to read input file: %s", exc)
        manager.log_history(
            job_name=job_name,
            event_type="read_error",
            status="failed",
            details={"error": str(exc), "input_file": str(input_file)},
        )
        raise

    total_count = success_count + len(failed_records)

    manager.log_history(
        job_name=job_name,
        event_type="analysis_complete",
        status="success",
        row_count=total_count,
        details={
            "success_count": success_count,
            "failed_count": len(failed_records),
            "input_file": str(input_file),
        },
    )

    manager.logger.info(
        "Analysis complete: %d total, %d passed, %d failed",
        total_count, success_count, len(failed_records)
    )

    # Save failed records if any
    failed_filepath = None
    if failed_records:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        failed_filename = f"failed_records_{timestamp}.csv"
        failed_filepath = failed_path / failed_filename

        # Add metadata columns to fieldnames
        failed_fieldnames = fieldnames + ["_error_message", "_row_number", "_source_file"]

        with open(failed_filepath, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=failed_fieldnames)
            writer.writeheader()
            writer.writerows(failed_records)

        manager.log_history(
            job_name=job_name,
            event_type="failed_records_saved",
            status="success",
            row_count=len(failed_records),
            details={"output_path": str(failed_filepath)},
        )

        manager.logger.info(
            "Saved %d failed records to %s",
            len(failed_records), failed_filepath
        )

    return {
        "total_count": total_count,
        "success_count": success_count,
        "failed_count": len(failed_records),
        "input_file": str(input_file),
        "failed_output_path": str(failed_filepath) if failed_filepath else None,
    }
