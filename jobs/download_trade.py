"""
Job to fetch land trade (real transaction price) data from data.go.kr API.

Supports two modes:
1. Single region mode: Fetch data for a specific region (lawd_cd + deal_ymd)
2. Full traversal mode: Fetch data for all regions stored in region_codes table

OpenAPI: apis.data.go.kr/1613000/RTMSDataSvcLandTrade/getRTMSDataSvcLandTrade
"""

from __future__ import annotations

import argparse
import csv
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from clients.opendata_client import (
    OpenDataAPIError,
    OpenDataClient,
    fetch_land_trade as _fetch_land_trade,
    DEFAULT_TIMEOUT,
    DEFAULT_NUM_OF_ROWS,
)


def _get_deal_ymd(months_ago: int = 0) -> str:
    """Get YYYYMM format for N months ago."""
    from dateutil.relativedelta import relativedelta
    target = datetime.now() - relativedelta(months=months_ago)
    return target.strftime("%Y%m")


def _get_region_codes_from_db(manager: Any) -> list[str]:
    """
    Fetch unique 5-digit region codes from database.

    Queries the region_codes table for current valid codes.
    """
    query = """
        SELECT DISTINCT SUBSTRING(region_cd, 1, 5) as code
        FROM region_codes
        WHERE is_current = 1
        ORDER BY code
    """
    try:
        rows = manager.execute_query(query)
        return [row["code"] for row in rows]
    except Exception as exc:
        manager.logger.warning("Failed to fetch region codes from DB: %s", exc)
        return []


def save_to_csv(
    records: list[dict[str, Any]],
    output_dir: str | Path,
    lawd_cd: str,
    deal_ymd: str,
) -> Path:
    """
    Save records to CSV file.

    Parameters
    ----------
    records:
        List of records to save.
    output_dir:
        Output directory path.
    lawd_cd:
        Legal district code (included in filename).
    deal_ymd:
        Deal year-month (included in filename).

    Returns
    -------
    Path
        Path to the saved CSV file.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"land_trade_{lawd_cd}_{deal_ymd}_{timestamp}.csv"
    filepath = output_path / filename

    if not records:
        # Create empty file with no columns
        filepath.write_text("", encoding="utf-8-sig")
        return filepath

    # Collect all unique field names across all records
    fieldnames = []
    seen = set()
    for record in records:
        for key in record.keys():
            if key not in seen:
                fieldnames.append(key)
                seen.add(key)

    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    return filepath


def save_all_to_csv(
    records: list[dict[str, Any]],
    output_dir: str | Path,
) -> Path:
    """
    Save all records to a single CSV file.

    Parameters
    ----------
    records:
        All collected records.
    output_dir:
        Output directory path.

    Returns
    -------
    Path
        Path to the saved CSV file.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"land_trade_all_{timestamp}.csv"
    filepath = output_path / filename

    if not records:
        filepath.write_text("", encoding="utf-8-sig")
        return filepath

    # Collect all unique field names
    fieldnames = list(dict.fromkeys(
        key for record in records for key in record.keys()
    ))

    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    return filepath


def run(
    *,
    manager: Any,
    # Single region mode parameters
    service_key: str | None = None,
    lawd_cd: str | None = None,
    deal_ymd: str | None = None,
    # Full traversal mode parameters
    output_dir: str = "output/land_trade",
    deal_months: int = 1,
    batch_size: int = 10,
    num_of_rows: int = DEFAULT_NUM_OF_ROWS,
    timeout: float = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """
    Fetch land trade data and save as CSV.

    Operates in two modes:
    1. Single region mode: If lawd_cd and deal_ymd are provided, fetch that specific data.
    2. Full traversal mode: If lawd_cd is not provided, fetch data for all regions in DB.

    Parameters
    ----------
    manager:
        IngestionManager instance.
    service_key:
        Public data portal service key (optional, uses KeyManager if not provided).
    lawd_cd:
        5-digit legal district code (optional, triggers single region mode).
    deal_ymd:
        6-digit year-month (YYYYMM) (optional).
    output_dir:
        Directory to save CSV files.
    deal_months:
        Number of months to fetch in full traversal mode.
    batch_size:
        Progress logging interval (every N regions).
    num_of_rows:
        Number of rows per API request.
    timeout:
        Request timeout in seconds.

    Returns
    -------
    dict[str, Any]
        Result summary with row_count and output_path.
    """
    job_name = "download_trade"

    # Get service key from KeyManager if not provided
    if service_key is None:
        service_key = manager.get_api_key("opendata_service_key")

    # Determine mode based on parameters
    if lawd_cd is not None:
        # Single region mode
        return _run_single_region(
            manager=manager,
            service_key=service_key,
            lawd_cd=lawd_cd,
            deal_ymd=deal_ymd or _get_deal_ymd(0),
            output_dir=output_dir,
            num_of_rows=num_of_rows,
            timeout=timeout,
            job_name=job_name,
        )
    else:
        # Full traversal mode
        return _run_full_traversal(
            manager=manager,
            service_key=service_key,
            output_dir=output_dir,
            deal_months=deal_months,
            batch_size=batch_size,
            num_of_rows=num_of_rows,
            timeout=timeout,
            job_name=job_name,
        )


def _run_single_region(
    *,
    manager: Any,
    service_key: str,
    lawd_cd: str,
    deal_ymd: str,
    output_dir: str,
    num_of_rows: int,
    timeout: float,
    job_name: str,
) -> dict[str, Any]:
    """Run single region fetch mode."""
    manager.logger.info(
        "Starting land trade fetch: LAWD_CD=%s, DEAL_YMD=%s",
        lawd_cd, deal_ymd
    )

    try:
        # Create client and fetch data
        client = OpenDataClient(
            service_key=service_key,
            timeout=timeout,
            num_of_rows=num_of_rows,
        )

        def on_page(page_no: int, page_count: int, total_count: int) -> None:
            manager.logger.info(
                "Fetched page %d: %d records (total: %d)",
                page_no, page_count, total_count
            )

        records = client.fetch_land_trade(
            lawd_cd=lawd_cd,
            deal_ymd=deal_ymd,
            on_page=on_page,
        )

        # Log data load
        manager.log_history(
            job_name=job_name,
            event_type="data_load",
            status="success",
            row_count=len(records),
            details={"lawd_cd": lawd_cd, "deal_ymd": deal_ymd},
        )

        # Save to CSV
        output_path = save_to_csv(records, output_dir, lawd_cd, deal_ymd)

        manager.log_history(
            job_name=job_name,
            event_type="csv_save",
            status="success",
            row_count=len(records),
            details={"output_path": str(output_path)},
        )

        manager.logger.info(
            "Saved %d records to %s", len(records), output_path
        )

        return {
            "row_count": len(records),
            "output_path": str(output_path),
            "lawd_cd": lawd_cd,
            "deal_ymd": deal_ymd,
        }

    except OpenDataAPIError as exc:
        manager.logger.error("API error: %s", exc)
        manager.log_history(
            job_name=job_name,
            event_type="api_error",
            status="failed",
            details={"error": str(exc), "lawd_cd": lawd_cd, "deal_ymd": deal_ymd},
        )
        raise


def _run_full_traversal(
    *,
    manager: Any,
    service_key: str,
    output_dir: str,
    deal_months: int,
    batch_size: int,
    num_of_rows: int,
    timeout: float,
    job_name: str,
) -> dict[str, Any]:
    """Run full region traversal mode."""
    manager.logger.info(
        "Starting full land trade download (months=%d)",
        deal_months
    )

    # Get region codes from database
    region_codes = _get_region_codes_from_db(manager)
    if not region_codes:
        manager.logger.warning("No region codes found in database. Run update_region_codes first.")
        return {
            "row_count": 0,
            "status": "no_region_codes",
            "message": "No region codes in database. Run update_region_codes job first.",
        }

    manager.logger.info("Found %d region codes", len(region_codes))

    # Generate deal_ymd list
    deal_ymds = [_get_deal_ymd(i) for i in range(deal_months)]
    manager.logger.info("Fetching data for months: %s", deal_ymds)

    # Create client
    client = OpenDataClient(
        service_key=service_key,
        timeout=timeout,
        num_of_rows=num_of_rows,
    )

    # Collect all records
    all_records: list[dict[str, Any]] = []
    failed_regions: list[dict[str, Any]] = []

    for idx, code in enumerate(region_codes, 1):
        for deal_ymd in deal_ymds:
            try:
                records = client.fetch_land_trade(
                    lawd_cd=code,
                    deal_ymd=deal_ymd,
                )

                # Add metadata to each record
                for record in records:
                    record["_lawd_cd"] = code
                    record["_deal_ymd"] = deal_ymd
                    record["_fetched_at"] = datetime.now().isoformat()

                all_records.extend(records)

            except OpenDataAPIError as exc:
                manager.logger.warning(
                    "Failed to fetch %s/%s: %s",
                    code, deal_ymd, exc
                )
                failed_regions.append({
                    "code": code,
                    "deal_ymd": deal_ymd,
                    "error": str(exc),
                })

        # Progress logging
        if idx % batch_size == 0:
            manager.logger.info(
                "Progress: %d/%d regions processed (%d records)",
                idx, len(region_codes), len(all_records)
            )

    manager.log_history(
        job_name=job_name,
        event_type="data_load",
        status="success",
        row_count=len(all_records),
        details={
            "regions_processed": len(region_codes),
            "regions_failed": len(failed_regions),
            "deal_months": deal_months,
        },
    )

    # Save all records to single file
    output_path = save_all_to_csv(all_records, output_dir)

    manager.log_history(
        job_name=job_name,
        event_type="csv_save",
        status="success",
        row_count=len(all_records),
        details={"output_path": str(output_path)},
    )

    manager.logger.info(
        "Saved %d records to %s",
        len(all_records), output_path
    )

    return {
        "row_count": len(all_records),
        "output_path": str(output_path),
        "regions_processed": len(region_codes),
        "regions_failed": len(failed_regions),
        "failed_regions": failed_regions,
    }


def main() -> None:
    """
    CLI entry point for standalone execution.

    Examples
    --------
    # Using command line arguments
    python -m jobs.download_trade --service-key YOUR_KEY --lawd-cd 11110 --deal-ymd 202401

    # Using environment variables
    export LAND_TRADE_SERVICE_KEY=YOUR_KEY
    python -m jobs.download_trade --lawd-cd 11110 --deal-ymd 202401
    """
    parser = argparse.ArgumentParser(
        description="Fetch land trade data from data.go.kr API and save as CSV."
    )
    parser.add_argument(
        "--service-key",
        default=os.environ.get("LAND_TRADE_SERVICE_KEY", ""),
        help="Public data portal service key (or set LAND_TRADE_SERVICE_KEY env var)",
    )
    parser.add_argument(
        "--lawd-cd",
        required=True,
        help="5-digit legal district code (법정동코드 앞 5자리)",
    )
    parser.add_argument(
        "--deal-ymd",
        required=True,
        help="6-digit year-month (YYYYMM)",
    )
    parser.add_argument(
        "--output-dir",
        default="output/land_trade",
        help="Output directory for CSV files",
    )
    parser.add_argument(
        "--num-of-rows",
        type=int,
        default=DEFAULT_NUM_OF_ROWS,
        help="Number of rows per API request",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        help="Request timeout in seconds",
    )

    args = parser.parse_args()

    if not args.service_key:
        parser.error("--service-key is required (or set LAND_TRADE_SERVICE_KEY env var)")

    # Simple logging for standalone execution
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger(__name__)

    logger.info("Fetching land trade data: LAWD_CD=%s, DEAL_YMD=%s", args.lawd_cd, args.deal_ymd)

    def on_page(page_no: int, page_count: int, total_count: int) -> None:
        logger.info("Fetched page %d: %d records (total: %d)", page_no, page_count, total_count)

    try:
        records = _fetch_land_trade(
            service_key=args.service_key,
            lawd_cd=args.lawd_cd,
            deal_ymd=args.deal_ymd,
            num_of_rows=args.num_of_rows,
            timeout=args.timeout,
            on_page=on_page,
        )

        output_path = save_to_csv(records, args.output_dir, args.lawd_cd, args.deal_ymd)

        logger.info("Saved %d records to %s", len(records), output_path)

    except OpenDataAPIError as exc:
        logger.error("Failed: %s", exc)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
