"""
Job to update legal district codes (법정동 코드) from data.go.kr API.

Scheduled to run monthly to keep region codes current.
"""

from __future__ import annotations

from typing import Any

from clients.opendata_client import OpenDataClient, OpenDataAPIError


def run(
    *,
    manager: Any,
    output_table: str = "region_codes",
) -> dict[str, Any]:
    """
    Update legal district code list and store in database.

    Parameters
    ----------
    manager:
        IngestionManager instance.
    output_table:
        Target table name for SCD2 storage.

    Returns
    -------
    dict[str, Any]
        Result summary with row_count and table name.
    """
    job_name = "update_region_codes"

    manager.logger.info("Starting region code update")

    try:
        # Get API key from KeyManager
        service_key = manager.get_api_key("opendata_service_key")

        # Create client and fetch data
        client = OpenDataClient(service_key=service_key)

        def on_page(page_no: int, count: int, total: int) -> None:
            manager.logger.info(
                "Fetched page %d: %d records (total: %d)",
                page_no, count, total
            )

        records = client.fetch_region_codes(on_page=on_page)

        manager.log_history(
            job_name=job_name,
            event_type="data_load",
            status="success",
            row_count=len(records),
        )

        manager.logger.info("Fetched %d region codes", len(records))

        # Store using SCD2 pattern
        # Key field is region_cd (10-digit code)
        # Other fields are attributes that may change
        inserted = manager.upsert_scd2(
            table=output_table,
            records=records,
            key_fields=["region_cd"],
            attribute_fields=[
                "sido_cd",
                "sgg_cd",
                "umd_cd",
                "ri_cd",
                "locatjumin_cd",
                "locatjijuk_cd",
                "locatadd_nm",
                "locat_order",
                "locat_rm",
                "locathigh_cd",
                "locallow_nm",
                "adpt_de",
            ],
        )

        manager.log_history(
            job_name=job_name,
            event_type="scd2_upsert",
            status="success",
            row_count=inserted,
            details={"table": output_table},
        )

        manager.logger.info(
            "Updated %d region codes in table %s",
            inserted, output_table
        )

        return {
            "row_count": inserted,
            "table": output_table,
            "total_fetched": len(records),
        }

    except OpenDataAPIError as exc:
        manager.logger.error("API error: %s", exc)
        manager.log_history(
            job_name=job_name,
            event_type="api_error",
            status="failed",
            details={"error": str(exc)},
        )
        raise
