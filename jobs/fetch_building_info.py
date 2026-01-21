"""
Job to fetch building information from V-world API and store via SCD2.
"""

from __future__ import annotations

from typing import Any


def run(
    *,
    manager: Any,
    api_key: str,
    addresses: list[str],
    scd_table: str = "building_info_scd",
) -> dict[str, Any]:
    """
    Fetch building information for given addresses from V-world API.

    Parameters
    ----------
    manager:
        IngestionManager instance.
    api_key:
        V-world API key.
    addresses:
        List of addresses to search.
    scd_table:
        Target SCD2 table name.

    Returns
    -------
    dict[str, Any]
        Result summary with row_count.
    """
    records = []

    for address in addresses:
        # Step 1: Search address to get PNU
        search_result = manager.request_source_data(
            "vworld",
            "search_address",
            address=address,
            api_key=api_key,
            size=1,
        )

        items = search_result.get("result", {}).get("items", [])
        if not items:
            manager.logger.warning("No results for address: %s", address)
            continue

        item = items[0]
        pnu = item.get("id")
        addr_info = item.get("address", {})
        point = item.get("point", {})

        # Step 2: Get building age info
        try:
            building_result = manager.request_source_data(
                "vworld",
                "call_vworld_api",
                api_name="getBuildingAge",
                params={"pnu": pnu, "format": "json"},
                api_key=api_key,
                parse_json=True,
            )
            building_fields = building_result.get("buildingAges", {}).get("field", [])
            building = building_fields[0] if building_fields else {}
        except Exception as exc:
            manager.logger.warning("Failed to get building info for PNU %s: %s", pnu, exc)
            building = {}

        # Build record
        record = {
            "pnu": pnu,
            "road_address": addr_info.get("road", ""),
            "parcel_address": addr_info.get("parcel", ""),
            "building_name": building.get("buldNm") or addr_info.get("bldnm", ""),
            "zipcode": addr_info.get("zipcode", ""),
            "x": point.get("x", ""),
            "y": point.get("y", ""),
            "ground_floors": building.get("groundFloorCo", ""),
            "underground_floors": building.get("undgrndFloorCo", ""),
            "building_height": building.get("buldHg", ""),
            "total_area": building.get("buldTotar", ""),
            "building_age": building.get("buldAge", ""),
            "structure": building.get("strctCodeNm", ""),
            "main_use": building.get("mainPrposCodeNm", ""),
            "approval_date": building.get("useConfmDe", ""),
        }
        records.append(record)

    # Log data load
    manager.log_history(
        job_name="fetch_building_info",
        event_type="data_load",
        status="success",
        row_count=len(records),
        details={"addresses": addresses},
    )

    # Upsert to SCD2 table
    inserted = manager.upsert_scd2(
        table=scd_table,
        records=records,
        key_fields=["pnu"],
        attribute_fields=[
            "road_address", "parcel_address", "building_name", "zipcode",
            "x", "y", "ground_floors", "underground_floors", "building_height",
            "total_area", "building_age", "structure", "main_use", "approval_date",
        ],
    )

    manager.log_history(
        job_name="fetch_building_info",
        event_type="scd2_upsert",
        status="success",
        row_count=inserted,
        details={"table": scd_table},
    )

    return {"row_count": inserted, "table": scd_table, "total_addresses": len(addresses)}
