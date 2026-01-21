"""
Job registry mapping names to callables consumed by the ingestion manager.
"""

from . import fetch_DB_transactions
from . import fetch_building_info
from . import download_trade
from . import update_region_codes
from . import analyze_trade_data

JOB_REGISTRY = {
    "fetch_DB_transactions": fetch_DB_transactions.run,
    "fetch_building_info": fetch_building_info.run,
    "download_trade": download_trade.run,
    "update_region_codes": update_region_codes.run,
    "analyze_trade_data": analyze_trade_data.run,
}

__all__ = [
    "JOB_REGISTRY",
    "fetch_DB_transactions",
    "fetch_building_info",
    "download_trade",
    "update_region_codes",
    "analyze_trade_data",
]
