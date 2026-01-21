"""
Job registry mapping names to callables consumed by the ingestion manager.
"""

from . import fetch_transactions
from . import fetch_building_info

JOB_REGISTRY = {
    "fetch_transactions": fetch_transactions.run,
    "fetch_building_info": fetch_building_info.run,
}

__all__ = ["JOB_REGISTRY", "fetch_transactions", "fetch_building_info"]
