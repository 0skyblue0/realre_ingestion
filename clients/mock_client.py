"""
Mock client for testing and development.

Provides dummy data when real API clients are unavailable.
"""

from __future__ import annotations

import random
import string
from datetime import datetime, timezone
from typing import Any


def _generate_tx_id() -> str:
    return "TX" + "".join(random.choices(string.ascii_uppercase + string.digits, k=8))


def fetch_transactions(limit: int = 5, **kwargs: Any) -> list[dict[str, Any]]:
    """
    Generate mock transaction records.

    Parameters
    ----------
    limit:
        Number of mock records to generate.

    Returns
    -------
    list[dict[str, Any]]
        List of transaction dictionaries with tx_id, amount, currency, updated_at.
    """
    now = datetime.now(timezone.utc).isoformat()
    records = []
    for _ in range(limit):
        records.append({
            "tx_id": _generate_tx_id(),
            "amount": str(round(random.uniform(100, 10000), 2)),
            "currency": random.choice(["KRW", "USD", "EUR"]),
            "updated_at": now,
        })
    return records


__all__ = ["fetch_transactions"]
