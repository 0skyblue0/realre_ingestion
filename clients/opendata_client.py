"""
Client for Korea Public Data Portal (data.go.kr) OpenAPI.

This module provides a reusable client for interacting with various data.go.kr
APIs, with built-in support for XML response parsing, pagination, and error handling.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

import requests

__all__ = [
    "OpenDataAPIError",
    "OpenDataClient",
    "fetch_land_trade",
    "fetch_region_codes",
]

# Default configuration
DEFAULT_TIMEOUT = 30.0
DEFAULT_NUM_OF_ROWS = 1000

# API Endpoints
LAND_TRADE_ENDPOINT = "http://apis.data.go.kr/1613000/RTMSDataSvcLandTrade/getRTMSDataSvcLandTrade"
REGION_CODE_ENDPOINT = "https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"


class OpenDataAPIError(RuntimeError):
    """Raised when a data.go.kr API request fails."""


def _parse_xml_items(xml_text: str) -> tuple[list[dict[str, Any]], int]:
    """
    Parse XML response from data.go.kr API.

    Parameters
    ----------
    xml_text:
        Raw XML response text.

    Returns
    -------
    tuple[list[dict[str, Any]], int]
        List of item records and total count.

    Raises
    ------
    OpenDataAPIError
        If XML parsing fails or API returns an error code.
    """
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as exc:
        raise OpenDataAPIError(f"Failed to parse XML response: {exc}") from exc

    # Check response header for errors
    header = root.find(".//header")
    if header is not None:
        result_code = header.findtext("resultCode", "")
        result_msg = header.findtext("resultMsg", "")
        # data.go.kr uses "00" or "000" as success codes
        if result_code not in ("00", "000"):
            raise OpenDataAPIError(f"API error [{result_code}]: {result_msg}")

    # Get total count from body
    total_count = int(root.findtext(".//body/totalCount", "0"))

    # Parse items
    records = []
    items = root.findall(".//items/item")
    for item in items:
        record = {}
        for child in item:
            tag = child.tag.strip()
            text = (child.text or "").strip()
            record[tag] = text
        if record:
            records.append(record)

    return records, total_count


@dataclass
class OpenDataClient:
    """
    Client for Korea Public Data Portal (data.go.kr) APIs.

    Parameters
    ----------
    service_key:
        API service key issued by data.go.kr.
    timeout:
        Default request timeout in seconds.
    num_of_rows:
        Default number of rows per page for paginated requests.

    Examples
    --------
    >>> client = OpenDataClient(service_key="YOUR_KEY")
    >>> records = client.fetch_land_trade(lawd_cd="11110", deal_ymd="202401")
    """

    service_key: str
    timeout: float = DEFAULT_TIMEOUT
    num_of_rows: int = DEFAULT_NUM_OF_ROWS
    _session: requests.Session = field(default_factory=requests.Session, repr=False)

    def _request(
        self,
        endpoint: str,
        params: Mapping[str, Any],
        *,
        timeout: float | None = None,
    ) -> str:
        """
        Perform HTTP GET request to the specified endpoint.

        Parameters
        ----------
        endpoint:
            API endpoint URL.
        params:
            Query parameters.
        timeout:
            Request timeout (uses instance default if None).

        Returns
        -------
        str
            Response text.

        Raises
        ------
        OpenDataAPIError
            If the request fails.
        """
        request_timeout = timeout if timeout is not None else self.timeout

        # Build query params with service key
        query_params = {"serviceKey": self.service_key, **params}

        try:
            response = self._session.get(
                endpoint,
                params=query_params,
                timeout=request_timeout,
            )
            response.raise_for_status()
        except requests.exceptions.Timeout as exc:
            raise OpenDataAPIError(
                f"Request timeout after {request_timeout}s"
            ) from exc
        except requests.exceptions.RequestException as exc:
            raise OpenDataAPIError(f"Request failed: {exc}") from exc

        return response.text

    def _fetch_paginated(
        self,
        endpoint: str,
        params: Mapping[str, Any],
        *,
        num_of_rows: int | None = None,
        timeout: float | None = None,
        on_page: Callable[[int, int, int], None] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch all pages from a paginated API endpoint.

        Parameters
        ----------
        endpoint:
            API endpoint URL.
        params:
            Base query parameters (excluding pagination params).
        num_of_rows:
            Number of rows per page (uses instance default if None).
        timeout:
            Request timeout per page.
        on_page:
            Optional callback called after each page: on_page(page_no, page_count, total_count).

        Returns
        -------
        list[dict[str, Any]]
            All fetched records across all pages.
        """
        rows_per_page = num_of_rows if num_of_rows is not None else self.num_of_rows
        all_records: list[dict[str, Any]] = []
        page_no = 1

        while True:
            page_params = {
                **params,
                "pageNo": page_no,
                "numOfRows": rows_per_page,
            }

            response_text = self._request(endpoint, page_params, timeout=timeout)
            records, total_count = _parse_xml_items(response_text)
            all_records.extend(records)

            if on_page is not None:
                on_page(page_no, len(records), total_count)

            # Check if we have fetched all records
            if len(all_records) >= total_count or len(records) == 0:
                break

            page_no += 1

        return all_records

    def fetch_land_trade(
        self,
        lawd_cd: str,
        deal_ymd: str,
        *,
        num_of_rows: int | None = None,
        timeout: float | None = None,
        on_page: Callable[[int, int, int], None] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch land trade (real transaction price) data.

        Parameters
        ----------
        lawd_cd:
            5-digit legal district code (법정동코드 앞 5자리).
        deal_ymd:
            6-digit year-month (YYYYMM).
        num_of_rows:
            Number of rows per page.
        timeout:
            Request timeout in seconds.
        on_page:
            Optional callback for progress: on_page(page_no, page_count, total_count).

        Returns
        -------
        list[dict[str, Any]]
            List of land trade records.

        Raises
        ------
        OpenDataAPIError
            If the API request fails.

        Examples
        --------
        >>> client = OpenDataClient(service_key="YOUR_KEY")
        >>> records = client.fetch_land_trade("11110", "202401")
        >>> print(f"Fetched {len(records)} records")
        """
        params = {
            "LAWD_CD": lawd_cd,
            "DEAL_YMD": deal_ymd,
        }

        return self._fetch_paginated(
            LAND_TRADE_ENDPOINT,
            params,
            num_of_rows=num_of_rows,
            timeout=timeout,
            on_page=on_page,
        )

    def fetch_region_codes(
        self,
        *,
        locatadd_nm: str | None = None,
        num_of_rows: int | None = None,
        timeout: float | None = None,
        on_page: Callable[[int, int, int], None] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch legal district code (법정동 코드) list.

        Parameters
        ----------
        locatadd_nm:
            Region name filter (e.g., "서울", "경기").
        num_of_rows:
            Number of rows per page.
        timeout:
            Request timeout in seconds.
        on_page:
            Optional callback for progress: on_page(page_no, page_count, total_count).

        Returns
        -------
        list[dict[str, Any]]
            List of region code records with fields:
            - region_cd: 10-digit legal district code
            - sido_cd: 2-digit province code
            - sgg_cd: 3-digit city/county code
            - umd_cd: 3-digit district code
            - ri_cd: 2-digit sub-district code
            - locatadd_nm: Full address name
            - locallow_nm: Lowest level name
            - adpt_de: Creation date
        """
        params: dict[str, Any] = {"type": "xml"}
        if locatadd_nm:
            params["locatadd_nm"] = locatadd_nm

        return self._fetch_paginated(
            REGION_CODE_ENDPOINT,
            params,
            num_of_rows=num_of_rows,
            timeout=timeout,
            on_page=on_page,
        )

    def fetch_region_codes_5digit(
        self,
        **kwargs: Any,
    ) -> list[dict[str, str]]:
        """
        Extract unique 5-digit legal district codes (시군구 단위).

        Parameters
        ----------
        **kwargs:
            Passed to fetch_region_codes().

        Returns
        -------
        list[dict[str, str]]
            List of unique 5-digit codes:
            [{"code": "11110", "name": "서울특별시 종로구", "sido_cd": "11", "sgg_cd": "110"}, ...]
        """
        records = self.fetch_region_codes(**kwargs)

        # Extract unique 5-digit codes (시군구 level)
        seen: set[str] = set()
        result: list[dict[str, str]] = []

        for record in records:
            full_code = record.get("region_cd", "")
            if len(full_code) >= 5:
                code_5 = full_code[:5]
                if code_5 not in seen:
                    seen.add(code_5)
                    result.append({
                        "code": code_5,
                        "name": record.get("locatadd_nm", ""),
                        "sido_cd": record.get("sido_cd", ""),
                        "sgg_cd": record.get("sgg_cd", ""),
                    })

        return result


# Module-level convenience function
def fetch_land_trade(
    service_key: str,
    lawd_cd: str,
    deal_ymd: str,
    *,
    num_of_rows: int = DEFAULT_NUM_OF_ROWS,
    timeout: float = DEFAULT_TIMEOUT,
    on_page: Callable[[int, int, int], None] | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch land trade data using a one-off client.

    This is a convenience function for simple use cases. For multiple API calls,
    prefer creating an :class:`OpenDataClient` instance to reuse the session.

    Parameters
    ----------
    service_key:
        API service key issued by data.go.kr.
    lawd_cd:
        5-digit legal district code.
    deal_ymd:
        6-digit year-month (YYYYMM).
    num_of_rows:
        Number of rows per page.
    timeout:
        Request timeout in seconds.
    on_page:
        Optional callback for progress.

    Returns
    -------
    list[dict[str, Any]]
        List of land trade records.
    """
    client = OpenDataClient(
        service_key=service_key,
        timeout=timeout,
        num_of_rows=num_of_rows,
    )
    return client.fetch_land_trade(
        lawd_cd=lawd_cd,
        deal_ymd=deal_ymd,
        on_page=on_page,
    )


def fetch_region_codes(
    service_key: str,
    *,
    locatadd_nm: str | None = None,
    num_of_rows: int = DEFAULT_NUM_OF_ROWS,
    timeout: float = DEFAULT_TIMEOUT,
    on_page: Callable[[int, int, int], None] | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch region codes using a one-off client.

    This is a convenience function for simple use cases. For multiple API calls,
    prefer creating an :class:`OpenDataClient` instance to reuse the session.

    Parameters
    ----------
    service_key:
        API service key issued by data.go.kr.
    locatadd_nm:
        Region name filter.
    num_of_rows:
        Number of rows per page.
    timeout:
        Request timeout in seconds.
    on_page:
        Optional callback for progress.

    Returns
    -------
    list[dict[str, Any]]
        List of region code records.
    """
    client = OpenDataClient(
        service_key=service_key,
        timeout=timeout,
        num_of_rows=num_of_rows,
    )
    return client.fetch_region_codes(
        locatadd_nm=locatadd_nm,
        on_page=on_page,
    )
