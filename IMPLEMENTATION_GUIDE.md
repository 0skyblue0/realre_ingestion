# Implementation Guide: Ingestion Manager Enhancement

## Overview

이 문서는 realre_ingestion 프로젝트의 기능 확장을 위한 구현 가이드입니다. Claude AI는 이 지침을 따라 코드를 작성해야 합니다.

---

## 1. CLI 최소화 및 JSON 기반 설정 체계

### 목표
- 개별 Job의 CLI 인터페이스를 제거하고, 모든 설정을 JSON 파일로 관리
- `Ingestion_Manager.py` 실행 시 필요한 argument는 최소화 (passphrase만 필수)

### 구현 요구사항

#### 1.1 Ingestion_Manager.py 수정

```python
# 최소화된 CLI 인터페이스
python Ingestion_Manager.py --passphrase <passphrase>

# 선택적 옵션
python Ingestion_Manager.py --passphrase <passphrase> --config config/settings.json
```

**필수 Arguments:**
- `--passphrase`: key_manager 복호화용 패스프레이즈 (필수)

**선택적 Arguments:**
- `--config`: 설정 파일 경로 (기본값: `config/settings.json`)
- `--once`: 한 번만 실행 후 종료
- `--dry-run`: 실제 실행 없이 스케줄 검증만 수행

#### 1.2 설정 파일 구조 (`config/settings.json`)

```json
{
  "database": {
    "host": "localhost",
    "port": 5432,
    "name": "realre_ingestion",
    "user": "postgres",
    "password_key": "db_password"  // key_manager에서 조회할 키 이름
  },
  "key_manager": {
    "storage_path": "secrets/keys.json"
  },
  "schedule_file": "config/schedules.json",
  "output_dir": "output",
  "log_level": "INFO"
}
```

#### 1.3 파일 구조 변경

```
config/
├── settings.json          # 전역 설정
└── schedules.json         # 통합 스케줄 파일

secrets/
└── keys.json              # 암호화된 API 키 저장소
```

---

## 2. 통합 스케줄 파일 및 Crontab 방식 스케줄링

### 목표
- 여러 개의 스케줄 JSON 파일을 하나로 통합
- crontab 표현식 지원 (`"0 0 * * 0"` = 매주 일요일 00:00)

### 구현 요구사항

#### 2.1 통합 스케줄 파일 (`config/schedules.json`)

```json
{
  "schema": "2.0",
  "jobs": [
    {
      "name": "update_region_codes",
      "description": "법정동 코드 목록 갱신",
      "schedule": {
        "type": "cron",
        "expression": "0 0 1 * *"  // 매월 1일 00:00
      },
      "args": {
        "output_table": "region_codes"
      },
      "enabled": true
    },
    {
      "name": "download_land_trade",
      "description": "토지 실거래가 전체 다운로드",
      "schedule": {
        "type": "cron",
        "expression": "0 2 * * 0"  // 매주 일요일 02:00
      },
      "args": {
        "output_dir": "output/land_trade",
        "deal_months": 1  // 최근 N개월 데이터
      },
      "enabled": true
    },
    {
      "name": "analyze_trade_data",
      "description": "실거래가 데이터 분석",
      "schedule": {
        "type": "cron",
        "expression": "0 6 * * 1"  // 매주 월요일 06:00
      },
      "args": {
        "input_dir": "output/land_trade",
        "failed_output_dir": "output/failed_records"
      },
      "enabled": true,
      "depends_on": ["download_land_trade"]  // 의존성 (선택적 구현)
    }
  ]
}
```

#### 2.2 Scheduler 클래스 수정 (`manager/scheduler.py`)

**새로운 스케줄 타입 추가:**

| 타입 | 설명 | 예시 |
|------|------|------|
| `interval` | 초 단위 반복 (기존) | `{"type": "interval", "seconds": 300}` |
| `daily` | 매일 특정 시각 (기존) | `{"type": "daily", "time": "02:00"}` |
| `weekly` | 매주 특정 요일 (기존) | `{"type": "weekly", "weekday": "sunday", "time": "02:00"}` |
| `cron` | Crontab 표현식 (신규) | `{"type": "cron", "expression": "0 2 * * 0"}` |

**Cron 표현식 형식:**
```
┌───────────── 분 (0-59)
│ ┌───────────── 시 (0-23)
│ │ ┌───────────── 일 (1-31)
│ │ │ ┌───────────── 월 (1-12)
│ │ │ │ ┌───────────── 요일 (0-6, 0=일요일)
│ │ │ │ │
* * * * *
```

**구현 시 참고:**
- `croniter` 라이브러리 사용 권장: `pip install croniter`
- 또는 직접 파싱 구현 (간단한 케이스만 지원)

```python
from croniter import croniter
from datetime import datetime

def compute_next_run_cron(self, expression: str, now: datetime) -> datetime:
    cron = croniter(expression, now)
    return cron.get_next(datetime)
```

---

## 3. Key Manager 통합

### 목표
- 모든 API 키와 비밀번호를 `key_manager`를 통해 암호화 관리
- passphrase는 실행 시 argument로만 입력

### 구현 요구사항

#### 3.1 Key Manager 초기화 흐름

```python
# manager/core.py

class IngestionManager:
    def __init__(self, *, passphrase: str, config_path: str = "config/settings.json"):
        self.config = self._load_config(config_path)

        # Key Manager 초기화
        self.key_manager = KeyManager(
            storage_path=self.config["key_manager"]["storage_path"],
            passphrase=passphrase,
        )

        # DB 비밀번호 복호화
        db_password = self.key_manager.get(self.config["database"]["password_key"])

        # DB 연결
        self.db = DBAdapter(
            host=self.config["database"]["host"],
            port=self.config["database"]["port"],
            database=self.config["database"]["name"],
            user=self.config["database"]["user"],
            password=db_password,
        )
```

#### 3.2 API 키 조회 인터페이스

```python
# manager/core.py

def get_api_key(self, key_name: str) -> str:
    """
    key_manager에서 API 키를 복호화하여 반환.

    Parameters
    ----------
    key_name:
        secrets/keys.json에 저장된 키 이름

    Returns
    -------
    str
        복호화된 API 키
    """
    value = self.key_manager.get(key_name)
    if value is None:
        raise KeyError(f"API key '{key_name}' not found in key_manager")
    return value
```

#### 3.3 키 등록 유틸리티 스크립트

`temp_utili/register_keys.py`:

```python
"""
API 키 등록 유틸리티.

사용법:
    python -m temp_utili.register_keys --passphrase <passphrase>

대화형으로 키를 입력받아 암호화하여 저장합니다.
"""

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--passphrase", required=True)
    args = parser.parse_args()

    km = KeyManager("secrets/keys.json", passphrase=args.passphrase, auto_persist=True)

    print("키 등록 모드. 'quit'을 입력하면 종료합니다.")
    while True:
        key_name = input("키 이름: ").strip()
        if key_name.lower() == "quit":
            break
        key_value = input("키 값: ").strip()
        km.set(key_name, key_value)
        print(f"'{key_name}' 등록 완료")
```

#### 3.4 필요한 키 목록

| 키 이름 | 용도 | 사용처 |
|---------|------|--------|
| `db_password` | PostgreSQL 비밀번호 | DBAdapter |
| `opendata_service_key` | data.go.kr API 키 | opendata_client |
| `vworld_api_key` | V-world API 키 | vworld_client |
| `juso_api_key` | 도로명주소 API 키 | juso_client |

---

## 4. 법정동 코드 목록 다운로드 기능

### 목표
- `opendata_client.py`에 법정동 코드 조회 기능 추가
- 월 1회 자동 갱신

### 구현 요구사항

#### 4.1 API 정보

- **Endpoint**: `https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList`
- **Method**: GET
- **Response**: XML

**요청 파라미터:**

| 파라미터 | 필수 | 설명 |
|---------|------|------|
| serviceKey | Y | 인증키 |
| pageNo | N | 페이지 번호 (기본값: 1) |
| numOfRows | N | 한 페이지 결과 수 (기본값: 10, 최대: 1000) |
| type | N | 응답 타입 (xml/json) |
| locatadd_nm | N | 지역명 검색 |

**응답 필드 (주요):**

| 필드명 | 설명 |
|--------|------|
| region_cd | 법정동코드 (10자리) |
| sido_cd | 시도코드 (2자리) |
| sgg_cd | 시군구코드 (3자리) |
| umd_cd | 읍면동코드 (3자리) |
| ri_cd | 리코드 (2자리) |
| locatjumin_cd | 행정동코드 |
| locatjijuk_cd | 지적코드 |
| locatadd_nm | 전체 주소명 |
| locat_order | 서열 |
| locat_rm | 비고 |
| locathigh_cd | 상위 법정동코드 |
| locallow_nm | 최하위 법정동명 |
| adpt_de | 생성일 |

#### 4.2 OpenDataClient 확장

```python
# clients/opendata_client.py

REGION_CODE_ENDPOINT = "https://apis.data.go.kr/1741000/StanReginCd/getStanReginCdList"

class OpenDataClient:
    # ... 기존 코드 ...

    def fetch_region_codes(
        self,
        *,
        locatadd_nm: str | None = None,
        num_of_rows: int | None = None,
        timeout: float | None = None,
        on_page: Callable[[int, int, int], None] | None = None,
    ) -> list[dict[str, Any]]:
        """
        법정동 코드 목록 조회.

        Parameters
        ----------
        locatadd_nm:
            지역명 필터 (예: "서울", "경기")
        num_of_rows:
            페이지당 행 수
        timeout:
            요청 타임아웃
        on_page:
            페이지 콜백

        Returns
        -------
        list[dict[str, Any]]
            법정동 코드 레코드 목록
        """
        params = {"type": "xml"}
        if locatadd_nm:
            params["locatadd_nm"] = locatadd_nm

        return self._fetch_paginated(
            REGION_CODE_ENDPOINT,
            params,
            num_of_rows=num_of_rows,
            timeout=timeout,
            on_page=on_page,
        )

    def fetch_region_codes_5digit(self, **kwargs) -> list[dict[str, str]]:
        """
        5자리 법정동 코드만 추출하여 반환.

        Returns
        -------
        list[dict[str, str]]
            [{"code": "11110", "name": "서울특별시 종로구"}, ...]
        """
        records = self.fetch_region_codes(**kwargs)

        # 10자리 코드에서 앞 5자리만 추출 (시군구 단위)
        seen = set()
        result = []
        for record in records:
            full_code = record.get("region_cd", "")
            if len(full_code) >= 5:
                code_5 = full_code[:5]
                if code_5 not in seen and code_5.endswith("00000")[-5:] == "00":
                    # 시군구 단위만 추출 (읍면동 코드가 000인 경우)
                    pass
                if code_5 not in seen:
                    seen.add(code_5)
                    result.append({
                        "code": code_5,
                        "name": record.get("locatadd_nm", ""),
                        "sido_cd": record.get("sido_cd", ""),
                        "sgg_cd": record.get("sgg_cd", ""),
                    })

        return result
```

#### 4.3 법정동 코드 갱신 Job

새 파일: `jobs/update_region_codes.py`

```python
"""
법정동 코드 목록 갱신 Job.

매월 1회 실행하여 최신 법정동 코드를 DB에 저장합니다.
"""

from __future__ import annotations
from typing import Any

from clients.opendata_client import OpenDataClient


def run(
    *,
    manager: Any,
    output_table: str = "region_codes",
) -> dict[str, Any]:
    """
    법정동 코드 목록을 갱신하여 DB에 저장.
    """
    job_name = "update_region_codes"

    manager.logger.info("Starting region code update")

    # API 키 조회
    service_key = manager.get_api_key("opendata_service_key")

    # 클라이언트 생성 및 데이터 조회
    client = OpenDataClient(service_key=service_key)

    def on_page(page_no, count, total):
        manager.logger.info(f"Fetched page {page_no}: {count} records (total: {total})")

    records = client.fetch_region_codes(on_page=on_page)

    manager.log_history(
        job_name=job_name,
        event_type="data_load",
        status="success",
        row_count=len(records),
    )

    # SCD2로 저장
    inserted = manager.upsert_scd2(
        table=output_table,
        records=records,
        key_fields=["region_cd"],
    )

    manager.log_history(
        job_name=job_name,
        event_type="scd2_upsert",
        status="success",
        row_count=inserted,
        details={"table": output_table},
    )

    return {"row_count": inserted, "table": output_table}
```

---

## 5. 토지 실거래가 전체 다운로드 Job 수정

### 목표
- 모든 법정동 코드(5자리)를 순회하며 데이터 수집
- 주 1회 실행
- 결과를 단일 파일로 저장

### 구현 요구사항

#### 5.1 download_trade.py 수정

```python
"""
토지 실거래가 전체 다운로드 Job.

모든 법정동 코드를 순회하며 실거래가 데이터를 수집하여
단일 CSV 파일로 저장합니다.
"""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Any

from clients.opendata_client import OpenDataClient, OpenDataAPIError


def _get_deal_ymd(months_ago: int = 0) -> str:
    """N개월 전의 YYYYMM 형식 반환."""
    from dateutil.relativedelta import relativedelta
    target = datetime.now() - relativedelta(months=months_ago)
    return target.strftime("%Y%m")


def _get_region_codes_from_db(manager: Any) -> list[str]:
    """
    DB에서 5자리 법정동 코드 목록 조회.

    region_codes 테이블에서 현재 유효한 코드만 조회합니다.
    """
    # 직접 쿼리 실행 (DBAdapter에 메서드 추가 필요할 수 있음)
    query = """
        SELECT DISTINCT SUBSTRING(region_cd, 1, 5) as code
        FROM region_codes
        WHERE is_current = 1
        ORDER BY code
    """
    # 또는 manager.db.fetch_region_codes() 메서드 구현
    pass


def run(
    *,
    manager: Any,
    output_dir: str = "output/land_trade",
    deal_months: int = 1,
    batch_size: int = 10,
) -> dict[str, Any]:
    """
    전체 법정동 코드를 순회하며 토지 실거래가 다운로드.

    Parameters
    ----------
    manager:
        IngestionManager 인스턴스
    output_dir:
        출력 디렉토리
    deal_months:
        조회할 최근 개월 수
    batch_size:
        로깅 간격 (N개 지역마다 진행상황 로깅)
    """
    job_name = "download_land_trade"

    manager.logger.info("Starting full land trade download")

    # API 키 조회
    service_key = manager.get_api_key("opendata_service_key")
    client = OpenDataClient(service_key=service_key)

    # 법정동 코드 목록 조회
    region_codes = _get_region_codes_from_db(manager)
    manager.logger.info(f"Found {len(region_codes)} region codes")

    # 조회 대상 월 목록
    deal_ymds = [_get_deal_ymd(i) for i in range(deal_months)]

    # 전체 레코드 수집
    all_records = []
    failed_regions = []

    for idx, code in enumerate(region_codes, 1):
        for deal_ymd in deal_ymds:
            try:
                records = client.fetch_land_trade(
                    lawd_cd=code,
                    deal_ymd=deal_ymd,
                )

                # 메타데이터 추가
                for record in records:
                    record["_lawd_cd"] = code
                    record["_deal_ymd"] = deal_ymd
                    record["_fetched_at"] = datetime.now().isoformat()

                all_records.extend(records)

            except OpenDataAPIError as e:
                manager.logger.warning(f"Failed to fetch {code}/{deal_ymd}: {e}")
                failed_regions.append({"code": code, "deal_ymd": deal_ymd, "error": str(e)})

        # 진행상황 로깅
        if idx % batch_size == 0:
            manager.logger.info(f"Progress: {idx}/{len(region_codes)} regions processed")

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

    # 단일 파일로 저장
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"land_trade_all_{timestamp}.csv"
    filepath = output_path / filename

    if all_records:
        # 모든 필드 수집
        fieldnames = list(dict.fromkeys(
            key for record in all_records for key in record.keys()
        ))

        with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_records)

    manager.log_history(
        job_name=job_name,
        event_type="csv_save",
        status="success",
        row_count=len(all_records),
        details={"output_path": str(filepath)},
    )

    manager.logger.info(f"Saved {len(all_records)} records to {filepath}")

    return {
        "row_count": len(all_records),
        "output_path": str(filepath),
        "regions_processed": len(region_codes),
        "regions_failed": len(failed_regions),
        "failed_regions": failed_regions,
    }
```

---

## 6. 데이터 분석 Job (껍데기)

### 목표
- 분석 알고리즘 구현을 위한 프레임워크 제공
- 실패한 행을 별도 파일로 추출

### 구현 요구사항

새 파일: `jobs/analyze_trade_data.py`

```python
"""
토지 실거래가 데이터 분석 Job.

다운로드된 실거래가 데이터를 분석하고, 분석 실패 행을 별도 저장합니다.

NOTE: 분석 알고리즘은 추후 구현 예정입니다.
"""

from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Any


def analyze_record(record: dict[str, Any]) -> tuple[bool, str | None]:
    """
    단일 레코드 분석.

    Parameters
    ----------
    record:
        분석할 레코드

    Returns
    -------
    tuple[bool, str | None]
        (성공 여부, 실패 시 에러 메시지)

    TODO
    ----
    실제 분석 로직 구현 필요:
    - 데이터 유효성 검증
    - 이상치 탐지
    - 비즈니스 규칙 검증
    """
    # TODO: 분석 알고리즘 구현
    # 현재는 모든 레코드를 성공으로 처리
    return True, None


def run(
    *,
    manager: Any,
    input_dir: str = "output/land_trade",
    failed_output_dir: str = "output/failed_records",
    input_pattern: str = "land_trade_all_*.csv",
) -> dict[str, Any]:
    """
    실거래가 데이터 분석 및 실패 행 추출.

    Parameters
    ----------
    manager:
        IngestionManager 인스턴스
    input_dir:
        입력 파일 디렉토리
    failed_output_dir:
        실패 레코드 출력 디렉토리
    input_pattern:
        입력 파일 glob 패턴
    """
    job_name = "analyze_trade_data"

    manager.logger.info("Starting trade data analysis")

    input_path = Path(input_dir)
    failed_path = Path(failed_output_dir)
    failed_path.mkdir(parents=True, exist_ok=True)

    # 최신 파일 찾기
    files = sorted(input_path.glob(input_pattern), reverse=True)
    if not files:
        manager.logger.warning(f"No input files found matching {input_pattern}")
        return {"row_count": 0, "status": "no_input"}

    input_file = files[0]
    manager.logger.info(f"Analyzing file: {input_file}")

    # 분석 실행
    success_count = 0
    failed_records = []

    with open(input_file, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []

        for row_num, record in enumerate(reader, start=2):  # 헤더 제외하고 2부터
            success, error_msg = analyze_record(record)

            if success:
                success_count += 1
            else:
                record["_error_message"] = error_msg
                record["_row_number"] = row_num
                record["_source_file"] = str(input_file)
                failed_records.append(record)

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

    # 실패 레코드 저장
    if failed_records:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        failed_filename = f"failed_records_{timestamp}.csv"
        failed_filepath = failed_path / failed_filename

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

        manager.logger.info(f"Saved {len(failed_records)} failed records to {failed_filepath}")

    return {
        "total_count": total_count,
        "success_count": success_count,
        "failed_count": len(failed_records),
        "input_file": str(input_file),
    }
```

---

## 7. JOB_REGISTRY 업데이트

### 구현 요구사항

`jobs/__init__.py` 수정:

```python
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
    "download_trade": download_trade.run,           # 이름 변경 고려: download_land_trade
    "update_region_codes": update_region_codes.run,  # 신규
    "analyze_trade_data": analyze_trade_data.run,    # 신규
}

__all__ = [
    "JOB_REGISTRY",
    "fetch_DB_transactions",
    "fetch_building_info",
    "download_trade",
    "update_region_codes",
    "analyze_trade_data",
]
```

---

## 8. 단위 테스트 (Jupyter Notebook)

### 목표
- 각 기능별 테스트 노트북 제공
- `unit_test/` 폴더에 저장

### 구현 요구사항

#### 8.1 폴더 구조

```
unit_test/
├── 01_key_manager_test.ipynb
├── 02_scheduler_cron_test.ipynb
├── 03_opendata_client_test.ipynb
├── 04_region_codes_test.ipynb
├── 05_land_trade_download_test.ipynb
├── 06_analysis_job_test.ipynb
└── 07_integration_test.ipynb
```

#### 8.2 각 노트북 구조

**01_key_manager_test.ipynb:**

```python
# Cell 1: 환경 설정
import sys
sys.path.insert(0, '..')

from key_manager import KeyManager, encrypt_value, decrypt_value, generate_passphrase

# Cell 2: 패스프레이즈 생성 테스트
passphrase = generate_passphrase()
print(f"Generated passphrase: {passphrase}")

# Cell 3: 암호화/복호화 테스트
original = "test_api_key_12345"
encrypted = encrypt_value(original, passphrase)
decrypted = decrypt_value(encrypted, passphrase)
assert original == decrypted, "Encryption/Decryption failed"
print("✅ Encryption/Decryption test passed")

# Cell 4: KeyManager 저장/로드 테스트
km = KeyManager(storage_path="test_keys.json", passphrase=passphrase, auto_persist=True)
km.set("test_key", "test_value")
assert km.get("test_key") == "test_value"
print("✅ KeyManager set/get test passed")

# Cell 5: 정리
import os
os.remove("test_keys.json")
print("✅ Cleanup complete")
```

**02_scheduler_cron_test.ipynb:**

```python
# Cell 1: 환경 설정
import sys
sys.path.insert(0, '..')

from datetime import datetime
from manager.scheduler import Scheduler, ScheduledJob

# Cell 2: Cron 표현식 파싱 테스트
# (croniter 설치 필요)
from croniter import croniter

now = datetime.now()
expressions = [
    ("0 0 * * *", "매일 00:00"),
    ("0 2 * * 0", "매주 일요일 02:00"),
    ("0 0 1 * *", "매월 1일 00:00"),
]

for expr, desc in expressions:
    cron = croniter(expr, now)
    next_run = cron.get_next(datetime)
    print(f"{desc}: {expr} -> Next: {next_run}")

# Cell 3: ScheduledJob cron 타입 테스트
# ... 구현 후 테스트
```

**03_opendata_client_test.ipynb:**

```python
# Cell 1: 환경 설정
import sys
sys.path.insert(0, '..')

from clients.opendata_client import OpenDataClient

# Cell 2: API 키 입력 (테스트용)
SERVICE_KEY = input("Service Key: ")

# Cell 3: 토지 실거래가 조회 테스트
client = OpenDataClient(service_key=SERVICE_KEY)
records = client.fetch_land_trade(lawd_cd="11110", deal_ymd="202401")
print(f"Fetched {len(records)} records")
print(records[0] if records else "No records")

# Cell 4: 법정동 코드 조회 테스트
region_records = client.fetch_region_codes(num_of_rows=100)
print(f"Fetched {len(region_records)} region codes")
print(region_records[0] if region_records else "No records")
```

**04_region_codes_test.ipynb:**

```python
# Cell 1: 환경 설정
import sys
sys.path.insert(0, '..')

from clients.opendata_client import OpenDataClient

# Cell 2: 5자리 코드 추출 테스트
SERVICE_KEY = input("Service Key: ")
client = OpenDataClient(service_key=SERVICE_KEY)

codes_5digit = client.fetch_region_codes_5digit()
print(f"Total 5-digit codes: {len(codes_5digit)}")

# Cell 3: 시도별 분포 확인
from collections import Counter
sido_dist = Counter(c["sido_cd"] for c in codes_5digit)
for sido, count in sorted(sido_dist.items()):
    print(f"시도코드 {sido}: {count}개")
```

---

## 9. 의존성 추가

### 구현 요구사항

`requirements.txt` 생성 또는 업데이트:

```
psycopg2-binary>=2.9.0
requests>=2.28.0
croniter>=1.3.0
python-dateutil>=2.8.0
```

설치:
```bash
pip install -r requirements.txt
```

---

## 10. 구현 순서 권장

1. **Phase 1: 기반 구조**
   - [ ] `config/settings.json` 구조 정의
   - [ ] `manager/core.py` 수정 (KeyManager 통합, 설정 파일 로드)
   - [ ] `Ingestion_Manager.py` CLI 수정

2. **Phase 2: 스케줄러 확장**
   - [ ] `manager/scheduler.py`에 cron 표현식 지원 추가
   - [ ] `config/schedules.json` 통합 스케줄 파일 생성

3. **Phase 3: API 클라이언트 확장**
   - [ ] `clients/opendata_client.py`에 `fetch_region_codes` 추가
   - [ ] 단위 테스트 작성

4. **Phase 4: Job 구현**
   - [ ] `jobs/update_region_codes.py` 생성
   - [ ] `jobs/download_trade.py` 수정 (전체 법정동 순회)
   - [ ] `jobs/analyze_trade_data.py` 생성 (껍데기)

5. **Phase 5: 테스트 및 검증**
   - [ ] `unit_test/` 노트북 작성
   - [ ] 통합 테스트 실행

---

## 참고: 기존 파일 수정 요약

| 파일 | 수정 내용 |
|------|----------|
| `Ingestion_Manager.py` | CLI 최소화, passphrase argument 추가 |
| `manager/core.py` | KeyManager 통합, 설정 파일 로드, `get_api_key()` 메서드 |
| `manager/scheduler.py` | cron 표현식 지원, `croniter` 사용 |
| `manager/db.py` | `fetch_region_codes()` 쿼리 메서드 (선택적) |
| `clients/opendata_client.py` | `fetch_region_codes()`, `fetch_region_codes_5digit()` |
| `jobs/__init__.py` | 새 Job 등록 |
| `jobs/download_trade.py` | 전체 법정동 순회 로직 |

| 신규 파일 | 설명 |
|----------|------|
| `config/settings.json` | 전역 설정 |
| `config/schedules.json` | 통합 스케줄 |
| `jobs/update_region_codes.py` | 법정동 코드 갱신 Job |
| `jobs/analyze_trade_data.py` | 데이터 분석 Job (껍데기) |
| `unit_test/*.ipynb` | 단위 테스트 노트북 |
