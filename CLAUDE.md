# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 실행 명령어 (Commands)

```bash
# 최소 CLI - passphrase만 필수
python Ingestion_Manager.py --passphrase <passphrase>

# 한 번만 실행
python Ingestion_Manager.py --passphrase <passphrase> --once

# 스케줄 유효성 검증 (dry-run)
python Ingestion_Manager.py --passphrase <passphrase> --dry-run

# 상시 스케줄링 (5초 간격 폴링)
python Ingestion_Manager.py --passphrase <passphrase> --poll 5

# 비동기 실행
python Ingestion_Manager.py --passphrase <passphrase> --async

# 설정 파일 지정
python Ingestion_Manager.py --passphrase <passphrase> --config config/settings.json

# 스케줄 파일 override
python Ingestion_Manager.py --passphrase <passphrase> --schedule config/schedules.json

# Job 직접 실행 (CLI)
python -m jobs.download_trade --service-key <key> --lawd-cd 11110 --deal-ymd 202401

# API 키 등록 유틸리티
python -m temp_utili.register_keys --passphrase <passphrase>
```

## 아키텍처 (Architecture)

### 핵심 구조

```
Ingestion_Manager.py (진입점)
    └── manager/core.py::IngestionManager (오케스트레이터)
            ├── manager/scheduler.py::Scheduler (스케줄 관리, cron 지원)
            ├── manager/db.py::DBAdapter (PostgreSQL + SCD2)
            ├── manager/clients.py::ClientLoader (동적 클라이언트 로딩)
            └── key_manager::KeyManager (암호화된 키 관리)
```

### 설정 파일 구조

```
config/
├── settings.json          # 전역 설정 (DB, KeyManager, 로깅)
└── schedules.json         # 통합 스케줄 파일 (cron 지원)

secrets/
└── keys.json              # 암호화된 API 키 저장소
```

### IngestionManager 역할

- 설정 JSON 파일 로드 (`config/settings.json`)
- KeyManager 통합 (passphrase 기반 암호화)
- 스케줄 JSON 파일 로드 (`Scheduler.from_file`)
- 예약된 Job 실행 (`run_once`, `run_forever`)
- API 키 조회 (`get_api_key`)
- 클라이언트를 통한 외부 데이터 조회 (`request_source_data`)
- 실행 이력 기록 (`log_history` → `ingestion_history` 테이블)
- SCD2 방식 데이터 적재 (`upsert_scd2`)
- Raw SQL 쿼리 실행 (`execute_query`)

### Job 등록 방식

`jobs/__init__.py`의 `JOB_REGISTRY` 딕셔너리에 job 이름 → callable 매핑. 새 job 추가 시:
1. `jobs/` 디렉토리에 모듈 생성
2. `run(*, manager, **args)` 함수 구현
3. `JOB_REGISTRY`에 등록

### 등록된 Jobs

| Job 이름 | 파일 | 설명 |
|---------|------|------|
| `fetch_DB_transactions` | `jobs/fetch_DB_transactions.py` | mock 트랜잭션 → SCD2 테이블 |
| `fetch_building_info` | `jobs/fetch_building_info.py` | V-world 건물정보 → SCD2 테이블 |
| `download_trade` | `jobs/download_trade.py` | 토지 실거래가 → CSV (단일/전체 지역 모드) |
| `update_region_codes` | `jobs/update_region_codes.py` | 법정동 코드 갱신 → SCD2 테이블 |
| `analyze_trade_data` | `jobs/analyze_trade_data.py` | 실거래가 데이터 분석 (skeleton) |

### 스케줄 타입

- `interval`: 초 단위 반복 (`{"type": "interval", "seconds": 300}`)
- `daily`: 매일 특정 시각 (`{"type": "daily", "time": "HH:MM"}`)
- `weekly`: 매주 특정 요일/시각 (`{"type": "weekly", "weekday": "monday", "time": "HH:MM"}`)
- `cron`: Crontab 표현식 (`{"type": "cron", "expression": "0 2 * * 0"}`)

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

## 클라이언트 구조 (clients/)

### 클라이언트 로딩

`ClientLoader.load(name)`은 `clients.{name}_client` 또는 `clients.{name}` 모듈을 동적 임포트. 로드 실패 시 mock 클라이언트로 폴백.

### opendata_client (공공데이터포털 API)

- `OpenDataClient`: data.go.kr API 클라이언트 클래스
  - `fetch_land_trade(lawd_cd, deal_ymd)`: 토지 실거래가 조회
  - `fetch_region_codes()`: 법정동 코드 목록 조회
  - `fetch_region_codes_5digit()`: 5자리 시군구 코드 추출
  - `_fetch_paginated()`: 페이징 처리
  - `_request()`: HTTP 요청 + 에러 처리
- `_parse_xml_items()`: XML 응답 파싱 (items > item)
- `OpenDataAPIError`: API 예외 클래스

### juso_client (도로명 주소 API)

- `search_road_addresses`: 주소 검색
- `fetch_road_address_detail`: 주소 상세 조회

### vworld_client (공간정보 API)

- `call_vworld_api`: `clients/vworld/vworld_url.json` 메타데이터 기반 API 호출
- `search_address`: 주소 검색 (ROAD → PARCEL 자동 폴백)

### mock_client (테스트용)

- `fetch_transactions`: 더미 트랜잭션 데이터 생성

### _http_helpers (내부 유틸)

- `normalize_params`: 쿼리 파라미터 정규화
- `request_bytes`: urllib 기반 HTTP GET 요청

## Key Manager

### 필요한 키 목록

| 키 이름 | 용도 | 사용처 |
|---------|------|--------|
| `db_password` | PostgreSQL 비밀번호 | DBAdapter |
| `opendata_service_key` | data.go.kr API 키 | opendata_client |
| `vworld_api_key` | V-world API 키 | vworld_client |
| `juso_api_key` | 도로명주소 API 키 | juso_client |

### 키 등록 방법

```bash
python -m temp_utili.register_keys --passphrase <your-passphrase>
```

## DB 스키마 (PostgreSQL)

- `ingestion_history`: job 실행 이력 (job_name, event_type, status, started_at, ended_at, duration_ms, row_count, details)
- `region_codes`: 법정동 코드 목록 (SCD2)
- SCD2 테이블: `upsert_scd2` 호출 시 자동 생성, `valid_from`, `valid_to`, `is_current`, `row_hash` 컬럼 포함

## 의존성

```bash
pip install -r requirements.txt
```

필수 패키지:
- `psycopg2-binary`: PostgreSQL 연결용
- `requests`: HTTP 클라이언트
- `croniter`: Cron 표현식 파싱
- `python-dateutil`: 날짜 계산 유틸리티
