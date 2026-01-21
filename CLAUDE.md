# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 실행 명령어 (Commands)

```bash
# 한 번만 실행
python Ingestion_Manager.py --schedule schedules/interval_schedule.json --once

# 상시 스케줄링 (5초 간격 폴링)
python Ingestion_Manager.py --schedule schedules/interval_schedule.json --poll 5

# 비동기 실행
python Ingestion_Manager.py --schedule schedules/interval_schedule.json --async

# DB 경로 지정
python Ingestion_Manager.py --schedule schedules/interval_schedule.json --db path/to/db.db
```

## 아키텍처 (Architecture)

### 핵심 구조

```
Ingestion_Manager.py (진입점)
    └── manager/core.py::IngestionManager (오케스트레이터)
            ├── manager/scheduler.py::Scheduler (스케줄 관리)
            ├── manager/db.py::DBAdapter (SQLite + SCD2)
            └── manager/clients.py::ClientLoader (동적 클라이언트 로딩)
```

### IngestionManager 역할

- 스케줄 JSON 파일 로드 (`Scheduler.from_file`)
- 예약된 Job 실행 (`run_once`, `run_forever`)
- 클라이언트를 통한 외부 데이터 조회 (`request_source_data`)
- 실행 이력 기록 (`log_history` → `ingestion_history` 테이블)
- SCD2 방식 데이터 적재 (`upsert_scd2`)

### Job 등록 방식

`jobs/__init__.py`의 `JOB_REGISTRY` 딕셔너리에 job 이름 → callable 매핑. 새 job 추가 시:
1. `jobs/` 디렉토리에 모듈 생성
2. `run(*, manager, **args)` 함수 구현
3. `JOB_REGISTRY`에 등록

### 클라이언트 로딩

`ClientLoader.load(name)`은 `clients.{name}_client` 또는 `clients.{name}` 모듈을 동적 임포트. 로드 실패 시 mock 클라이언트로 폴백.

### 스케줄 타입

- `interval`: 초 단위 반복 (`{"type": "interval", "seconds": 300}`)
- `daily`: 매일 특정 시각 (`{"type": "daily", "time": "HH:MM"}`)
- `weekly`: 매주 특정 요일/시각 (`{"type": "weekly", "weekday": "monday", "time": "HH:MM"}`)

## 외부 API 클라이언트

### juso_client (도로명 주소 API)
- `search_road_addresses`: 주소 검색
- `fetch_road_address_detail`: 주소 상세 조회

### vworld_client (공간정보 API)
- `call_vworld_api`: `clients/vworld/vworld_url.json` 메타데이터 기반 API 호출
- `search_address`: 주소 검색 (ROAD → PARCEL 자동 폴백)

## DB 스키마

- `ingestion_history`: job 실행 이력 (job_name, event_type, status, started_at, ended_at, duration_ms, row_count, details)
- SCD2 테이블: `upsert_scd2` 호출 시 자동 생성, `valid_from`, `valid_to`, `is_current`, `row_hash` 컬럼 포함
