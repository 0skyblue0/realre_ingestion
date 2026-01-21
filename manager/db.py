from __future__ import annotations

import hashlib
import json
import threading
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

import psycopg2
import psycopg2.extras


def _utcnow_iso() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()


class DBAdapter:
    """
    PostgreSQL adapter with history logging and SCD2 helpers.

    The adapter provides transactional guarantees and optimistic concurrency for
    SCD2 upserts.
    """

    def __init__(
        self,
        *,
        host: str = "localhost",
        port: int = 5432,
        database: str = "realre_ingestion",
        user: str = "postgres",
        password: str = "",
        dsn: str | None = None,
    ):
        self._lock = threading.RLock()
        if dsn:
            self._conn = psycopg2.connect(dsn)
        else:
            self._conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
            )
        self._conn.autocommit = False
        self.ensure_history_schema()

    # ------------------------------------------------------------------ setup
    def _get_cursor(self):
        return self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def ensure_history_schema(self) -> None:
        with self._lock:
            cursor = self._get_cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS ingestion_history (
                    id SERIAL PRIMARY KEY,
                    job_name TEXT,
                    event_type TEXT,
                    status TEXT,
                    started_at TEXT,
                    ended_at TEXT,
                    duration_ms INTEGER,
                    row_count INTEGER,
                    details TEXT
                );
                """
            )
            self._conn.commit()

    # ----------------------------------------------------------------- helpers
    def log_history(
        self,
        *,
        job_name: str,
        event_type: str,
        status: str,
        started_at: str | None = None,
        ended_at: str | None = None,
        duration_ms: int | None = None,
        row_count: int | None = None,
        details: Mapping[str, Any] | str | None = None,
    ) -> int:
        payload = details
        if isinstance(details, Mapping):
            payload = json.dumps(details, ensure_ascii=False)
        with self._lock:
            cursor = self._get_cursor()
            cursor.execute(
                """
                INSERT INTO ingestion_history
                (job_name, event_type, status, started_at, ended_at, duration_ms, row_count, details)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
                """,
                (
                    job_name,
                    event_type,
                    status,
                    started_at or _utcnow_iso(),
                    ended_at,
                    duration_ms,
                    row_count,
                    payload,
                ),
            )
            result = cursor.fetchone()
            self._conn.commit()
        return int(result["id"])

    def fetch_history(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            cursor = self._get_cursor()
            cursor.execute(
                "SELECT * FROM ingestion_history ORDER BY id DESC LIMIT %s;",
                (limit,),
            )
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def execute_query(
        self,
        query: str,
        params: tuple | None = None,
    ) -> list[dict[str, Any]]:
        """
        Execute a raw SQL query and return results.

        Parameters
        ----------
        query:
            SQL query string.
        params:
            Optional query parameters.

        Returns
        -------
        list[dict[str, Any]]
            Query results as list of dictionaries.
        """
        with self._lock:
            cursor = self._get_cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            if cursor.description:
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
            self._conn.commit()
            return []

    # ------------------------------------------------------------------- SCD2
    def ensure_scd2_table(
        self,
        table: str,
        key_fields: Sequence[str],
        attribute_fields: Sequence[str],
    ) -> None:
        key_cols = ", ".join(f"{name} TEXT NOT NULL" for name in key_fields)
        attr_cols = ", ".join(f"{name} TEXT" for name in attribute_fields)
        with self._lock:
            cursor = self._get_cursor()
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    id SERIAL PRIMARY KEY,
                    {key_cols},
                    {attr_cols},
                    valid_from TEXT NOT NULL,
                    valid_to TEXT NOT NULL,
                    is_current INTEGER NOT NULL,
                    row_hash TEXT NOT NULL
                );
                """
            )
            idx_cols = "_".join(key_fields)
            cursor.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{table}_{idx_cols} ON {table} ({', '.join(key_fields)});"
            )
            cursor.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{table}_current ON {table}(is_current);"
            )
            self._conn.commit()

    def _compute_hash(self, record: Mapping[str, Any], fields: Sequence[str]) -> str:
        hasher = hashlib.sha256()
        for field in fields:
            value = "" if record.get(field) is None else str(record.get(field))
            hasher.update(value.encode("utf-8"))
        return hasher.hexdigest()

    def upsert_scd2(
        self,
        *,
        table: str,
        records: Iterable[Mapping[str, Any]],
        key_fields: Sequence[str],
        attribute_fields: Sequence[str] | None = None,
    ) -> int:
        """
        Apply SCD2 logic for the given records.

        Returns the number of inserted history rows for visibility.
        """
        rows = list(records)
        if not rows:
            return 0
        attribute_fields = list(attribute_fields or [])
        if not attribute_fields:
            attribute_fields = [f for f in rows[0].keys() if f not in key_fields]
        self.ensure_scd2_table(table, key_fields, attribute_fields)

        inserted = 0
        now = _utcnow_iso()
        with self._lock:
            cursor = self._get_cursor()
            for row in rows:
                row_hash = self._compute_hash(row, attribute_fields)
                key_values = [row[field] for field in key_fields]
                placeholders = " AND ".join(f"{field}=%s" for field in key_fields)
                cursor.execute(
                    f"""
                    SELECT id, row_hash FROM {table}
                    WHERE {placeholders} AND is_current=1
                    ORDER BY id DESC LIMIT 1;
                    """,
                    key_values,
                )
                current = cursor.fetchone()
                if current and current["row_hash"] == row_hash:
                    continue
                if current:
                    cursor.execute(
                        f"""
                        UPDATE {table}
                        SET valid_to=%s, is_current=0
                        WHERE id=%s;
                        """,
                        (now, current["id"]),
                    )
                columns = list(key_fields) + list(attribute_fields)
                values = [row.get(col) for col in columns]
                cursor.execute(
                    f"""
                    INSERT INTO {table} ({', '.join(columns)}, valid_from, valid_to, is_current, row_hash)
                    VALUES ({', '.join('%s' for _ in columns)}, %s, %s, 1, %s);
                    """,
                    (*values, now, "9999-12-31T00:00:00+00:00", row_hash),
                )
                inserted += 1
            self._conn.commit()
        return inserted
