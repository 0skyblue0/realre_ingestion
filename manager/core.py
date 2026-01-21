from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from utility.Logger import create_logger

from .clients import ClientLoader, ClientLoadError
from .db import DBAdapter
from .scheduler import Scheduler


def _load_config(config_path: str | Path) -> dict[str, Any]:
    """
    Load configuration from JSON file.

    Parameters
    ----------
    config_path:
        Path to the settings JSON file.

    Returns
    -------
    dict[str, Any]
        Configuration dictionary.
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    raw = path.read_text(encoding="utf-8")
    return json.loads(raw)


class IngestionManager:
    """
    Orchestrates scheduled jobs, client access, and persistence.
    """

    def __init__(
        self,
        *,
        passphrase: str | None = None,
        config_path: str | Path = "config/settings.json",
        # Legacy parameters for backward compatibility
        db_host: str | None = None,
        db_port: int | None = None,
        db_name: str | None = None,
        db_user: str | None = None,
        db_password: str | None = None,
        db_dsn: str | None = None,
        async_mode: bool = False,
        logger_name: str = "ingestion.manager",
        skip_db: bool = False,
    ):
        self.logger = create_logger(logger_name)
        self.config: dict[str, Any] = {}
        self.key_manager = None
        self.passphrase = passphrase

        # Try to load config if exists
        config_file = Path(config_path)
        if config_file.exists():
            self.config = _load_config(config_path)
            self.logger.info("Loaded config from %s", config_path)

            # Initialize KeyManager if passphrase provided
            if passphrase:
                self._init_key_manager(passphrase)

        # Determine DB connection parameters
        # Priority: explicit params > config file > defaults
        db_config = self.config.get("database", {})

        final_db_host = db_host if db_host is not None else db_config.get("host", "localhost")
        final_db_port = db_port if db_port is not None else db_config.get("port", 5432)
        final_db_name = db_name if db_name is not None else db_config.get("name", "realre_ingestion")
        final_db_user = db_user if db_user is not None else db_config.get("user", "postgres")

        # DB password: explicit param > key_manager > config > default
        final_db_password = db_password
        if final_db_password is None:
            password_key = db_config.get("password_key")
            if password_key and self.key_manager:
                try:
                    final_db_password = self.key_manager.get(password_key)
                except Exception as exc:
                    self.logger.warning("Failed to get DB password from key_manager: %s", exc)
            if final_db_password is None:
                final_db_password = db_config.get("password", "")

        self.db = None
        if not skip_db:
            self.db = DBAdapter(
                host=final_db_host,
                port=final_db_port,
                database=final_db_name,
                user=final_db_user,
                password=final_db_password,
                dsn=db_dsn,
            )
        self.client_loader = ClientLoader()
        self.async_mode = async_mode
        self._schedule: Scheduler | None = None

    def _init_key_manager(self, passphrase: str) -> None:
        """Initialize KeyManager from config settings."""
        from key_manager import KeyManager

        km_config = self.config.get("key_manager", {})
        storage_path = km_config.get("storage_path", "secrets/keys.json")

        self.key_manager = KeyManager(
            storage_path=storage_path,
            passphrase=passphrase,
        )
        self.logger.info("KeyManager initialized with storage: %s", storage_path)

    def get_api_key(self, key_name: str) -> str:
        """
        Retrieve an API key from KeyManager.

        Parameters
        ----------
        key_name:
            Key identifier in the secrets storage.

        Returns
        -------
        str
            Decrypted API key.

        Raises
        ------
        KeyError
            If key not found or KeyManager not initialized.
        """
        if self.key_manager is None:
            raise RuntimeError(
                "KeyManager not initialized. Provide passphrase when creating IngestionManager."
            )
        value = self.key_manager.get(key_name)
        if value is None:
            raise KeyError(f"API key '{key_name}' not found in key_manager")
        return value

    # ---------------------------------------------------------------- schedule
    def load_schedule(self, schedule_path: str | Path | None = None) -> None:
        """
        Load job schedule from file.

        Parameters
        ----------
        schedule_path:
            Path to schedule JSON file. If None, uses schedule_file from config.
        """
        if schedule_path is None:
            schedule_path = self.config.get("schedule_file", "config/schedules.json")
        self._schedule = Scheduler.from_file(schedule_path)
        self.logger.info("Loaded schedule from %s", schedule_path)

    # ----------------------------------------------------------------- clients
    def request_source_data(self, client: str, method: str, **params: Any) -> Any:
        try:
            module = self.client_loader.load(client)
        except ClientLoadError as exc:
            self.logger.warning("Falling back to mock client due to: %s", exc)
            module = self.client_loader.load("mock")
        return self.client_loader.call(module, method, **params)

    # --------------------------------------------------------------------- DB
    def log_history(self, **kwargs: Any) -> int:
        return self.db.log_history(**kwargs)

    def fetch_history(self, limit: int = 50) -> list[dict[str, Any]]:
        return self.db.fetch_history(limit)

    def upsert_scd2(
        self,
        table: str,
        records: list[dict[str, Any]],
        key_fields: list[str],
        attribute_fields: list[str] | None = None,
    ) -> int:
        return self.db.upsert_scd2(
            table=table,
            records=records,
            key_fields=key_fields,
            attribute_fields=attribute_fields,
        )

    def execute_query(self, query: str, params: tuple | None = None) -> list[dict[str, Any]]:
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
        return self.db.execute_query(query, params)

    # -------------------------------------------------------------------- jobs
    def _run_job(self, job_name: str, job_callable: Callable[..., Any], args: dict[str, Any]) -> None:
        start = time.perf_counter()
        started_at = datetime.now(timezone.utc).isoformat()
        history_id = self.log_history(
            job_name=job_name,
            event_type="job_start",
            status="started",
            started_at=started_at,
            details={"args": args},
        )
        try:
            result = job_callable(manager=self, **args)
            duration_ms = int((time.perf_counter() - start) * 1000)
            row_count = result.get("row_count") if isinstance(result, dict) else None
            self.log_history(
                job_name=job_name,
                event_type="job_end",
                status="success",
                started_at=started_at,
                ended_at=datetime.now(timezone.utc).isoformat(),
                duration_ms=duration_ms,
                row_count=row_count,
                details=result,
            )
        except Exception as exc:  # pragma: no cover - runtime safety
            duration_ms = int((time.perf_counter() - start) * 1000)
            self.log_history(
                job_name=job_name,
                event_type="job_error",
                status="failed",
                started_at=started_at,
                ended_at=datetime.now(timezone.utc).isoformat(),
                duration_ms=duration_ms,
                details={"error": str(exc)},
            )
            self.logger.exception("Job %s failed", job_name)

    async def _run_job_async(self, job_name: str, job_callable: Callable[..., Any], args: dict[str, Any]) -> None:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._run_job, job_name, job_callable, args)

    def _get_job_callable(self, job_name: str) -> Callable[..., Any]:
        from jobs import JOB_REGISTRY

        try:
            return JOB_REGISTRY[job_name]
        except KeyError as exc:
            raise RuntimeError(f"Unknown job '{job_name}'") from exc

    # ------------------------------------------------------------------- loops
    async def _run_all_jobs_async(self, jobs: list) -> None:
        tasks = []
        for job in jobs:
            job_callable = self._get_job_callable(job.name)
            tasks.append(self._run_job_async(job.name, job_callable, job.args))
        await asyncio.gather(*tasks)

    def run_once(self) -> None:
        if self._schedule is None:
            raise RuntimeError("Schedule not loaded.")
        now = datetime.now(timezone.utc)
        jobs = self._schedule.due_jobs(now)
        if not jobs:
            self.logger.info("No jobs due at %s", now.isoformat())
            return
        if self.async_mode:
            asyncio.run(self._run_all_jobs_async(jobs))
        else:
            for job in jobs:
                job_callable = self._get_job_callable(job.name)
                self._run_job(job.name, job_callable, job.args)

    def run_forever(self, poll_interval: int = 5) -> None:
        if self._schedule is None:
            raise RuntimeError("Schedule not loaded.")
        self.logger.info("Starting scheduler loop (async=%s)", self.async_mode)
        while True:
            self.run_once()
            time.sleep(poll_interval)


def build_arg_parser() -> argparse.ArgumentParser:
    """
    Build argument parser with minimal CLI.

    Required:
        --passphrase: Passphrase for KeyManager decryption

    Optional:
        --config: Path to settings.json (default: config/settings.json)
        --schedule: Override schedule file from config
        --once: Run once and exit
        --dry-run: Validate schedule without executing
        --async: Enable async execution
        --poll: Poll interval in seconds
    """
    parser = argparse.ArgumentParser(
        description="Ingestion manager - minimal CLI with JSON config."
    )
    parser.add_argument(
        "--passphrase",
        required=True,
        help="Passphrase for KeyManager decryption.",
    )
    parser.add_argument(
        "--config",
        default="config/settings.json",
        help="Path to settings JSON file (default: config/settings.json).",
    )
    parser.add_argument(
        "--schedule",
        default=None,
        help="Override schedule file from config.",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run jobs that are due only once and exit.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate schedule without executing jobs.",
    )
    parser.add_argument(
        "--async",
        dest="async_mode",
        action="store_true",
        help="Enable asyncio execution.",
    )
    parser.add_argument(
        "--poll",
        type=int,
        default=5,
        help="Scheduler poll interval (seconds).",
    )
    return parser


def run_from_cli(argv: list[str] | None = None) -> None:
    args = build_arg_parser().parse_args(argv)

    manager = IngestionManager(
        passphrase=args.passphrase,
        config_path=args.config,
        async_mode=args.async_mode,
        skip_db=args.dry_run,  # Skip DB connection in dry-run mode
    )

    schedule_path = args.schedule
    manager.load_schedule(schedule_path)

    if args.dry_run:
        print("Dry run mode - schedule validated successfully.")
        print(f"Loaded {len(manager._schedule.jobs)} enabled jobs:")
        for job in manager._schedule.jobs:
            print(f"  - {job.name}: {job.description or '(no description)'}")
        return

    if args.once:
        manager.run_once()
    else:
        manager.run_forever(poll_interval=args.poll)
