"""
Watchdog-Based Alarm CSV Folder Monitor.

Monitors a configured directory for `.csv` alarm files exported by a PLC or HMI,
parses and logs the alarm records into the `cycle_alarm_logs` database table,
and moves each processed CSV file into `<watch_dir>/.bckup/`.
"""

import asyncio
import csv
from datetime import datetime, timezone
import logging
import os
from pathlib import Path
import shutil
from typing import Any, Callable, Optional

from sqlalchemy import insert, select
from watchdog.events import FileMovedEvent, FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from app.database import get_Session_context
from app.models import CycleAlarmLog, CycleRecord, CycleStatus

logger = logging.getLogger(__name__)

DEFAULT_WATCH_DIR = Path(os.getenv("ALARM_CSV_WATCH_DIR", "./watched_alarms"))


def _parse_timestamp(raw_ts: Optional[str]) -> datetime:
    """Parse an ISO or common PLC/HMI timestamp string into a UTC datetime."""
    if not raw_ts or not raw_ts.strip():
        return datetime.now(timezone.utc)
    cleaned = raw_ts.strip()
    try:
        dt = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%d.%m.%Y %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
            try:
                return datetime.strptime(cleaned, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return datetime.now(timezone.utc)


def parse_alarm_csv_file(csv_path: Path) -> list[dict[str, Any]]:
    """
    Parse an alarm CSV file into a list of alarm dictionaries.

    Supports flexible column names (`timestamp`/`time`, `alarm_code`/`code`/`id`,
    `alarm_text`/`message`/`description`/`text`, `severity`/`level`, `cycle_id`).
    """
    rows_out: list[dict[str, Any]] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(2048)
        f.seek(0)
        if not sample.strip():
            return rows_out

        # Detect delimiter (; or ,) common in European/Siemens HMI CSV exports
        delimiter = ";" if sample.count(";") > sample.count(",") else ","
        reader = csv.DictReader(f, delimiter=delimiter)

        for raw_row in reader:
            norm = {
                (k.strip().lower() if k else ""): (v.strip() if v else "")
                for k, v in raw_row.items()
            }
            if not any(norm.values()):
                continue

            ts_str = (
                norm.get("timestamp")
                or norm.get("time")
                or norm.get("date_time")
            )
            alarm_code = (
                norm.get("alarm_code")
                or norm.get("code")
                or norm.get("alarm_id")
                or "ALM_CSV"
            )
            alarm_text = (
                norm.get("alarm_text")
                or norm.get("message")
                or norm.get("description")
                or norm.get("text")
                or "PLC/HMI CSV Alarm"
            )
            severity = (
                norm.get("severity")
                or norm.get("level")
                or "CRITICAL"
            ).upper()

            explicit_cycle_id: Optional[int] = None
            if norm.get("cycle_id") and norm["cycle_id"].isdigit():
                explicit_cycle_id = int(norm["cycle_id"])

            rows_out.append(
                {
                    "cycle_id": explicit_cycle_id,
                    "timestamp": _parse_timestamp(ts_str),
                    "alarm_code": alarm_code[:64],
                    "alarm_text": alarm_text[:500],
                    "severity": severity[:32],
                    "source_file": csv_path.name,
                }
            )

    return rows_out


class _WatchdogCSVHandler(FileSystemEventHandler):
    """Watchdog event handler that schedules CSV ingestion on the asyncio loop."""

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        callback: Callable[[Path], Any],
    ) -> None:
        super().__init__()
        self._loop = loop
        self._callback = callback

    def _maybe_dispatch(self, path_str: str) -> None:
        path = Path(path_str)
        if ".bckup" in path.parts:
            return
        if path.name.startswith(".") or path.suffix.lower() != ".csv":
            return
        self._loop.call_soon_threadsafe(
            lambda: asyncio.create_task(self._callback(path))
        )

    def on_created(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            self._maybe_dispatch(str(event.src_path))

    def on_moved(self, event: FileMovedEvent) -> None:
        if not event.is_directory:
            self._maybe_dispatch(str(event.dest_path))

    def on_modified(self, event: FileSystemEvent) -> None:
        if not event.is_directory:
            self._maybe_dispatch(str(event.src_path))


class AlarmCSVWatcherService:
    """
    Monitors `watch_dir` for `.csv` alarm files using `watchdog`, stores parsed
    alarms in `cycle_alarm_logs`, and moves processed files to `watch_dir/.bckup/`.
    """

    def __init__(
        self,
        watch_dir: Path | str = DEFAULT_WATCH_DIR,
        active_cycle_id_provider: Optional[Callable[[], Optional[int]]] = None,
    ) -> None:
        self.watch_dir = Path(watch_dir).resolve()
        self.backup_dir = self.watch_dir / ".bckup"
        self.active_cycle_id_provider = active_cycle_id_provider

        self._observer: Optional[Observer] = None
        self._processing_files: set[Path] = set()
        self.processed_files_count: int = 0
        self.ingested_alarms_count: int = 0

    def ensure_directories(self) -> None:
        """Create the watched folder and its `.bckup` subfolder if they do not exist."""
        self.watch_dir.mkdir(parents=True, exist_ok=True)
        self.backup_dir.mkdir(parents=True, exist_ok=True)

    async def start(self) -> None:
        """Start the `watchdog` observer and process any pre-existing CSV files."""
        self.ensure_directories()
        loop = asyncio.get_running_loop()

        # Process any CSV files already sitting in watch_dir on startup
        for existing_csv in sorted(self.watch_dir.glob("*.csv")):
            await self.process_csv_file(existing_csv)

        handler = _WatchdogCSVHandler(loop=loop, callback=self.process_csv_file)
        self._observer = Observer()
        self._observer.schedule(handler, str(self.watch_dir), recursive=False)
        self._observer.start()
        logger.info(
            "Started Watchdog Alarm CSV Monitor on '%s' (backup='%s')",
            self.watch_dir,
            self.backup_dir,
        )

    async def stop(self) -> None:
        """Stop the `watchdog` observer thread cleanly."""
        if self._observer is not None:
            self._observer.stop()
            await asyncio.to_thread(self._observer.join, 2.0)
            self._observer = None
            logger.info("Stopped Watchdog Alarm CSV Monitor.")

    async def _resolve_target_cycle_id(self) -> Optional[int]:
        """
        Determine which cycle ID to attach incoming CSV alarms to:
        1. Active cycle from the live OPC-UA poller (if running)
        2. Most recent RUNNING or latest CycleRecord in the DB.
        """
        if self.active_cycle_id_provider is not None:
            active_id = self.active_cycle_id_provider()
            if active_id is not None:
                return active_id

        async with get_Session_context() as session:
            # First check if there is a RUNNING cycle
            running_res = await session.execute(
                select(CycleRecord.id)
                .where(CycleRecord.status == CycleStatus.RUNNING)
                .order_by(CycleRecord.id.desc())
                .limit(1)
            )
            running_id = running_res.scalar_one_or_none()
            if running_id is not None:
                return int(running_id)
        return None

    async def process_csv_file(self, csv_path: Path) -> int:
        """
        Read a `.csv` file, insert its alarm rows into `cycle_alarm_logs`,
        and move the file to `.bckup/`.
        """
        resolved = csv_path.resolve()
        if resolved in self._processing_files or not resolved.is_file():
            return 0

        self._processing_files.add(resolved)
        try:
            # Small settle delay to ensure the writer has finished flushing the file
            await asyncio.sleep(0.05)
            if not resolved.is_file():
                return 0

            parsed_rows = await asyncio.to_thread(parse_alarm_csv_file, resolved)
            fallback_cycle_id = await self._resolve_target_cycle_id()

            if parsed_rows:
                for row in parsed_rows:
                    if row["cycle_id"] is None:
                        row["cycle_id"] = fallback_cycle_id

                async with get_Session_context() as session:
                    await session.execute(insert(CycleAlarmLog), parsed_rows)

                self.ingested_alarms_count += len(parsed_rows)
                logger.info(
                    "[CSV WATCHER] Ingested %d alarm(s) from '%s' (cycle_id=%s)",
                    len(parsed_rows),
                    resolved.name,
                    fallback_cycle_id,
                )

            # Move file to .bckup subfolder
            self.backup_dir.mkdir(parents=True, exist_ok=True)
            dest_path = self.backup_dir / resolved.name
            if dest_path.exists():
                ts_suffix = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
                dest_path = (
                    self.backup_dir / f"{resolved.stem}_{ts_suffix}{resolved.suffix}"
                )

            await asyncio.to_thread(shutil.move, str(resolved), str(dest_path))
            self.processed_files_count += 1
            logger.info(
                "[CSV WATCHER] Moved processed CSV '%s' -> '%s'",
                resolved.name,
                dest_path.relative_to(self.watch_dir.parent),
            )
            return len(parsed_rows)

        except Exception as exc:
            logger.exception(
                "Error processing alarm CSV file '%s': %s", resolved, exc
            )
            return 0
        finally:
            self._processing_files.discard(resolved)
