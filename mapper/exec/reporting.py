from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
import traceback
from typing import Any, Mapping


def _stamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stringify(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    return value


def _normalize_fields(fields: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: _stringify(value)
        for key, value in fields.items()
        if value is not None and value != ""
    }


def _write_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(dict(payload), sort_keys=True, default=str) + "\n")


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True, default=str), encoding="utf-8")


@dataclass(frozen=True)
class StageEvent:
    timestamp: str
    level: str
    scope: str
    message: str
    stage_name: str | None = None
    task_name: str | None = None
    step_name: str | None = None
    fields: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StageErrorRecord:
    timestamp: str
    stage_name: str | None
    task_name: str | None
    error_type: str
    error_message: str
    traceback_text: str
    fields: dict[str, Any] = field(default_factory=dict)


class PipelineReporter:
    def __init__(
        self,
        *,
        run_name: str,
        event_log_path: Path | None = None,
        console: bool = True,
    ) -> None:
        self.run_name = run_name
        self.event_log_path = event_log_path
        self.console = console

    def _emit(self, level: str, message: str, **fields: Any) -> None:
        payload = StageEvent(
            timestamp=_stamp(),
            level=level,
            scope="pipeline",
            message=message,
            fields=_normalize_fields(fields),
        )
        if self.event_log_path is not None:
            _write_jsonl(self.event_log_path, asdict(payload))
        if self.console:
            kv = " ".join(f"{key}={value}" for key, value in payload.fields.items())
            suffix = f" {kv}" if kv else ""
            print(f"[pipeline] {message}{suffix}")

    def info(self, message: str, **fields: Any) -> None:
        self._emit("info", message, **fields)

    def warn(self, message: str, **fields: Any) -> None:
        self._emit("warn", message, **fields)

    def error(self, message: str, **fields: Any) -> None:
        self._emit("error", message, **fields)

    def exception(self, exc: BaseException, **fields: Any) -> None:
        error_fields = dict(fields)
        error_fields["error_type"] = exc.__class__.__name__
        error_fields["error_message"] = str(exc)
        self._emit("error", "pipeline_exception", **error_fields)


class StageReporter:
    def __init__(
        self,
        *,
        stage_name: str,
        task_name: str,
        event_log_path: Path,
        error_path: Path,
        console: bool = True,
    ) -> None:
        self.stage_name = stage_name
        self.task_name = task_name
        self.event_log_path = event_log_path
        self.error_path = error_path
        self.console = console

    def _console_prefix(self, level: str, step_name: str | None = None, compact: bool = False) -> str:
        if compact and step_name and level == "info":
            return f"[{step_name}]"
        prefix = f"[stage][{self.stage_name}]"
        if step_name:
            prefix += f"[{step_name}]"
        if level == "warn":
            prefix += "[warn]"
        elif level == "error":
            prefix += "[error]"
        return prefix

    def _emit(
        self,
        level: str,
        message: str,
        *,
        step_name: str | None = None,
        compact: bool = False,
        console: bool | None = None,
        **fields: Any,
    ) -> None:
        payload = StageEvent(
            timestamp=_stamp(),
            level=level,
            scope="stage",
            message=message,
            stage_name=self.stage_name,
            task_name=self.task_name,
            step_name=step_name,
            fields=_normalize_fields(fields),
        )
        _write_jsonl(self.event_log_path, asdict(payload))
        if self.console if console is None else console:
            kv = " ".join(f"{key}={value}" for key, value in payload.fields.items())
            suffix = f" {kv}" if kv else ""
            print(f"{self._console_prefix(level, step_name, compact=compact)} {message}{suffix}")

    def info(self, message: str, **fields: Any) -> None:
        self._emit("info", message, **fields)

    def warn(self, message: str, **fields: Any) -> None:
        self._emit("warn", message, **fields)

    def error(self, message: str, **fields: Any) -> None:
        self._emit("error", message, **fields)

    def step_start(self, step_name: str, *, compact: bool = False, **fields: Any) -> None:
        self._emit("info", "start", step_name=step_name, compact=compact, **fields)

    def step_done(self, step_name: str, *, compact: bool = False, **fields: Any) -> None:
        self._emit("info", "done", step_name=step_name, compact=compact, **fields)

    def step_skip(self, step_name: str, *, compact: bool = False, **fields: Any) -> None:
        self._emit("info", "skip", step_name=step_name, compact=compact, **fields)

    def artifact_written(self, key: str, path: Path, **fields: Any) -> None:
        payload = {"artifact_key": key, "path": path, **fields}
        self._emit("info", "artifact_written", console=False, **payload)

    def metric(self, name: str, value: Any, **fields: Any) -> None:
        payload = {"metric": name, "value": value, **fields}
        self._emit("info", "metric", console=False, **payload)

    def exception(self, exc: BaseException, **fields: Any) -> Path:
        record = StageErrorRecord(
            timestamp=_stamp(),
            stage_name=self.stage_name,
            task_name=self.task_name,
            error_type=exc.__class__.__name__,
            error_message=str(exc),
            traceback_text="".join(traceback.format_exception(exc)),
            fields=_normalize_fields(fields),
        )
        _write_json(self.error_path, asdict(record))
        self._emit(
            "error",
            "exception",
            error_type=record.error_type,
            error_message=record.error_message,
            error_path=self.error_path,
            **record.fields,
        )
        return self.error_path
