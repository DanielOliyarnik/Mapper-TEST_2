from __future__ import annotations

from dataclasses import dataclass
import shutil
import sys
import time
from typing import Any, Mapping


@dataclass
class ProgressState:
    label: str
    total: int | None = None
    unit: str | None = None
    completed: int = 0
    started_at: float | None = None
    last_tick_at: float | None = None
    last_emit_at: float | None = None
    avg_unit_sec: float | None = None


class ProgressRenderer:
    def update(self, line1: str, line2: str | None = None) -> None:
        raise NotImplementedError

    def finish(self) -> None:
        return None


class SingleLineRenderer(ProgressRenderer):
    def __init__(self, *, enabled: bool = True) -> None:
        self.enabled = enabled

    def update(self, line1: str, line2: str | None = None) -> None:
        if not self.enabled:
            return
        print(line1)
        if line2:
            print(line2)


class TwoLineRenderer(ProgressRenderer):
    def __init__(self, *, enabled: bool = True) -> None:
        self.enabled = enabled and sys.stdout.isatty()
        self._initialized = False

    def _clip(self, text: str) -> str:
        width = shutil.get_terminal_size((120, 20)).columns
        if len(text) <= width:
            return text
        return text[: max(0, width - 1)]

    def _ensure_init(self) -> None:
        if not self.enabled or self._initialized:
            return
        sys.stdout.write("\n\n")
        sys.stdout.flush()
        self._initialized = True

    def update(self, line1: str, line2: str | None = None) -> None:
        if not self.enabled:
            print(line1)
            if line2:
                print(line2)
            return
        self._ensure_init()
        second = line2 or ""
        out = "\x1b[2A"
        out += "\x1b[2K\r" + self._clip(line1) + "\n"
        out += "\x1b[2K\r" + self._clip(second) + "\n"
        sys.stdout.write(out)
        sys.stdout.flush()

    def finish(self) -> None:
        if not self.enabled:
            return
        sys.stdout.write("\x1b[2K\r\n\x1b[2K\r\n")
        sys.stdout.flush()


def _coerce_bool(value: Any, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _coerce_int(value: Any, default: int) -> int:
    if value is None:
        return default
    return int(value)


def _coerce_float(value: Any, default: float) -> float:
    if value is None:
        return default
    return float(value)


class ProgressTracker:
    def __init__(
        self,
        *,
        stage_name: str,
        task_name: str,
        enabled: bool = True,
        style: str = "single_line",
        every_n: int = 1,
        min_seconds: float = 1.0,
        show_eta: bool = True,
    ) -> None:
        self.stage_name = stage_name
        self.task_name = task_name
        self.enabled = enabled
        self.every_n = max(1, every_n)
        self.min_seconds = max(0.0, min_seconds)
        self.show_eta = show_eta
        self.state: ProgressState | None = None
        if style == "two_line":
            self.renderer: ProgressRenderer = TwoLineRenderer(enabled=enabled)
        else:
            self.renderer = SingleLineRenderer(enabled=enabled)

    @classmethod
    def from_config(
        cls,
        *,
        stage_name: str,
        task_name: str,
        reporting_cfg: Mapping[str, Any] | None,
    ) -> "ProgressTracker":
        cfg = dict(reporting_cfg or {})
        return cls(
            stage_name=stage_name,
            task_name=task_name,
            enabled=_coerce_bool(cfg.get("progress"), True),
            style=str(cfg.get("progress_style", "single_line")),
            every_n=_coerce_int(cfg.get("progress_every"), 1),
            min_seconds=_coerce_float(cfg.get("progress_min_seconds"), 1.0),
            show_eta=_coerce_bool(cfg.get("show_eta"), True),
        )

    def start(self, label: str, total: int | None = None, unit: str | None = None) -> None:
        if not self.enabled:
            return
        now = time.time()
        self.state = ProgressState(
            label=label,
            total=total if total is None else max(0, int(total)),
            unit=unit,
            started_at=now,
            last_tick_at=now,
            last_emit_at=None,
        )

    def _should_emit(self, completed: int, total: int | None, force: bool, now: float) -> bool:
        if not self.enabled:
            return False
        if force:
            return True
        if completed <= 0:
            return False
        if total is not None and completed >= total:
            return True
        if completed % self.every_n != 0:
            return False
        if self.state and self.state.last_emit_at is not None and now - self.state.last_emit_at < self.min_seconds:
            return False
        return True

    def _update_average(self, completed: int, now: float) -> None:
        if self.state is None:
            return
        last_tick_at = self.state.last_tick_at or now
        dt = now - last_tick_at
        delta_units = max(0, completed - self.state.completed)
        if delta_units > 0 and dt >= 0:
            sample = dt / float(delta_units)
            if self.state.avg_unit_sec is None:
                self.state.avg_unit_sec = sample
            else:
                self.state.avg_unit_sec = 0.9 * self.state.avg_unit_sec + 0.1 * sample
        self.state.completed = completed
        self.state.last_tick_at = now

    @staticmethod
    def _format_clock(seconds: float) -> str:
        seconds = max(0, int(seconds))
        minutes, sec = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        return f"{hours:02d}:{minutes:02d}:{sec:02d}"

    @staticmethod
    def _format_extra(extra: Mapping[str, Any] | str | None) -> str:
        if extra is None:
            return ""
        if isinstance(extra, str):
            return extra
        parts: list[str] = []
        for key, value in extra.items():
            if value is None or value == "":
                continue
            parts.append(f"{key}={value}")
        return " ".join(parts)

    def update(
        self,
        completed: int,
        *,
        total: int | None = None,
        loss: float | None = None,
        extra: Mapping[str, Any] | str | None = None,
        detail: str | None = None,
        force: bool = False,
    ) -> None:
        if not self.enabled:
            return
        if self.state is None:
            self.start(label=f"{self.stage_name}.{self.task_name}", total=total)
        if self.state is None:
            return
        if total is not None:
            self.state.total = int(total)
        now = time.time()
        self._update_average(completed, now)
        current_total = self.state.total
        if not self._should_emit(completed, current_total, force, now):
            return
        elapsed = now - (self.state.started_at or now)
        pct = None
        eta = None
        if current_total is not None and current_total > 0:
            pct = 100.0 * min(1.0, completed / float(current_total))
            if self.show_eta and self.state.avg_unit_sec is not None and completed < current_total:
                eta = self.state.avg_unit_sec * max(0, current_total - completed)
        bar = ""
        if current_total is not None and current_total > 0:
            bar_len = 20
            filled = int(bar_len * min(1.0, completed / float(current_total)))
            bar = "[" + "#" * filled + "." * (bar_len - filled) + "]"
        prefix = f"[stage][{self.stage_name}]"
        label = self.state.label
        parts = [prefix, label]
        if current_total is not None and current_total > 0:
            parts.append(f"{completed}/{current_total}")
            parts.append(bar)
            parts.append(f"{pct:5.1f}%")
        else:
            parts.append(f"count={completed}")
        parts.append(f"elapsed={self._format_clock(elapsed)}")
        if eta is not None:
            parts.append(f"eta={self._format_clock(eta)}")
        if loss is not None:
            parts.append(f"loss={loss:.4g}")
        extra_text = self._format_extra(extra)
        if extra_text:
            parts.append(extra_text)
        line1 = " ".join(part for part in parts if part)
        self.renderer.update(line1, detail)
        self.state.last_emit_at = now

    def finish(self, *, status: str = "done", extra: Mapping[str, Any] | str | None = None) -> None:
        if not self.enabled:
            return
        if self.state is not None:
            self.update(
                self.state.completed,
                total=self.state.total,
                extra=extra,
                force=True,
            )
        self.renderer.finish()

