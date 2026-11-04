"""Where a run's files live: `<root>/runs/<run_id>/`."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RunPaths:
    root: Path
    """The artifacts root, for example `<project>/target`."""
    run_id: str

    @property
    def run_dir(self) -> Path:
        return self.root / "runs" / self.run_id

    @property
    def manifest(self) -> Path:
        return self.run_dir / "manifest.json"

    @property
    def run_results(self) -> Path:
        return self.run_dir / "run_results.json"

    @property
    def units_dir(self) -> Path:
        return self.run_dir / "units"

    def events(self, invocation_id: str) -> Path:
        return self.run_dir / "events" / f"{invocation_id}.jsonl"

    def attempt(self, unit_id: str, n: int) -> Path:
        return self.units_dir / unit_id / f"attempt-{n}.json"

    def log(self, unit_id: str, n: int) -> Path:
        return self.run_dir / "logs" / f"{unit_id}-attempt-{n}.log"

    def attempt_numbers(self, unit_id: str) -> list[int]:
        folder = self.units_dir / unit_id
        if not folder.is_dir():
            return []
        found = (re.fullmatch(r"attempt-(\d+)\.json", p.name) for p in folder.iterdir())
        return sorted(int(m.group(1)) for m in found if m)

    def next_attempt(self, unit_id: str) -> int:
        """One above the highest attempt file: a reused run id keeps counting."""
        numbers = self.attempt_numbers(unit_id)
        return numbers[-1] + 1 if numbers else 1
