"""
GramIQ MandiBhav — Active National Matrix Loader
================================================
Loads, validates, and interleaves the 1,799 active commodity-state pairs mapping.
Provides resilient round-robin state scheduling to prevent single-state starvation
and upstream rate-limit bottlenecks.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Any

from app.config import (
    ACTIVE_MATRIX_PATH,
    CORE_PRIORITY_CROPS,
    CORE_PRIORITY_STATES,
)

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass


def interleave_tasks_by_state(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Reorganizes tasks in a round-robin interleaved fashion across all unique states.
    Ensures parallel workers query distinct states continuously, preventing
    state-level starvation and distributing requests evenly across regional hubs.
    """
    if not tasks:
        return []

    state_buckets: dict[str, list[dict[str, Any]]] = {}
    for t in tasks:
        s_name = t.get("state_name") or f"State_{t.get('state_id', '')}"
        state_buckets.setdefault(s_name, []).append(t)

    interleaved: list[dict[str, Any]] = []
    max_len = max((len(bucket) for bucket in state_buckets.values()), default=0)

    for idx in range(max_len):
        for s_name in sorted(state_buckets.keys()):
            bucket = state_buckets[s_name]
            if idx < len(bucket):
                interleaved.append(bucket[idx])

    return interleaved


def load_active_task_matrix(
    matrix_path: str | Path | None = None, interleave: bool = True
) -> list[dict[str, Any]]:
    """
    Loads the verified active (commodity, state) task matrix covering all cultivated
    crops across India. Returns interleaved tasks across states by default.
    """
    target_path = Path(matrix_path) if matrix_path else ACTIVE_MATRIX_PATH

    if target_path.exists():
        try:
            with open(target_path, "r", encoding="utf-8") as f:
                tasks = json.load(f)
                if tasks and isinstance(tasks, list):
                    return interleave_tasks_by_state(tasks) if interleave else tasks
        except Exception as e:
            print(f"  [Matrix] ⚠️ Error loading {target_path.name}: {e}")

    # Fallback to Core Priority Matrix
    print("  [Matrix] ⚠️ Using core priority matrix fallback...")
    core_tasks = []
    for cid, cname in CORE_PRIORITY_CROPS:
        for sid in CORE_PRIORITY_STATES:
            core_tasks.append({
                "commodity_id": cid,
                "state_id": sid,
                "commodity_name": cname,
                "state_name": f"State_{sid}",
            })
    return interleave_tasks_by_state(core_tasks) if interleave else core_tasks
