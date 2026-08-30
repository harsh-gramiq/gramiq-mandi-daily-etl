"""
GramIQ MandiBhav — Active National Matrix Loader
================================================
Loads and validates the 1,799 active commodity-state pairs mapping.
Provides resilient fallbacks if the matrix file is missing.
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


def load_active_task_matrix(matrix_path: str | Path | None = None) -> list[dict[str, Any]]:
    """
    Loads the verified active (commodity, state) task matrix covering all cultivated
    crops across India. Falls back to the core priority matrix if unavailable.
    """
    target_path = Path(matrix_path) if matrix_path else ACTIVE_MATRIX_PATH

    if target_path.exists():
        try:
            with open(target_path, "r", encoding="utf-8") as f:
                tasks = json.load(f)
                if tasks and isinstance(tasks, list):
                    return tasks
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
    return core_tasks
