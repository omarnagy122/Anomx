from __future__ import annotations

import runpy
from pathlib import Path

DASHBOARD_APP = Path(__file__).resolve().parents[1] / "src" / "dashboard" / "app.py"
runpy.run_path(str(DASHBOARD_APP), run_name="__main__")
