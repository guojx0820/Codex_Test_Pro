#!/usr/bin/env python3
"""Cross-platform launcher for the GUI app.

- Uses existing virtual env if present.
- Creates venv without installing extra packages.
- Falls back to system Python if venv creation is unavailable.
"""

from __future__ import annotations

import os
import platform
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
APP = ROOT / "app.py"
VENV = ROOT / ".venv"


def _venv_python_path() -> Path:
    if platform.system().lower().startswith("win"):
        return VENV / "Scripts" / "python.exe"
    return VENV / "bin" / "python"


def _run(cmd: list[str]) -> int:
    proc = subprocess.run(cmd, cwd=str(ROOT))
    return proc.returncode


def ensure_venv() -> Path | None:
    py = _venv_python_path()
    if py.exists():
        return py

    print("[INFO] Creating virtual environment: .venv")
    try:
        code = _run([sys.executable, "-m", "venv", str(VENV)])
        if code != 0:
            print("[WARN] venv creation failed, fallback to system Python.")
            return None
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] venv creation error: {exc}")
        return None

    return py if py.exists() else None


def main() -> int:
    if not APP.exists():
        print("[ERROR] app.py not found.")
        return 1

    venv_python = ensure_venv()
    if venv_python:
        print(f"[INFO] Launching GUI with venv Python: {venv_python}")
        return _run([str(venv_python), str(APP)])

    print(f"[INFO] Launching GUI with system Python: {sys.executable}")
    return _run([sys.executable, str(APP)])


if __name__ == "__main__":
    raise SystemExit(main())
