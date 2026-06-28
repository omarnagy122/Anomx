"""Run local AnomX handoff checks without starting Docker containers."""
from __future__ import annotations

import compileall
import shutil
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _run(command: list[str]) -> None:
    print(f"\n[check] {' '.join(command)}")
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def cleanup_runtime_artifacts() -> None:
    removed = 0
    for path in list(PROJECT_ROOT.rglob("__pycache__")) + list(PROJECT_ROOT.rglob(".pytest_cache")):
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
            removed += 1
    for path in PROJECT_ROOT.rglob("*.pyc"):
        path.unlink(missing_ok=True)
        removed += 1
    for path in PROJECT_ROOT.rglob("*.log"):
        path.unlink(missing_ok=True)
        removed += 1
    print(f"[cleanup] removed {removed} runtime artifact path(s)")


def check_yaml_files() -> None:
    print("\n[check] docker compose YAML parse")
    try:
        import yaml  # type: ignore
    except ModuleNotFoundError:
        print("[skip] PyYAML is not installed; Docker Compose itself can still validate with `docker compose config`.")
        return
    for filename in ("docker-compose.yml", "docker-compose.airflow.yml"):
        data = yaml.safe_load((PROJECT_ROOT / filename).read_text(encoding="utf-8"))
        services = data.get("services", {}) if isinstance(data, dict) else {}
        if not services:
            raise SystemExit(f"{filename} has no services")
        print(f"[ok] {filename}: {len(services)} services")


def check_compileall() -> None:
    print("\n[check] Python compileall")
    ok = compileall.compile_dir(PROJECT_ROOT, quiet=1)
    if not ok:
        raise SystemExit("compileall failed")
    print("[ok] Python files compile")


def main() -> None:
    cleanup_runtime_artifacts()
    check_yaml_files()
    check_compileall()
    _run([sys.executable, "-m", "pytest", "-q"])
    _run([sys.executable, "scripts/quick_local_demo_without_kafka.py"])
    cleanup_runtime_artifacts()
    print("\n[ok] all local checks passed")


if __name__ == "__main__":
    main()
