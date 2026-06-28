from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_command_reference_exists_and_covers_operational_sections():
    commands = (PROJECT_ROOT / "COMMANDS.md").read_text(encoding="utf-8")
    required_phrases = [
        "Docker Compose validation",
        "Direct Kafka simulation flow",
        "Optional MQTT machine simulation flow",
        "PostgreSQL checks",
        "Manual prediction commands",
        "Export processed data for model training",
        "Airflow commands",
        "Backup and restore commands",
        "Local tests and checks",
        "Git commands",
    ]
    for phrase in required_phrases:
        assert phrase in commands


def test_readme_is_description_only_and_points_to_commands_file():
    readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")
    assert "COMMANDS.md" in readme
    assert "docker compose up" not in readme.lower()
    assert "python -m pytest" not in readme.lower()
    assert "git push" not in readme.lower()


def test_root_reports_were_moved_to_docs_reports():
    assert not (PROJECT_ROOT / "MERGE_REPORT.md").exists()
    assert not (PROJECT_ROOT / "TEST_REPORT.md").exists()
    assert (PROJECT_ROOT / "docs" / "reports" / "MERGE_REPORT.md").exists()
    assert (PROJECT_ROOT / "docs" / "reports" / "TEST_REPORT.md").exists()


def test_ignore_files_exclude_runtime_artifacts():
    gitignore = (PROJECT_ROOT / ".gitignore").read_text(encoding="utf-8")
    dockerignore = (PROJECT_ROOT / ".dockerignore").read_text(encoding="utf-8")
    for pattern in ["__pycache__/", "*.py[cod]", ".pytest_cache/", "orchestration/airflow/logs/", "*.log"]:
        assert pattern in gitignore
        assert pattern in dockerignore
    assert "infra/db/backups/*.sql" in gitignore
    assert "infra/db/backups/*.sql" in dockerignore
    assert "exports/processed/*.csv" in gitignore
    assert "exports/processed/*.csv" in dockerignore
    assert "!exports/processed/.gitkeep" in gitignore


def test_requirements_include_pipeline_dependencies():
    requirements = (PROJECT_ROOT / "requirements.txt").read_text(encoding="utf-8").lower()
    for package in [
        "pandas",
        "psycopg2-binary",
        "kafka-python-ng",
        "paho-mqtt",
        "pyspark",
        "pytest",
    ]:
        assert package in requirements

    airflow_requirements = (PROJECT_ROOT / "orchestration" / "airflow" / "requirements.txt").read_text(encoding="utf-8").lower()
    for package in ["pandas", "psycopg2-binary", "kafka-python-ng", "paho-mqtt", "pyspark"]:
        assert package in airflow_requirements


def test_root_is_organized_into_source_orchestration_and_infra_layers():
    assert (PROJECT_ROOT / "src" / "ingestion").is_dir()
    assert (PROJECT_ROOT / "src" / "prediction").is_dir()
    assert (PROJECT_ROOT / "orchestration" / "airflow").is_dir()
    assert (PROJECT_ROOT / "infra" / "db" / "init.sql").is_file()
    assert (PROJECT_ROOT / "infra" / "mqtt" / "mosquitto" / "config" / "mosquitto.conf").is_file()
    assert not (PROJECT_ROOT / "ingestion").exists()
    assert not (PROJECT_ROOT / "prediction").exists()
    assert not (PROJECT_ROOT / "airflow").exists()
    assert not (PROJECT_ROOT / "db").exists()
