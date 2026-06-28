from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_export_script_and_output_folder_exist():
    assert (PROJECT_ROOT / "scripts" / "export_processed_data_to_csv.py").is_file()
    assert (PROJECT_ROOT / "exports" / "processed" / ".gitkeep").is_file()


def test_export_script_uses_copy_with_csv_header():
    source = (PROJECT_ROOT / "scripts" / "export_processed_data_to_csv.py").read_text(encoding="utf-8")
    assert "processed_sensor_data" in source
    assert "copy_expert" in source
    assert "WITH CSV HEADER" in source
    assert "POSTGRES_HOST" in source
    assert "exports" in source and "processed" in source


def test_export_script_cli_defaults():
    import scripts.export_processed_data_to_csv as exporter

    parser = exporter.build_parser()
    args = parser.parse_args([])

    assert args.schema == "public"
    assert args.table == "processed_sensor_data"
    assert args.output == exporter.DEFAULT_OUTPUT
    assert args.order_by == "id"


def test_docker_compose_contains_data_exporter_service():
    compose = (PROJECT_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "data-exporter:" in compose
    assert "scripts/export_processed_data_to_csv.py" in compose
    assert "./exports:/app/exports" in compose
    assert "processed_sensor_data.csv" in compose


def test_export_documentation_exists_and_is_linked():
    docs = (PROJECT_ROOT / "docs" / "EXPORT_PROCESSED_DATA.md").read_text(encoding="utf-8")
    readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")
    commands = (PROJECT_ROOT / "COMMANDS.md").read_text(encoding="utf-8")

    assert "processed_sensor_data" in docs
    assert "docker compose run --rm data-exporter" in docs
    assert "docs/EXPORT_PROCESSED_DATA.md" in readme
    assert "Export processed data for model training" in commands
