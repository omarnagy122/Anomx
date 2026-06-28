"""Export processed PostgreSQL data to a CSV file for model training.

The script is intentionally small and operational: it reads the same PostgreSQL
settings used by the rest of AnomX, validates that the target table exists, and
writes a CSV with headers to exports/processed by default.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import config  # noqa: E402

DEFAULT_SCHEMA = "public"
DEFAULT_TABLE = "processed_sensor_data"
DEFAULT_OUTPUT = PROJECT_ROOT / "exports" / "processed" / "processed_sensor_data.csv"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export processed PostgreSQL data to CSV for model training."
    )
    parser.add_argument(
        "--schema",
        default=DEFAULT_SCHEMA,
        help=f"PostgreSQL schema name. Default: {DEFAULT_SCHEMA}",
    )
    parser.add_argument(
        "--table",
        default=DEFAULT_TABLE,
        help=f"Table to export. Default: {DEFAULT_TABLE}",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"CSV output path. Default: {DEFAULT_OUTPUT.relative_to(PROJECT_ROOT)}",
    )
    parser.add_argument(
        "--order-by",
        default="id",
        help="Column used to order exported rows. Use an empty value to disable ordering. Default: id",
    )
    return parser


def _connection_kwargs() -> dict[str, object]:
    return {
        "host": config.POSTGRES_HOST,
        "port": config.POSTGRES_PORT,
        "dbname": config.POSTGRES_DB,
        "user": config.POSTGRES_USER,
        "password": config.POSTGRES_PASSWORD,
    }


def _table_exists(cursor, schema: str, table: str) -> bool:
    cursor.execute("SELECT to_regclass(%s);", (f"{schema}.{table}",))
    return cursor.fetchone()[0] is not None


def _table_columns(cursor, schema: str, table: str) -> list[str]:
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position;
        """,
        (schema, table),
    )
    return [row[0] for row in cursor.fetchall()]


def _table_count(cursor, schema: str, table: str) -> int:
    from psycopg2 import sql

    cursor.execute(
        sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
            sql.Identifier(schema),
            sql.Identifier(table),
        )
    )
    return int(cursor.fetchone()[0])


def _copy_query(schema: str, table: str, columns: Sequence[str], order_by: str | None):
    from psycopg2 import sql

    relation = sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(table))
    if order_by and order_by in columns:
        select_query = sql.SQL("SELECT * FROM {} ORDER BY {}" ).format(
            relation,
            sql.Identifier(order_by),
        )
    else:
        select_query = sql.SQL("SELECT * FROM {}" ).format(relation)
    return sql.SQL("COPY ({}) TO STDOUT WITH CSV HEADER").format(select_query)


def export_table_to_csv(schema: str, table: str, output: Path, order_by: str | None = "id") -> int:
    import psycopg2

    output = output if output.is_absolute() else PROJECT_ROOT / output
    output.parent.mkdir(parents=True, exist_ok=True)

    with psycopg2.connect(**_connection_kwargs()) as connection:
        with connection.cursor() as cursor:
            if not _table_exists(cursor, schema, table):
                raise RuntimeError(f"Table not found: {schema}.{table}")

            columns = _table_columns(cursor, schema, table)
            if not columns:
                raise RuntimeError(f"Table has no readable columns: {schema}.{table}")

            row_count = _table_count(cursor, schema, table)
            copy_sql = _copy_query(schema, table, columns, order_by).as_string(connection)
            with output.open("w", encoding="utf-8", newline="") as csv_file:
                cursor.copy_expert(copy_sql, csv_file)

    return row_count


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    order_by = args.order_by.strip() if args.order_by is not None else None
    try:
        row_count = export_table_to_csv(
            schema=args.schema,
            table=args.table,
            output=args.output,
            order_by=order_by,
        )
    except Exception as exc:  # pragma: no cover - operational error output
        print(f"[export:error] {exc}", file=sys.stderr)
        return 1

    output = args.output if args.output.is_absolute() else PROJECT_ROOT / args.output
    print(f"[export:ok] table={args.schema}.{args.table}")
    print(f"[export:ok] rows={row_count}")
    print(f"[export:ok] file={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
