import csv

import pytest

from dbcsv.engine.exceptions import DatabaseException
from dbcsv.engine.relational.table import Table


@pytest.fixture
def csv_employee_table(tmp_path):
    schema = "schema1"
    table = "table1"
    columns = [
        {"column_name": "id", "column_type": "INT"},
        {"column_name": "name", "column_type": "VARCHAR"},
        {"column_name": "age", "column_type": "INT"},
        {"column_name": "email", "column_type": "VARCHAR"},
    ]
    expected_data = [
        {"id": 1, "name": "John Doe", "age": 28, "email": "john.doe@example.com"},
        {"id": 2, "name": "Jane Smith", "age": 34, "email": "jane.smith@example.com"},
        {
            "id": 3,
            "name": "Michael Brown",
            "age": 22,
            "email": "michael.brown@example.com",
        },
        {"id": 4, "name": "Emily Davis", "age": 29, "email": "emily.davis@example.com"},
        {
            "id": 5,
            "name": "Chris Wilson",
            "age": 31,
            "email": "chris.wilson@example.com",
        },
    ]

    schema_dir = tmp_path / schema
    schema_dir.mkdir()
    table_path = schema_dir / f"{table}.csv"

    with table_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "age", "email"])
        for row in expected_data:
            writer.writerow([row["id"], row["name"], row["age"], row["email"]])

    # Patch STORAGE_PATH temporarily
    import dbcsv.engine.setting

    original_path = dbcsv.engine.setting.STORAGE_PATH
    dbcsv.engine.setting.STORAGE_PATH = tmp_path

    yield {
        "schema_name": schema,
        "table_name": table,
        "columns": columns,
        "expected": expected_data,
    }

    dbcsv.engine.setting.STORAGE_PATH = original_path


def test_load_employee_table(csv_employee_table):
    table = Table.load(
        csv_employee_table["schema_name"],
        csv_employee_table["table_name"],
        csv_employee_table["columns"],
    )
    assert table.table_name == "table1"
    assert table.column_names == ["id", "name", "age", "email"]
    assert table.column_types == ["INT", "VARCHAR", "INT", "VARCHAR"]

    rows = list(table.load_data_gen())
    assert rows == csv_employee_table["expected"]
    assert rows[0]["name"] == "John Doe"
    assert rows[-1]["email"] == "chris.wilson@example.com"


def test_missing_table_error(csv_employee_table):
    with pytest.raises(DatabaseException, match=r"Table: non_existing not exists"):
        Table.load("invalid_schema", "non_existing", csv_employee_table["columns"])
