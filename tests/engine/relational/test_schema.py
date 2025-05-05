import csv

import pytest
import yaml

from dbcsv.engine.exceptions import DatabaseException
from dbcsv.engine.relational.schema import Schema


@pytest.fixture
def sample_schema(tmp_path):
    schema_name = "schema1"
    table_name = "table1"
    columns = [
        {"column_name": "id", "column_type": "INT"},
        {"column_name": "name", "column_type": "VARCHAR"},
        {"column_name": "age", "column_type": "INT"},
        {"column_name": "email", "column_type": "VARCHAR"},
    ]
    table_data = [
        [1, "John Doe", 28, "john.doe@example.com"],
        [2, "Jane Smith", 34, "jane.smith@example.com"],
        [3, "Michael Brown", 22, "michael.brown@example.com"],
        [4, "Emily Davis", 29, "emily.davis@example.com"],
        [5, "Chris Wilson", 31, "chris.wilson@example.com"],
    ]

    schema_dir = tmp_path / schema_name
    schema_dir.mkdir()

    # Create CSV file
    csv_path = schema_dir / f"{table_name}.csv"
    with csv_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([c["column_name"] for c in columns])
        writer.writerows(table_data)

    # Create metadata.yaml
    metadata = {"tables": [{"table_name": table_name, "columns": columns}]}
    metadata_path = schema_dir / "metadata.yaml"
    with metadata_path.open("w") as f:
        yaml.dump(metadata, f)

    # Patch STORAGE_PATH temporarily
    import dbcsv.engine.setting

    original_path = dbcsv.engine.setting.STORAGE_PATH
    dbcsv.engine.setting.STORAGE_PATH = tmp_path

    yield {
        "schema_name": schema_name,
        "table_name": table_name,
        "columns": columns,
        "original_path": original_path,
    }

    # Cleanup
    dbcsv.engine.setting.STORAGE_PATH = original_path


def test_schema_load_and_access(sample_schema):
    schema = Schema.load(sample_schema["schema_name"])

    assert schema.schema_name == sample_schema["schema_name"]
    assert sample_schema["table_name"] in schema.list_table_names()
    assert schema.has_table(sample_schema["table_name"])
    assert schema.has_column(sample_schema["table_name"], "email")
    assert not schema.has_column(sample_schema["table_name"], "salary")

    table = schema.tables[sample_schema["table_name"]]
    assert table.table_name == sample_schema["table_name"]
    rows = list(table.load_data_gen())
    assert rows[0]["name"] == "John Doe"
    assert rows[-1]["email"] == "chris.wilson@example.com"


def test_schema_not_found(tmp_path):
    import dbcsv.engine.setting

    original_path = dbcsv.engine.setting.STORAGE_PATH
    dbcsv.engine.setting.STORAGE_PATH = tmp_path

    with pytest.raises(DatabaseException, match="Schema: not_exists not exists"):
        Schema.load("not_exists")

    dbcsv.engine.setting.STORAGE_PATH = original_path
