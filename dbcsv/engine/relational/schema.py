from pathlib import Path
from typing import Any, Dict, List

import yaml

from dbcsv.engine.exceptions import DatabaseException
from dbcsv.engine.relational.table import Table
from dbcsv.engine.setting import STORAGE_PATH


class Schema:
    _schema_name: str
    _schema_path: Path
    _metadata: Dict[str, Any]
    _metadata_path: Path
    _tables: Dict[str, Table] = {}

    def __init__(self, schema_name: str) -> None:
        self._schema_name = schema_name

    @property
    def schema_name(self) -> str:
        return self._schema_name

    @property
    def schema_path(self) -> str:
        return self._schema_path

    @property
    def metadata(self) -> Dict[str, Any]:
        return self._metadata

    @property
    def metadata_path(self) -> Dict[str, Any]:
        return self._metadata_path

    @property
    def tables(self) -> Dict[str, Table]:
        return self._tables

    @classmethod
    def load(cls, schema_name: str) -> "Schema":
        schema_path = STORAGE_PATH.joinpath(schema_name)
        metadata_path = schema_path.joinpath("metadata.yaml")

        if (not schema_path.exists()) or (schema_path is None):
            raise DatabaseException(f"Schema: {schema_name} not exists")

        if (not metadata_path.exists()) or (metadata_path is None):
            raise DatabaseException("Metadata not exists")

        schema = cls(schema_name)
        schema._schema_path = schema_path
        schema._metadata_path = metadata_path

        with open(metadata_path, "r") as f:
            schema._metadata = yaml.load(f, Loader=yaml.SafeLoader)

        for table in schema.metadata["tables"]:
            schema.load_table(
                schema_name=schema_name,
                table_name=table["table_name"],
                columns=table["columns"],
            )
        return schema

    def load_table(
        self, schema_name: str, table_name: str, columns: Dict[str, Any]
    ) -> Table:
        table = Table.load(schema_name, table_name, columns)
        self._tables[table_name] = table
        return table

    def list_table_names(self) -> List[str]:
        return [key for key in self._tables.keys()]

    def has_table(self, table: str) -> bool:
        return table in self.tables.keys()

    def has_column(self, table: str, column: str) -> bool:
        return self.has_table(table) and self.tables[table].has_column(column)
