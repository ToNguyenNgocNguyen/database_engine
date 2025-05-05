# import csv
# from collections.abc import Generator
import csv
from collections.abc import Generator
from pathlib import Path
from typing import Any, Dict, List, Literal

from dbcsv.engine.exceptions import DatabaseException
from dbcsv.engine.relational.datatype import DBTypeObject
from dbcsv.engine.setting import STORAGE_PATH

MODE = Literal["r", "w", "a"]


class Table:
    _table_name: str
    _table_path: Path
    _column_names: List[str]
    _column_types: List[str]

    def __init__(
        self, table_name: str, column_names: List[str], column_types: List[str]
    ):
        self._table_name = table_name
        self._column_names = column_names
        self._column_types = column_types

    @property
    def table_name(self) -> str:
        return self._table_name

    @property
    def column_names(self) -> List[str]:
        return self._column_names

    @property
    def column_types(self) -> List[str]:
        return self._column_types

    @classmethod
    def load(
        cls, schema_name: str, table_name: str, columns: List[Dict[str, Any]]
    ) -> "Table":
        column_names = [column["column_name"] for column in columns]
        column_types = [column["column_type"] for column in columns]

        table_path = STORAGE_PATH.joinpath(f"{schema_name}/{table_name}.csv")

        if (not table_path.exists()) or (table_path is None):
            raise DatabaseException(f"Table: {table_name} not exists")

        table = cls(table_name, column_names, column_types)
        table._table_path = table_path

        return table

    def load_data_gen(self) -> Generator[Dict[str, Any], None, None]:
        with self._table_path.open("r") as file:
            reader = csv.DictReader(file)

            for row in reader:
                row = DBTypeObject.convert_rowtype(
                    row=row, column_types=self.column_types
                )
                yield row

    def has_column(self, column: str) -> bool:
        return column in self.column_names

    def __repr__(self):
        return f"Table(table_name={self.table_name!r})"
