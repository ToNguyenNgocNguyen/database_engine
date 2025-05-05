import datetime
from typing import Any, Dict, List


class DBTypeObject:
    def __init__(self, *values: str):
        self.values = set(v.lower() for v in values)

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, str) and other.lower() in self.values

    def __contains__(self, item: str) -> bool:
        return item.lower() in self.values

    @staticmethod
    def convert_datatype(data: str, dtype: str = "") -> Any:
        dtype = dtype.lower()

        if dtype in STRING:
            if data.startswith("'") and data.endswith("'"):
                return data[1:-1]
            return data

        if data.startswith("'") and data.endswith("'") and dtype not in STRING:
            raise ValueError(
                f"Invalid {dtype} format: {data} is a string, not a {dtype}"
            )

        if dtype in INTEGER:
            try:
                return int(data)
            except ValueError:
                raise ValueError(f"Invalid integer format: {data}")

        if dtype in FLOAT:
            try:
                return float(data)
            except ValueError:
                raise ValueError(f"Invalid float format: {data}")

        if dtype in BOOLEAN:
            if data.lower() in {"true", "false"}:
                return data.lower() == "true"
            else:
                raise ValueError(f"Invalid boolean format: {data}")

        if dtype in DATE:
            try:
                return datetime.datetime.strptime(data, "%Y-%m-%d").date()
            except ValueError:
                raise ValueError(f"Invalid date format (expected %Y-%m-%d): {data}")

        if dtype in DATETIME:
            try:
                return datetime.datetime.strptime(data, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                raise ValueError(
                    f"Invalid datetime format (expected %Y-%m-%d %H:%M:%S): {data}"
                )

        if dtype in NULL:
            if data.lower() == "null":
                return None
            else:
                raise ValueError(f"Invalid null format: {data}")

        # Fallback: best-effort guess
        try:
            if data.startswith("'") and data.endswith("'"):
                return data[1:-1]
            return int(data)
        except ValueError:
            try:
                return float(data)
            except ValueError:
                return data

    @staticmethod
    def convert_rowtype(row: Dict[str, Any], column_types: List[str]) -> Dict[str, Any]:
        if len(row) != len(column_types):
            raise ValueError(
                f"Row length {len(row)} does not match column types length {len(column_types)}"
            )
        return {
            key: DBTypeObject.convert_datatype(row[key], dtype)
            for key, dtype in zip(row.keys(), column_types)
        }


# Define database type categories
STRING = DBTypeObject("VARCHAR", "TEXT", "CHAR")
INTEGER = DBTypeObject("INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT")
FLOAT = DBTypeObject("FLOAT", "DOUBLE", "DECIMAL", "DEC")
BOOLEAN = DBTypeObject("BOOLEAN", "BOOL")
DATE = DBTypeObject("DATE")
DATETIME = DBTypeObject("DATETIME", "TIMESTAMP")
NULL = DBTypeObject("NULL")
