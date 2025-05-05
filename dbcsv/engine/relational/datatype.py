import datetime
from functools import lru_cache
from typing import Any, Dict, List


class DBTypeObject:
    def __init__(self, *values: str):
        # Use frozenset for immutable set optimization
        self.values = frozenset(v.lower() for v in values)
        # Store original values for display purposes
        self.original_values = values

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, str) and other.lower() in self.values

    def __contains__(self, item: str) -> bool:
        return item.lower() in self.values

    @staticmethod
    @lru_cache(maxsize=1024)  # Cache common conversions
    def convert_datatype(data: str, dtype: str = "") -> Any:
        dtype = dtype.lower()

        # Fast path for empty data
        if not data:
            return None

        # Fast path for string types (most common case first)
        if dtype in STRING:
            if len(data) > 1 and data[0] == "'" and data[-1] == "'":
                return data[1:-1]
            return data

        # Check for quoted strings for non-string types
        if len(data) > 1 and data[0] == "'" and data[-1] == "'" and dtype not in STRING:
            raise ValueError(
                f"Invalid {dtype} format: {data} is a string, not a {dtype}"
            )

        # Numeric types
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

        # Boolean type
        if dtype in BOOLEAN:
            data_lower = data.lower()
            if data_lower == "true":
                return True
            elif data_lower == "false":
                return False
            raise ValueError(f"Invalid boolean format: {data}")

        # Date/time types
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

        # NULL type
        if dtype in NULL:
            if data.lower() == "null":
                return None
            raise ValueError(f"Invalid null format: {data}")

        # Fallback: best-effort conversion
        try:
            if len(data) > 1 and data[0] == "'" and data[-1] == "'":
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

        # Use dict comprehension with direct conversion
        return {
            key: DBTypeObject.convert_datatype(str(row[key]), dtype)
            for key, dtype in zip(row.keys(), column_types)
        }

    def __repr__(self):
        return f"DBTypeObject({', '.join(self.original_values)})"


# Define database type categories as module-level constants
STRING = DBTypeObject("VARCHAR", "TEXT", "CHAR")
INTEGER = DBTypeObject("INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT")
FLOAT = DBTypeObject("FLOAT", "DOUBLE", "DECIMAL", "DEC")
BOOLEAN = DBTypeObject("BOOLEAN", "BOOL")
DATE = DBTypeObject("DATE")
DATETIME = DBTypeObject("DATETIME", "TIMESTAMP")
NULL = DBTypeObject("NULL")
