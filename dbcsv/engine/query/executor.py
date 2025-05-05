import json
import re
from datetime import date, datetime
from typing import Any, Dict, Generator, List

from dbcsv.engine.relational.datatype import DBTypeObject
from dbcsv.engine.relational.schema import Schema

# Precompiled regex
RE_NUMERIC_LITERAL = re.compile(r"^-?\d+(\.\d+)?$")


class SelectExecutor:
    def __init__(self, schema: Schema):
        self.schema = schema

    def execute(self, query: Dict[str, Any]) -> Generator[str, None, None]:
        table_name = query["FROM"]
        select_columns = query["SELECT"]
        where_clause = query.get("WHERE")

        table = self.schema.tables[table_name]
        self.columns = dict(zip(table.column_names, table.column_types))
        data_gen = table.load_data_gen()

        for row in data_gen:
            if not where_clause or self.evaluate_condition(where_clause, row):
                yield self.format_row(row, select_columns)

    def format_row(self, row: Dict[str, Any], select_columns: List[str]) -> str:
        if select_columns == ["*"]:
            projected = [self.convert_value(v) for v in row.values()]
        else:
            projected = [
                self.convert_value(row[col]) for col in select_columns if col in row
            ]
        return json.dumps(projected)

    @staticmethod
    def convert_value(val: Any) -> Any:
        return val.isoformat() if isinstance(val, (date, datetime)) else val

    def evaluate_condition(self, expr: Dict[str, Any], row: Dict[str, Any]) -> bool:
        op = expr["op"]
        if op == "AND":
            return self.evaluate_condition(
                expr["left"], row
            ) and self.evaluate_condition(expr["right"], row)
        elif op == "OR":
            return self.evaluate_condition(
                expr["left"], row
            ) or self.evaluate_condition(expr["right"], row)

        # Leaf condition
        left_val, left_type = self.resolve_operand(expr["left"], row)
        right_val, right_type = self.resolve_operand(expr["right"], row)

        # Avoid unnecessary conversions if types already match or are literals
        if left_type and not isinstance(left_val, DBTypeObject):
            try:
                left_val = DBTypeObject.convert_datatype(left_val, left_type)
            except Exception:
                pass

        if right_type and not isinstance(right_val, DBTypeObject):
            try:
                right_val = DBTypeObject.convert_datatype(right_val, right_type)
            except Exception:
                pass

        return self.compare(left_val, op, right_val)

    def resolve_operand(
        self, operand: str, row: Dict[str, Any]
    ) -> tuple[Any, str | None]:
        operand_uc = operand.upper()
        if self.is_string_literal(operand):
            return operand[1:-1], None
        elif self.is_numeric_literal(operand):
            return self.cast_number(operand), None
        elif operand_uc == "TRUE":
            return 1, None
        elif operand_uc == "FALSE":
            return 0, None
        elif operand in row:
            return row[operand], self.columns.get(operand)
        else:
            return operand, None

    @staticmethod
    def is_string_literal(value: str) -> bool:
        return value.startswith("'") and value.endswith("'")

    @staticmethod
    def is_numeric_literal(value: str) -> bool:
        return RE_NUMERIC_LITERAL.match(value) is not None

    @staticmethod
    def cast_number(value: str) -> Any:
        return int(value) if "." not in value else float(value)

    @staticmethod
    def compare(left: Any, op: str, right: Any) -> bool:
        if op == "=":
            return left == right
        elif op in ("!=", "<>"):
            return left != right
        elif op == "<":
            return left < right
        elif op == ">":
            return left > right
        elif op == "<=":
            return left <= right
        elif op == ">=":
            return left >= right
        else:
            raise ValueError(f"Unknown operator: {op}")
