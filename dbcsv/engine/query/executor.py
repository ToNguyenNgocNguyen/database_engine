import json
import re
from typing import Any, Dict, Generator, List

from dbcsv.engine.relational.datatype import DBTypeObject
from dbcsv.engine.relational.schema import Schema


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
            if where_clause is None or self.evaluate_condition(where_clause, row):
                yield self.format_row(row, select_columns)

    def format_row(self, row: Dict[str, Any], select_columns: List[str]) -> str:
        if select_columns == ["*"]:
            return json.dumps(list(row.values()))
        else:
            projected = [row[col] for col in select_columns if col in row]
            return json.dumps(projected)

    def evaluate_condition(self, expr: Dict[str, Any], row: Dict[str, Any]) -> bool:
        if expr["op"] in ("AND", "OR"):
            left = self.evaluate_condition(expr["left"], row)
            right = self.evaluate_condition(expr["right"], row)
            return left and right if expr["op"] == "AND" else left or right

        # Leaf condition: {left, op, right}
        left_val = self.resolve_operand(expr["left"], row)
        right_val = self.resolve_operand(expr["right"], row)

        try:
            left_val = DBTypeObject.convert_datatype(left_val, self._column_type)
        except Exception:
            pass

        try:
            right_val = DBTypeObject.convert_datatype(right_val, self._column_type)
        except Exception:
            pass

        return self.compare(left_val, expr["op"], right_val)

    def resolve_operand(self, operand: str, row: Dict[str, Any]) -> Any:
        # Operand may be a literal or a column
        if self.is_string_literal(operand):
            return operand[1:-1]  # strip quotes
        elif self.is_numeric_literal(operand):
            return self.cast_number(operand)
        elif operand in row:
            self._column_type = self.columns[operand]
            return row[operand]
        else:
            # If not a column and not a literal, treat as string (e.g., bareword constant like TRUE)
            return operand

    def is_string_literal(self, value: str) -> bool:
        return value.startswith("'") and value.endswith("'")

    def is_numeric_literal(self, value: str) -> bool:
        return re.match(r"^-?\d+(\.\d+)?$", value) is not None

    def cast_number(self, value: str) -> Any:
        try:
            return int(value)
        except ValueError:
            return float(value)

    def compare(self, left: Any, op: str, right: Any) -> bool:
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


if __name__ == "__main__":
    from dbcsv.engine.query.lexical_analysis import SQLLexer
    from dbcsv.engine.query.semantic_analysis import SemanticAnalyzer
    from dbcsv.engine.query.syntactic_analysis import SQLParser
    from dbcsv.engine.relational import get_schema

    sql = "SELECT * FROM table2 WHERE age > 1 AND 1 = 1"

    # Lex + Parse
    lexer = SQLLexer()
    tokens = lexer.tokenize(sql)
    parser = SQLParser(tokens)
    parsed = parser.parse()

    # Schema + Semantic analysis
    schema = get_schema("schema1")
    analyzer = SemanticAnalyzer(schema)
    analyzer.analyze(parsed)

    # Execute
    executor = SelectExecutor(schema)
    for row in executor.execute(parsed):
        print(row)
