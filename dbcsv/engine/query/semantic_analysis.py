import re

from dbcsv.engine.exceptions import SyntaxException
from dbcsv.engine.relational.schema import Schema


class SemanticAnalyzer:
    _schema: Schema

    def __init__(self, schema: Schema) -> None:
        self._schema = schema

    @property
    def schema(self) -> Schema:
        return self._schema

    def analyze(self, query: dict):
        table = query["FROM"]

        if not self.schema.has_table(table):
            raise SyntaxException(f"Unknown table: {table}")

        # Check SELECT columns
        for col in query["SELECT"]:
            if col != "*" and not self.schema.has_column(table, col):
                raise SyntaxException(f"Unknown column in SELECT: {col}")

        # Check WHERE clause
        if "WHERE" in query:
            self._check_expression(query["WHERE"], table)

    def _is_literal(self, operand: str) -> bool:
        return bool(
            re.match(r"^\d+(\.\d+)?$", operand)  # Numeric literals
            or re.match(r"^'[^']*'$", operand)  # String literals
            or operand.upper() in ("TRUE", "FALSE")  # Boolean literals
        )

    def _check_expression(self, expr, table):
        if "op" in expr and expr["op"] in ("AND", "OR"):
            self._check_expression(expr["left"], table)
            self._check_expression(expr["right"], table)
        else:
            left = expr["left"]
            if not self._is_literal(left) and not self.schema.has_column(table, left):
                raise SyntaxException(f"Unknown column in WHERE clause: {left}")


if __name__ == "__main__":
    from dbcsv.engine.query.lexical_analysis import SQLLexer
    from dbcsv.engine.query.syntactic_analysis import SQLParser
    from dbcsv.engine.relational import get_schema

    sql = "SELECT name, hi FROM table1 WHERE (age > 30 AND name = 'Sales') OR status = 'active';"

    lexer = SQLLexer()
    tokens = lexer.tokenize(sql)

    parser = SQLParser(tokens)
    parsed_query = parser.parse()

    # Define schema
    database = get_schema("schema1")

    # Run semantic analysis
    analyzer = SemanticAnalyzer(database)
    analyzer.analyze(parsed_query)

    print("Semantic analysis passed.")
