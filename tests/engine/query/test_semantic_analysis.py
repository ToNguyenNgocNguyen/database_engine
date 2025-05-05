import pytest

from dbcsv.engine.exceptions import SyntaxException
from dbcsv.engine.query.semantic_analysis import SemanticAnalyzer
from dbcsv.engine.relational.schema import Schema


class DummySchema(Schema):
    def __init__(self, tables):
        self._tables = tables

    def has_table(self, table_name):
        return table_name in self._tables

    def has_column(self, table_name, column_name):
        return column_name in self._tables.get(table_name, [])


# Setup dummy schema
dummy_schema = DummySchema({"table1": ["name", "age", "status"]})

valid_query = {
    "SELECT": ["name", "age"],
    "FROM": "table1",
    "WHERE": {
        "op": "OR",
        "left": {
            "op": "AND",
            "left": {"left": "age", "op": ">", "right": 30},
            "right": {"left": "name", "op": "=", "right": "'Sales'"},
        },
        "right": {"left": "status", "op": "=", "right": "'active'"},
    },
}


def test_valid_semantic_analysis():
    analyzer = SemanticAnalyzer(dummy_schema)
    analyzer.analyze(valid_query)  # Should not raise


def test_unknown_table():
    analyzer = SemanticAnalyzer(dummy_schema)
    query = valid_query.copy()
    query["FROM"] = "nonexistent_table"
    with pytest.raises(SyntaxException, match=r"Unknown table: nonexistent_table"):
        analyzer.analyze(query)


def test_unknown_column_in_select():
    analyzer = SemanticAnalyzer(dummy_schema)
    query = valid_query.copy()
    query["SELECT"] = ["name", "hi"]  # "hi" not in schema
    with pytest.raises(SyntaxException, match=r"Unknown column in SELECT: hi"):
        analyzer.analyze(query)


def test_unknown_column_in_where():
    analyzer = SemanticAnalyzer(dummy_schema)
    query = valid_query.copy()
    query["WHERE"]["right"]["left"] = "nonexistent_column"
    with pytest.raises(
        SyntaxException, match=r"Unknown column in WHERE clause: nonexistent_column"
    ):
        analyzer.analyze(query)
