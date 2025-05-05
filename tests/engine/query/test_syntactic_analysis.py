import pytest

from dbcsv.engine.exceptions import SyntaxException
from dbcsv.engine.query.lexical_analysis import SQLLexer
from dbcsv.engine.query.syntactic_analysis import SQLParser


# Helper function to parse SQL with lexer and parser
def parse_sql(sql: str):
    lexer = SQLLexer()
    tokens = lexer.tokenize(sql)
    parser = SQLParser(tokens)
    return parser.parse()


def test_select_star():
    sql = "SELECT * FROM employees;"
    expected = {"SELECT": ["*"], "FROM": "employees"}
    parsed_query = parse_sql(sql)
    assert parsed_query == expected


def test_select_columns():
    sql = "SELECT name, age FROM employees;"
    expected = {"SELECT": ["name", "age"], "FROM": "employees"}
    parsed_query = parse_sql(sql)
    assert parsed_query == expected


def test_select_with_where():
    sql = "SELECT name, age FROM employees WHERE age > 30;"
    expected = {
        "SELECT": ["name", "age"],
        "FROM": "employees",
        "WHERE": {"left": "age", "op": ">", "right": "30"},
    }
    parsed_query = parse_sql(sql)
    assert parsed_query == expected


def test_where_and_or():
    sql = "SELECT name FROM employees WHERE age > 30 AND department = 'Sales';"
    expected = {
        "SELECT": ["name"],
        "FROM": "employees",
        "WHERE": {
            "op": "AND",
            "left": {"left": "age", "op": ">", "right": "30"},
            "right": {"left": "department", "op": "=", "right": "'Sales'"},
        },
    }
    parsed_query = parse_sql(sql)
    assert parsed_query == expected


def test_invalid_select():
    sql = "SELECT FROM employees;"
    with pytest.raises(SyntaxException, match=r"Expected 'FROM', got 'employees'"):
        parse_sql(sql)


def test_missing_from():
    sql = "SELECT name, age WHERE age > 30;"
    with pytest.raises(SyntaxException, match=r"Expected 'FROM', got 'WHERE'"):
        parse_sql(sql)


def test_invalid_operator():
    sql = "SELECT name FROM employees WHERE age ~ 30;"
    with pytest.raises(SyntaxException, match=r"Unexpected character: ~"):
        parse_sql(sql)
