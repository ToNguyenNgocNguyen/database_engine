import pytest

from dbcsv.engine.exceptions import SyntaxException
from dbcsv.engine.query.lexical_analysis import SQLLexer

lexer = SQLLexer()


def test_basic_select():
    sql = "SELECT * FROM users;"
    expected = ["SELECT", "*", "FROM", "users", ";"]
    assert lexer.tokenize(sql) == expected


def test_where_clause():
    sql = "SELECT name FROM users WHERE age > 30;"
    expected = ["SELECT", "name", "FROM", "users", "WHERE", "age", ">", "30", ";"]
    assert lexer.tokenize(sql) == expected


def test_complex_condition():
    sql = "SELECT * FROM employees WHERE (age > 30 AND department = 'Sales') OR (salary < 5000 AND status = 'active');"
    expected = [
        "SELECT",
        "*",
        "FROM",
        "employees",
        "WHERE",
        "(",
        "age",
        ">",
        "30",
        "AND",
        "department",
        "=",
        "'Sales'",
        ")",
        "OR",
        "(",
        "salary",
        "<",
        "5000",
        "AND",
        "status",
        "=",
        "'active'",
        ")",
        ";",
    ]
    assert lexer.tokenize(sql) == expected


def test_group_by_order_by():
    sql = "SELECT department, COUNT(*) FROM employees GROUP BY department ORDER BY COUNT(*) DESC;"
    expected = [
        "SELECT",
        "department",
        ",",
        "COUNT",
        "(",
        "*",
        ")",
        "FROM",
        "employees",
        "GROUP",
        "BY",
        "department",
        "ORDER",
        "BY",
        "COUNT",
        "(",
        "*",
        ")",
        "DESC",
        ";",
    ]
    tokens = lexer.tokenize(sql)
    assert tokens == expected[:-2] + [
        "DESC",
        ";",
    ]  # "DESC" is not in KEYWORDS, so treated as IDENT


def test_invalid_character():
    sql = "SELECT * FROM users WHERE name = @username;"
    with pytest.raises(SyntaxException, match=r"Unexpected character: @"):
        lexer.tokenize(sql)


def test_string_with_spaces():
    sql = "SELECT * FROM logs WHERE message = 'Disk full error';"
    expected = [
        "SELECT",
        "*",
        "FROM",
        "logs",
        "WHERE",
        "message",
        "=",
        "'Disk full error'",
        ";",
    ]
    assert lexer.tokenize(sql) == expected


def test_dot_and_identifiers():
    sql = "SELECT u.name FROM users u;"
    expected = ["SELECT", "u", ".", "name", "FROM", "users", "u", ";"]
    assert lexer.tokenize(sql) == expected


def test_where_and_or_uppercase():
    sql = "SELECT * FROM users WHERE age > 30 AND status = 'active' OR department = 'Sales';"
    tokens = lexer.tokenize(sql)
    assert "WHERE" in tokens
    assert "AND" in tokens
    assert "OR" in tokens


def test_where_and_or_lowercase():
    sql = "select * from users where age > 30 and status = 'active' or department = 'Sales';"
    tokens = lexer.tokenize(sql)
    # Keywords should be converted to uppercase
    assert "WHERE" in tokens
    assert "AND" in tokens
    assert "OR" in tokens
    assert "where" not in tokens
    assert "and" not in tokens
    assert "or" not in tokens


def test_where_and_or_mixed_case():
    sql = "SeLeCt * FrOm users WhErE age > 30 aNd status = 'active' oR department = 'Sales';"
    tokens = lexer.tokenize(sql)
    assert "WHERE" in tokens
    assert "AND" in tokens
    assert "OR" in tokens


def test_identifiers_similar_to_keywords():
    sql = "SELECT andrew, organizer FROM staff WHERE name = 'Orson';"
    tokens = lexer.tokenize(sql)
    # These should be treated as identifiers, not keywords
    assert "andrew" in tokens
    assert "organizer" in tokens
    assert "AND" not in tokens  # Not as a keyword in this case


def test_numeric_literals():
    sql = "SELECT * FROM data WHERE value = 123 AND score = 45.67;"
    expected = [
        "SELECT",
        "*",
        "FROM",
        "data",
        "WHERE",
        "value",
        "=",
        "123",
        "AND",
        "score",
        "=",
        "45.67",
        ";",
    ]
    assert lexer.tokenize(sql) == expected


def test_constant_condition_in_where():
    sql = "SELECT * FROM table2 WHERE 1 = 1 AND age > 20;"
    expected = [
        "SELECT",
        "*",
        "FROM",
        "table2",
        "WHERE",
        "1",
        "=",
        "1",
        "AND",
        "age",
        ">",
        "20",
        ";",
    ]
    assert lexer.tokenize(sql) == expected


def test_not_equals_operator():
    sql = "SELECT * FROM users WHERE status != 'inactive';"
    expected = [
        "SELECT",
        "*",
        "FROM",
        "users",
        "WHERE",
        "status",
        "!=",
        "'inactive'",
        ";",
    ]
    assert lexer.tokenize(sql) == expected


def test_unclosed_string_literal():
    sql = "SELECT * FROM logs WHERE message = 'Missing end;"
    with pytest.raises(SyntaxException, match=r"Unexpected character: '"):
        lexer.tokenize(sql)


def test_multiline_query():
    sql = """
    SELECT
        name,
        age
    FROM
        users
    WHERE
        age >= 18;
    """
    expected = [
        "SELECT",
        "name",
        ",",
        "age",
        "FROM",
        "users",
        "WHERE",
        "age",
        ">=",
        "18",
        ";",
    ]
    assert lexer.tokenize(sql) == expected


def test_operators_spacing():
    sql = "SELECT * FROM accounts WHERE balance>=1000 AND balance<=5000;"
    expected = [
        "SELECT",
        "*",
        "FROM",
        "accounts",
        "WHERE",
        "balance",
        ">=",
        "1000",
        "AND",
        "balance",
        "<=",
        "5000",
        ";",
    ]
    assert lexer.tokenize(sql) == expected
