import re
from typing import Any, Dict, List

from dbcsv.engine.exceptions import SyntaxException


class SQLParser:
    def __init__(self, tokens: List[str]):
        self.tokens = tokens
        self.pos = 0

    def current_token(self) -> str | None:
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def consume(self, expected: str | None = None) -> str | None:
        token = self.current_token()

        if expected and token != expected:
            raise SyntaxException(f"Expected '{expected}', got '{token}'")
        self.pos += 1
        return token

    def parse(self) -> Dict[str, Any]:
        query = {}
        query["SELECT"] = self.parse_select()
        query["FROM"] = self.parse_from()

        if self.current_token() == "WHERE":
            self.consume("WHERE")
            query["WHERE"] = self.parse_expression()
        if self.current_token() == ";":
            self.consume(";")
        return query

    def parse_select(self) -> List[str]:
        self.consume("SELECT")
        columns: List[str] = []

        if self.current_token() == "*":
            self.consume("*")
            columns.append("*")
        else:
            columns.append(self.parse_identifier())
            while self.current_token() == ",":
                self.consume(",")
                columns.append(self.parse_identifier())
        return columns

    def parse_from(self) -> str:
        self.consume("FROM")
        return self.parse_identifier()

    def parse_expression(self) -> Dict[str, Any]:
        left = self.parse_condition()
        while self.current_token() in ("AND", "OR"):
            op = self.consume()
            right = self.parse_condition()
            left = {"op": op, "left": left, "right": right}
        return left

    def parse_condition(self):
        if self.current_token() == "(":
            self.consume("(")
            expr = self.parse_expression()
            self.consume(")")
            return expr
        else:
            left = self.parse_operand()
            operator = self.parse_operator()
            right = self.parse_operand()
            return {"left": left, "op": operator, "right": right}

    def parse_operand(self) -> str:
        token = self.current_token()
        if re.match(r"^\d+(\.\d+)?$", token) or re.match(r"^'[^']*'$", token):
            return self.consume()  # value
        elif re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", token):
            return self.consume()  # identifier
        else:
            raise SyntaxException(f"Expected identifier or value, got '{token}'")

    def parse_operator(self) -> str | None:
        op = self.current_token()
        if op not in ("=", "<", ">", "<=", ">=", "<>", "!="):
            raise SyntaxException(f"Expected operator, got '{op}'")
        return self.consume()

    # def parse_value(self) -> str:
    #     token = self.current_token()
    #     if re.match(r"^\d+(\.\d+)?$", token) or re.match(r"^'[^']*'$", token):
    #         return self.consume()
    #     else:
    #         raise SyntaxException(f"Expected value, got '{token}'")

    def parse_identifier(self) -> str:
        token = self.current_token()
        if re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", token):
            return self.consume()
        else:
            raise SyntaxException(f"Expected identifier, got '{token}'")


if __name__ == "__main__":
    from dbcsv.engine.query.lexical_analysis import SQLLexer

    sql = "SELECT  FROM employees WHERE (age > 30 AND department = 'Sales') OR status = 'active';"
    lexer = SQLLexer()
    tokens = lexer.tokenize(sql)

    parser = SQLParser(tokens)
    parsed = parser.parse()
    print(parsed)
