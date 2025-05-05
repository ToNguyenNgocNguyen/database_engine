import re
import sys
from typing import Any, Dict, List

from dbcsv.engine.exceptions import SyntaxException

# Precompiled regex patterns for performance
RE_NUMBER = re.compile(r"^\d+(\.\d+)?$")
RE_STRING = re.compile(r"^'[^']*'$")
RE_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
VALID_OPERATORS = frozenset({"=", "<", ">", "<=", ">=", "<>", "!="})


class SQLParser:
    def __init__(self, tokens: List[str]):
        # Use string interning for identifiers
        self.tokens = [sys.intern(t) if t.isidentifier() else t for t in tokens]
        self.pos = 0

    def current_token(self) -> str | None:
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return None

    def consume(self, expected: str | None = None) -> str:
        token = self.current_token()
        if token is None:
            raise SyntaxException("Unexpected end of input")

        if expected and token != expected:
            raise SyntaxException(f"Expected '{expected}', got '{token}'")

        self.pos += 1
        return token

    def parse(self) -> Dict[str, Any]:
        query = {"SELECT": self.parse_select(), "FROM": self.parse_from()}

        token = self.current_token()
        if token == "WHERE":
            self.consume("WHERE")
            query["WHERE"] = self.parse_expression()

        if self.current_token() == ";":
            self.consume(";")

        return query

    def parse_select(self) -> List[str]:
        self.consume("SELECT")
        token = self.current_token()

        if token == "*":
            self.consume("*")
            return ["*"]

        columns = [self.parse_identifier()]
        while self.current_token() == ",":
            self.consume(",")
            columns.append(self.parse_identifier())

        return columns

    def parse_from(self) -> str:
        self.consume("FROM")
        return self.parse_identifier()

    def parse_expression(self) -> Dict[str, Any]:
        expr = self.parse_condition()
        while True:
            token = self.current_token()
            if token not in ("AND", "OR"):
                break
            op = self.consume()
            right = self.parse_condition()
            expr = {"op": op, "left": expr, "right": right}
        return expr

    def parse_condition(self) -> Dict[str, Any]:
        if self.current_token() == "(":
            self.consume("(")
            expr = self.parse_expression()
            self.consume(")")
            return expr

        left = self.parse_operand()
        operator = self.parse_operator()
        right = self.parse_operand()
        return {"left": left, "op": operator, "right": right}

    def parse_operand(self) -> str:
        token = self.current_token()
        if token is None:
            raise SyntaxException("Unexpected end of input")

        # Cache match results for better performance
        is_num = RE_NUMBER.match(token)
        is_str = RE_STRING.match(token)
        is_id = RE_IDENTIFIER.match(token)

        if is_num or is_str or is_id:
            return self.consume()

        raise SyntaxException(f"Expected identifier or value, got '{token}'")

    def parse_operator(self) -> str:
        token = self.current_token()
        if token not in VALID_OPERATORS:
            raise SyntaxException(f"Expected operator, got '{token}'")
        return self.consume()

    def parse_identifier(self) -> str:
        token = self.current_token()
        if token and RE_IDENTIFIER.match(token):
            return self.consume()
        raise SyntaxException(f"Expected identifier, got '{token}'")
