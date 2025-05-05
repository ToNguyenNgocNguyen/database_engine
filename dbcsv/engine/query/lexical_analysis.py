import re
from typing import List

from dbcsv.engine.exceptions import SyntaxException


class SQLLexer:
    def __init__(self):
        # SQL token patterns
        self.token_specification = [
            ("NUMBER", r"\b\d+(\.\d+)?\b"),  # Integer or decimal number
            ("STRING", r"'[^']*'"),  # Single quoted string
            ("ASTERISK", r"\*"),  # Asterisk
            ("OP", r"<=|>=|<>|!=|=|<|>"),  # Operators
            ("COMMA", r","),  # Comma
            ("SEMICOLON", r";"),  # Semicolon
            ("LPAREN", r"\("),  # Left parenthesis
            ("RPAREN", r"\)"),  # Right parenthesis
            ("DOT", r"\."),  # Dot
            ("IDENT", r"\b[a-zA-Z_][a-zA-Z0-9_]*\b"),  # Identifiers
            ("SKIP", r"\s+"),  # Skip whitespace
            ("MISMATCH", r"."),  # Any other character
        ]
        self.KEYWORDS = {
            "SELECT",
            "FROM",
            "WHERE",
            "AND",
            "OR",
            "JOIN",
            "ON",
            "AS",
            "GROUP",
            "BY",
            "ORDER",
            "HAVING",
            "LIMIT",
            "OFFSET",
        }

        # Compile the regex
        tok_regex = "|".join(
            f"(?P<{name}>{pattern})" for name, pattern in self.token_specification
        )
        self.get_token = re.compile(tok_regex).match

    def tokenize(self, sql: str) -> List[str]:
        tokens: List[str] = []
        pos = 0

        while pos < len(sql):
            match = self.get_token(sql, pos)
            if not match:
                raise SyntaxException(f"Illegal character at position {pos}")
            kind = match.lastgroup
            value = match.group()

            if kind == "SKIP":
                pass
            elif kind == "MISMATCH":
                raise SyntaxException(f"Unexpected character: {value}")
            elif kind == "IDENT" and value.upper() in self.KEYWORDS:
                tokens.append(value.upper())
            else:
                tokens.append(value)
            pos = match.end()

        return tokens


# Example usage
if __name__ == "__main__":
    sql = "SELECT * FROM employees WHERE (age > 30 AND department = 'Sales') OR (salary < 5000 AND status = 'active');"
    lexer = SQLLexer()
    print(lexer.tokenize(sql))
