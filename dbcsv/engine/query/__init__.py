from collections.abc import Generator

from dbcsv.engine.query.executor import SelectExecutor
from dbcsv.engine.query.lexical_analysis import SQLLexer
from dbcsv.engine.query.semantic_analysis import SemanticAnalyzer
from dbcsv.engine.query.syntactic_analysis import SQLParser
from dbcsv.engine.relational import get_schema


def run_query(sql: str, schema_name: str) -> Generator[str, None, None]:
    schema = get_schema(schema_name)

    # Lexical analysis
    lexer = SQLLexer()
    tokens = lexer.tokenize(sql)

    # Syntax parsing
    parser = SQLParser(tokens)
    parsed = parser.parse()

    # Semantic analysis
    analyzer = SemanticAnalyzer(schema)
    analyzer.analyze(parsed)

    # Execution
    executor = SelectExecutor(schema)
    return executor.execute(parsed)


if __name__ == "__main__":
    sql = "SELECT * FROM table1 WHERE (age > 30 AND age < 32) OR name = 'Michael Brown'"
    for row in run_query(sql=sql, schema_name="schema1"):
        print(row)
