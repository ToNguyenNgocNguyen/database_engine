from dbcsv.engine.relational.schema import Schema


def get_schema(schema_name: str) -> Schema:
    return Schema.load(schema_name)
