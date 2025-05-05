from dbcsv.dbapi2.connection import Connection
from dbcsv.dbapi2.utils import get_base_url_and_schema


def connect(
    dsn: str,
    user: str,
    password: str,
) -> Connection:
    base_url, schema = get_base_url_and_schema(dsn)
    return Connection.connect(base_url, user, password, schema)
