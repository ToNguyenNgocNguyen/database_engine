from urllib.parse import urlparse

from dbcsv.dbapi2.exceptions import OperationalError


def get_base_url_and_schema(dsn: str):
    parsed = urlparse(dsn)
    base_url = f"{parsed.scheme}://{parsed.hostname}"
    if parsed.port:
        base_url += f":{parsed.port}"
    schema = parsed.path.lstrip("/")

    if not base_url or not schema:
        raise OperationalError("dsn wrong format")
    return base_url, schema
