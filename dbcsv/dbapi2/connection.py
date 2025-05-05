from typing import NoReturn

from httpx import Client

from dbcsv.dbapi2.cursor import Cursor
from dbcsv.dbapi2.exceptions import AuthenticationError, NotSupportedError


class Connection:
    _base_url: str
    _token: str
    _schema: str
    _client: Client

    def __init__(self, base_url: str, token: str, schema: str, client: Client):
        self._base_url = base_url
        self._token = token
        self._client = client
        self._schema = schema

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def token(self) -> str:
        return self._token

    @property
    def schema(self) -> str:
        return self._schema

    @property
    def client(self) -> Client:
        return self._client

    @classmethod
    def connect(
        cls, base_url: str, user: str, password: str, schema: str
    ) -> "Connection":
        client = Client()
        try:
            response = client.post(
                f"{base_url}/auth/connect",
                data={"username": user, "password": password},
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
            response.raise_for_status()
            token = response.json()["access_token"]
            return cls(base_url, token, schema, client)
        except Exception:
            client.close()
            raise AuthenticationError(response.json().get("detail"))

    def cursor(self) -> "Cursor":
        return Cursor(self)

    def close(self) -> None:
        self._client.close()

    def _refresh(self) -> None:
        try:
            response = self._client.post(
                f"{self._base_url}/auth/refresh",
                headers={"Authorization": f"Bearer {self._token}"},
            )
            response.raise_for_status()
            self._token = response.json()["access_token"]
        except Exception:
            self._client.close()
            raise AuthenticationError(response.json().get("detail"))

    def commit(self) -> NoReturn:
        raise NotSupportedError("commit() not supported")

    def rollback(self) -> NoReturn:
        raise NotSupportedError("rollback() not supported")
