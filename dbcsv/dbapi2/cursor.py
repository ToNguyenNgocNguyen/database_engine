import json
import time
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import jwt

from dbcsv.dbapi2.exceptions import ProgrammingError
from dbcsv.dbapi2.setting import ACCESS_TOKEN_DELTA_SECONDS

if TYPE_CHECKING:
    from dbcsv.dbapi2.connection import Connection


class Cursor:
    def __init__(self, connection: "Connection"):
        self.connection = connection
        self.rowcount = -1
        self.description = None
        self._results: Optional[Iterator[bytes]] = None
        self._response = None
        self._stream_context = None
        self._closed = False

    def _ensure_open(self):
        if self._closed:
            raise ProgrammingError("Cannot operate on a closed cursor.")

    def _ensure_token(self):
        decoded: Dict[str, Any] = jwt.decode(
            self.connection.token, options={"verify_signature": False}
        )
        delta = decoded.get("exp", 0) - time.time()

        if delta < ACCESS_TOKEN_DELTA_SECONDS:
            self.connection._refresh()

    def execute(self, query: str) -> None:
        self._ensure_open()
        self._ensure_token()

        try:
            self._stream_context = self.connection.client.stream(
                method="POST",
                url=f"{self.connection.base_url}/query/sql",
                json={"sql_statement": query, "schema": self.connection.schema},
                headers={"Authorization": f"Bearer {self.connection.token}"},
            )
            self._response = self._stream_context.__enter__()  # enter manually
            self._response.raise_for_status()
            self._results = self._response.iter_bytes()
        except Exception:
            content = self._response.read()
            error_message = content.decode("utf-8", errors="replace")
            raise ProgrammingError(error_message)

    def fetchone(self) -> List[Any] | None:
        self._ensure_open()
        if self._results is None:
            raise ProgrammingError("No query executed")

        try:
            while True:
                chunk = next(self._results)
                if chunk.strip():  # skip empty chunks
                    return json.loads(chunk.decode())
        except StopIteration:
            return None

    def fetchmany(self, size: int = 1) -> List[List[Any]]:
        self._ensure_open()
        if self._results is None:
            raise ProgrammingError("No query executed")

        i = 1
        results = []
        for chunk in self._results:
            if chunk.strip():
                results.append(json.loads(chunk.decode()))

            if i >= size:
                break
            i += 1

        return results

    def fetchall(self) -> List[List[Any]]:
        self._ensure_open()
        if self._results is None:
            raise ProgrammingError("No query executed")

        results = []
        for chunk in self._results:
            if chunk.strip():
                results.append(json.loads(chunk.decode()))

        self.rowcount = len(results)
        return results

    def close(self):
        if self._stream_context:
            self._stream_context.__exit__(None, None, None)
        self._results = None
        self._response = None
        self._stream_context = None
        self._closed = True
