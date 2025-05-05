from fastapi import HTTPException


class SyntaxException(HTTPException):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(
            status_code=422,
            detail=message,
        )


class DatabaseException(HTTPException):
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(
            status_code=422,
            detail=message,
        )
