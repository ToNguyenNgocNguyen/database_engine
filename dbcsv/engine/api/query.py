from typing import Annotated

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from dbcsv.engine.dependencies import current_user_dependency
from dbcsv.engine.query import run_query
from dbcsv.engine.schemas.auth import User
from dbcsv.engine.schemas.sql_request import SQLRequest

router = APIRouter(prefix="/query", tags=["Query"])


@router.post("/sql")
async def query_by_sql(
    sql_request: SQLRequest, current_user: Annotated[User, current_user_dependency]
) -> StreamingResponse:
    results = run_query(sql=sql_request.sql_statement, schema_name=sql_request.schema)
    return StreamingResponse(results, media_type="text/event-stream")
