from typing import Annotated

from fastapi import APIRouter, Depends
from fastapi.security import OAuth2PasswordRequestForm

from dbcsv.engine.dependencies import current_user_dependency
from dbcsv.engine.schemas.auth import Token, User
from dbcsv.engine.security.auth import auth_manager

router = APIRouter(prefix="/auth", tags=["Authentication"])


@router.post("/connect")
async def connection(
    form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
) -> Token:
    return auth_manager.login_for_access_token(form_data.username, form_data.password)


@router.post("/refresh")
async def refresh_token(
    current_user: Annotated[User, current_user_dependency],
) -> Token:
    return auth_manager.refresh_for_access_token(current_user)
