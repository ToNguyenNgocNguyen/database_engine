from fastapi import Depends

from dbcsv.engine.security.auth import auth_manager

current_user_dependency = Depends(auth_manager.get_current_user)
