from fastapi import FastAPI

from dbcsv.engine.api import query, security

app = FastAPI()


@app.get("/")
def root():
    return "This is for database engine!"


app.include_router(router=query.router)
app.include_router(router=security.router)
