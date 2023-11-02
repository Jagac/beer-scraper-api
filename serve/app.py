from datetime import datetime
from http import HTTPStatus
from mangum import Mangum
from fastapi import FastAPI, Request

import sqlalchemy as db
import os

app = FastAPI(
    title="BeerAPI",
    description="Provides you with current beeradcocate's top new beers",
    version="0.0.1",
)
handler = Mangum(app)


rds_user = os.environ["rds_user"]
rds_password = os.environ["rds_password"]
rds_host = os.environ["rds_host"]


@app.get("/")
def _index(request: Request) -> dict:
    """Health check"""
    response = {
        "message": HTTPStatus.OK.phrase,
        "status-code": HTTPStatus.OK,
        "data": {},
    }
    return response


@app.get("/get-data")
def _fetch_data(request: Request):
    engine = db.create_engine(
        f"postgresql://{rds_user}:{rds_password}@{rds_host}:5432/postgres"
    )
    con = engine.connect()
    meta_data = db.MetaData()
    meta_data.reflect(bind=engine)

    final_table = meta_data.tables["final_table"]

    query = db.select(final_table)
    result = con.execute(query).fetchall()

    return [dict(final_result._mapping) for final_result in result]




