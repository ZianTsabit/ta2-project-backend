from fastapi import APIRouter, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from mongosequelizer.mongodb.mongodb import MongoDB
from mongosequelizer.mysql.mysql import MySQL
from mongosequelizer.postgresql.postgresql import PostgreSQL
from mongosequelizer.rdbms.rdbms import Rdbms
from mongosequelizer.transformator import MongoSequelizer

router = APIRouter(prefix="/api/rdbms", tags=["rdbms"])


@router.get("/")
async def root_rdbms():

    return JSONResponse(
        content={"message": "RDBMS Routers"}, status_code=status.HTTP_200_OK
    )


@router.post("/test-connection")
async def test_connection(rdbms_type: str, rdbms: Rdbms):
    connection_status = False
    if rdbms_type == "postgresql":
        postgresql = PostgreSQL(
            host=rdbms.host,
            port=rdbms.port,
            db=rdbms.db,
            username=rdbms.username,
            password=rdbms.password,
        )
        connection_status = postgresql.test_connection()

    elif rdbms_type == "mysql":
        mysql = MySQL(
            host=rdbms.host,
            port=rdbms.port,
            db=rdbms.db,
            username=rdbms.username,
            password=rdbms.password,
        )
        connection_status = mysql.test_connection()

    if connection_status:
        return JSONResponse(
            content={"status": connection_status, "message": "connection success"},
            status_code=status.HTTP_200_OK,
        )

    else:
        return JSONResponse(
            content={"status": connection_status, "message": "connection failed"},
            status_code=status.HTTP_200_OK,
        )


@router.post("/display-schema")
async def display_schema(rdbms_type: str, rdbms: Rdbms, mongodb: MongoDB):
    ddl = ""
    ddl = MongoSequelizer(rdbms_type, rdbms, mongodb).generate_ddl()
    if ddl != "":
        return JSONResponse(
            content=jsonable_encoder(ddl), status_code=status.HTTP_201_CREATED
        )
    else:
        return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/implement-schema")
async def implement_schema(rdbms_type: str, rdbms: Rdbms, mongodb: MongoDB):

    success = False
    success = MongoSequelizer(rdbms_type, rdbms, mongodb).implement_ddl()

    if success is True:
        return JSONResponse(
            content=jsonable_encoder(success), status_code=status.HTTP_201_CREATED
        )
    else:
        return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/migrate-data")
async def migrate_data(rdbms_type: str, rdbms: Rdbms, mongodb: MongoDB):
    success = False
    success = MongoSequelizer(rdbms_type, rdbms, mongodb).migrate_data()

    if success is True:
        return JSONResponse(
            content=jsonable_encoder(success), status_code=status.HTTP_201_CREATED
        )
    else:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
