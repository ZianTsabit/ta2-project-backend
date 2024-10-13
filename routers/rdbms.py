from fastapi import APIRouter, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from models.rdbms.mysql import MySQL
from models.rdbms.postgresql import PostgreSQL
from models.rdbms.rdbms import Rdbms

router = APIRouter(
    prefix="/api/rdbms",
    tags=["rdbms"]
)


@router.get("/")
async def root_rdbms():

    return JSONResponse(
        content={"message": "RDBMS Routers"},
        status_code=status.HTTP_200_OK
    )


@router.post("/test-connection")
async def test_connection(rdbms_type: str, rdbms: Rdbms):
    connection_status = False
    if rdbms_type == 'postgresql':
        postgresql = PostgreSQL(
            host=rdbms.host,
            port=rdbms.port,
            db=rdbms.db,
            username=rdbms.username,
            password=rdbms.password
        )
        connection_status = postgresql.test_connection()

    elif rdbms_type == 'mysql':
        mysql = MySQL(
            host=rdbms.host,
            port=rdbms.port,
            db=rdbms.db,
            username=rdbms.username,
            password=rdbms.password
        )
        connection_status = mysql.test_connection()

    if connection_status:
        return JSONResponse(
            content={
                "status": connection_status,
                "message": "connection success"
            },
            status_code=status.HTTP_200_OK
        )

    else:
        return JSONResponse(
            content={
                "status": connection_status,
                "message": "connection failed"
            },
            status_code=status.HTTP_200_OK
        )