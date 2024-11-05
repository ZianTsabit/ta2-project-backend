from fastapi import APIRouter, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from models.mongodb.mongodb import MongoDB
from models.mysql.mysql import MySQL
from models.postgresql.postgresql import PostgreSQL
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


@router.post("/display-schema")
async def display_schema(rdbms_type: str, rdbms: Rdbms, mongodb: MongoDB):
    ddl = ""
    mongodb.init_collection()
    collections = mongodb.get_collections()
    cardinalities = mongodb.mapping_all_cardinalities()

    if rdbms_type == 'postgresql':
        postgresql = PostgreSQL(
            host=rdbms.host,
            port=rdbms.port,
            db=rdbms.db,
            username=rdbms.username,
            password=rdbms.password
        )
        postgresql.process_mapping_cardinalities(mongodb, collections, cardinalities)
        postgresql.process_collection(mongodb, collections)

        schema = {k: v.to_dict() for k, v in postgresql.relations.items()}

        ddl = postgresql.generate_ddl(schema)

    elif rdbms_type == 'mysql':
        mysql = MySQL(
            host=rdbms.host,
            port=rdbms.port,
            db=rdbms.db,
            username=rdbms.username,
            password=rdbms.password
        )
        mysql.process_mapping_cardinalities(mongodb, collections, cardinalities)
        mysql.process_collection(mongodb, collections)

        schema = {k: v.to_dict() for k, v in mysql.relations.items()}

        ddl = mysql.generate_ddl(schema)

    if ddl != "":
        return JSONResponse(
            content=jsonable_encoder(ddl),
            status_code=status.HTTP_200_OK
        )

    else:
        return Response(
            status_code=status.HTTP_204_NO_CONTENT
        )
