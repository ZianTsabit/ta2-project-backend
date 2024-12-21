from fastapi import APIRouter, Response, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from mongosequelizer.mongodb.mongodb import MongoDB

router = APIRouter(prefix="/api/mongodb", tags=["mongodb"])


@router.get("/")
async def root_mongodb():

    return JSONResponse(
        content={"message": "MongoDB Routers"}, status_code=status.HTTP_200_OK
    )


@router.post("/test-connection")
async def test_connection(mongodb: MongoDB):

    connection_status = mongodb.test_connection()

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
async def display_schema(mongodb: MongoDB):

    schema = mongodb.display_schema()

    if schema:
        return JSONResponse(
            content=jsonable_encoder(schema), status_code=status.HTTP_200_OK
        )

    else:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
