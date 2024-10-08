from fastapi import APIRouter, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from models.mysql import MySQL
from models.postgresql import PostgreSQL

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
