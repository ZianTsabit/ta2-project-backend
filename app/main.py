from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from routers import mongodb, rdbms

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(mongodb.router)
app.include_router(rdbms.router)


@app.get("/")
def root():
    return JSONResponse(
        content={"message": "GooseSeqlify API"}, status_code=status.HTTP_200_OK
    )
