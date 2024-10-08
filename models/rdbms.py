from pydantic import BaseModel


class Rdbms(BaseModel):
    host: str
    port: int
    db: str
    username: str
    password: str
