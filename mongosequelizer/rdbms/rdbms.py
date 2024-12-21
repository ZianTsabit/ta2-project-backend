from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError


class Rdbms(BaseModel):
    host: str
    port: int
    db: str
    username: str
    password: str

    def create_engine_url(cls) -> str:
        raise NotImplementedError("Subclasses should implement this method")

    def create_connection(cls):
        engine = create_engine(cls.create_engine_url())
        return engine

    def test_connection(cls) -> bool:
        try:
            engine = cls.create_connection()
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            return True
        except OperationalError as e:
            print(e)
            return False

    def execute_query(cls, query: str) -> bool:
        try:
            engine = cls.create_connection()
            with engine.connect() as connection:
                with connection.begin():
                    queries = [q.strip() for q in query.split(";") if q.strip()]
                    for q in queries:
                        print(f"Executing: {q};")
                        connection.execute(text(f"{q}"))
            return True
        except OperationalError as e:
            print(e)
            return False
