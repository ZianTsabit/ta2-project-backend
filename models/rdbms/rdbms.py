from psycopg2 import connect
from pydantic import BaseModel


class Rdbms(BaseModel):
    host: str
    port: int
    db: str
    username: str
    password: str


    def create_connection(cls) -> connect:

        connection = connect(
            host=cls.host,
            port=cls.port,
            database=cls.db,
            user=cls.username,
            password=cls.password
        )

        return connection


    def test_connection(cls) -> bool:
        connection = None
        try:
            connection = cls.create_connection()
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            connection.close()
            return True

        except OperationalError as e:
            return False
