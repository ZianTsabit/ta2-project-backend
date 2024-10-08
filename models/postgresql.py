from psycopg2 import connect

from models.rdbms import Rdbms


class PostgreSQL(Rdbms):
    engine: str = "postgresql"

    @classmethod
    def create_connection(cls) -> connect :

        connection = connect(
            host=cls.host,
            port=cls.port,
            database=cls.db,
            user=cls.username,
            password=cls.password
        )

        return connection

    @classmethod
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
