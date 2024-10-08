from pydantic import BaseModel
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError


class MongoDB(BaseModel):
    host: str
    port: int
    db: str
    username: str
    password: str

    def create_client(cls) -> MongoClient:

        client = MongoClient(
            host=cls.host,
            port=cls.port,
            username=cls.username,
            password=cls.password,
            serverSelectionTimeoutMS=5000
        )

        return client


    def test_connection(cls) -> bool:
        client = None
        try:
            client = cls.create_client()
            client.server_info()
            return True

        except Exception as e:
            return False
        
        finally:
            if client:
                client.close()
