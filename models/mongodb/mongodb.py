from typing import List

from pydantic import BaseModel
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from models.mongodb.collection import Collection
from models.mongodb.field import Field
from models.type import MongoType


class MongoDB(BaseModel):
    host: str
    port: int
    db: str
    username: str
    password: str
    collections: List[Collection]

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


    def get_candidate_key(cls, client: MongoClient, collection: Collection) -> List:
    '''
    Function to get candidate key by collection
    '''
    total_documents = 0
    valid_fields = []
    candidate_key = []
    temp_candidate_key = []
    fields = set()

    db = client[cls.db]
    coll = db[collection.name]

    total_documents = coll.count_documents({})

    for document in coll.find():
        fields.update(document.keys())

    for field in fields:

        pipeline = [
            {"$match": {field: {"$exists": True}}},
            {"$group": {
                "_id": None,
                "count": {"$sum": 1}
            }}
        ]

        result = list(coll.aggregate(pipeline))

        if result[0]['count'] == total_documents:
            valid_fields.append(field)

    for field in valid_fields:

        unique_values = coll.aggregate([
            {"$group": {
                "_id": f"${field}"}},
            {"$count": "uniqueCount"}
        ])

        unique_count = list(unique_values)[0]['uniqueCount']

        uniqueness = unique_count/total_documents

        if uniqueness == 1.0:
            candidate_key.append(field)
        else:
            temp_candidate_key.append(field)

    if len(temp_candidate_key) > 1:

        combinations = []
        for r in range(2, len(temp_candidate_key) + 1):
            combinations.extend(itertools.combinations(temp_candidate_key, r))

        for comb in combinations:
            rem_fields = list(comb)

            inside_query = {}
            for i in rem_fields:
                inside_query[i] = f'${i}'

            unique_values = coll.aggregate([
                {
                    '$group': {
                        '_id': inside_query
                    }
                }, {
                    '$count': 'uniqueCount'
                }
            ])

            result = list(unique_values)[0]['uniqueCount']
            uniqueness = result/total_documents

            if uniqueness == 1.0:
                candidate_key.append(', '.join(rem_fields))

    client.close()

    return candidate_key


    def check_key_in_other_collection(cls, client: MongoClient) -> bool:
        '''
        Check if the instance of a field found in other collection
        get one instance of a key
        check all field on the target coll if that instance found or not
        '''
        pass


    def check_key_type(cls, client: MongoClient, key: Field) -> str:
        '''
        Check data type of a field
        '''
        pass


    def check_shortest_candidate_key(cls, candidate_key: list) -> str:
        '''
        Get the shortest candidate key
        '''
        pass


    def get_primary_key(cls, client: MongoClient, collection: Collection):
        '''
        Function to get primary key by collection

        priority:
        1. candidate key found in other collection
        2. candidate key have type oid
        3. candidate key is shortest
        4. else
        '''
        pass
