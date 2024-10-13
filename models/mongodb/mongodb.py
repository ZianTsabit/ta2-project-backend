import itertools
import json
from typing import List

from pydantic import BaseModel
from pymongo import MongoClient
from pymongo_schema.extract import extract_pymongo_client_schema

from models.mongodb.collection import Collection
from models.mongodb.field import Field


class MongoDB(BaseModel):
    host: str
    port: int
    db: str
    username: str
    password: str
    collections: List[Collection] = []

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
            print(e)
            return False

        finally:
            if client:
                client.close()

    def init_collection(cls):

        client = cls.create_client()
        db = client[cls.db]
        colls = db.list_collection_names()

        for coll in colls:
            client_coll = db[coll]
            collection = Collection(
                name=coll
            )

            schema = extract_pymongo_client_schema(client, cls.db, coll)
            total_documents = schema[cls.db][coll]['count']

            for i in schema[cls.db][coll]['object'].keys():

                null = True
                if schema[cls.db][coll]['object'][i]['prop_in_object'] == 1.0:
                    null = False

                data_type = schema[cls.db][coll]['object'][i]['type']

                unique_values = client_coll.aggregate([
                    {"$group": {
                        "_id": f"${i}"}},
                    {"$count": "uniqueCount"}
                ])

                unique_count = list(unique_values)[0]['uniqueCount']

                uniqueness = unique_count / total_documents

                unique = False
                if uniqueness == 1.0:
                    unique = False

                field = Field(
                    name=i,
                    data_type=data_type,
                    null=null,
                    unique=unique
                )

                collection.fields.append(field)

            cls.collections.append(collection)

        client.close()

    def process_object(cls, coll_name, object_data, parent_key="", result=None, final_schema=None) -> dict:
        client = cls.create_client()
        db = client[cls.db]
        collection = db[coll_name]

        if result is None:
            result = []

        if final_schema is None:
            final_schema = {}

        for key, value in object_data.items():

            res = {}
            data_type = value["type"]
            total_documents = value['count']

            if data_type == "OBJECT":

                nested_result = cls.process_object(
                    coll_name=coll_name,
                    object_data=value["object"],
                    parent_key=key,
                    result=[],
                    final_schema=final_schema
                )

                final_schema[key] = nested_result

                unique = False
                unique_values = collection.aggregate([
                    {"$group": {
                        "_id": f"${key}"}},
                    {"$count": "uniqueCount"}
                ])

                unique_count = list(unique_values)[0]['uniqueCount']
                uniqueness = unique_count / total_documents

                if uniqueness == 1.0:
                    unique = True

                res["name"] = key
                res["data_type"] = "object"
                res["not_null"] = False
                res["unique"] = unique

            elif data_type == "ARRAY" and value["array_type"] == "OBJECT":

                nested_result = cls.process_object(
                    coll_name=coll_name,
                    object_data=value["object"],
                    parent_key=key,
                    result=[],
                    final_schema=final_schema
                )

                final_schema[key] = nested_result

                not_null = False
                if value['prop_in_object'] == 1.0:
                    not_null = True

                pipeline = [
                    {"$unwind": f"${key}"},
                    {"$group": {
                        "_id": "$_id",
                        f"{key}": {"$addToSet": f"${key}"},
                        "count": {"$sum": 1}
                    }},
                    {"$project": {
                        "is_unique": {"$eq": [{"$size": f"${key}"}, "$count"]},
                        f"{key}": 1
                    }}
                ]

                results_unique = list(collection.aggregate(pipeline))

                unique = True
                for i in results_unique:
                    if i['is_unique'] is False:
                        unique = False
                        break

                res["name"] = key
                res["data_type"] = "array"
                res["not_null"] = not_null
                res["unique"] = unique

            elif data_type == "ARRAY" and value["array_type"] != "OBJECT":

                pipeline = [
                    {"$unwind": f"${key}"},
                    {"$group": {
                        "_id": "$_id",
                        f"{key}": {"$addToSet": f"${key}"},
                        "count": {"$sum": 1}
                    }},
                    {"$project": {
                        "is_unique": {"$eq": [{"$size": f"${key}"}, "$count"]},
                        f"{key}": 1
                    }}
                ]

                results_unique = list(collection.aggregate(pipeline))

                unique = True
                for i in results_unique:
                    if i['is_unique'] is False:
                        unique = False
                        break

                not_null = False
                if value['prop_in_object'] == 1.0:
                    not_null = True

                res["name"] = f"{key}"
                res["data_type"] = "array"
                res["not_null"] = not_null
                res["unique"] = unique

            else:
                not_null = False
                if coll_name == parent_key:
                    if value['prop_in_object'] == 1.0:
                        not_null = True
                else:
                    pipeline = [
                        {
                            "$unwind": f"${parent_key}"
                        }, {
                            '$count': 'count'
                        }
                    ]
                    count_values = collection.aggregate(pipeline)
                    res_count = list(count_values)[0]['count']
                    if round(value['prop_in_object'], 2) == round(res_count / collection.count_documents({}), 2):
                        not_null = True

                unique = False
                if coll_name == parent_key:
                    unique_values = collection.aggregate([
                        {"$group": {
                            "_id": f"${key}"}},
                        {"$count": "uniqueCount"}
                    ])

                    unique_count = list(unique_values)[0]['uniqueCount']
                    uniqueness = unique_count / total_documents

                    if uniqueness == 1.0:
                        unique = True

                    # pipeline = [
                    #     {
                    #         "$unwind": f"${parent_key}"
                    #     }, {
                    #         "$project": {
                    #             "_id": 0,
                    #             f"{key}": f"${parent_key}.{key}"
                    #         }
                    #     }, {
                    #         "$group": {
                    #             "_id": f"${key}"
                    #         }
                    #     }, {
                    #         "$count": 'uniqueCount'
                    #     }
                    # ]

                    # unique_values = collection.aggregate(pipeline)

                    # unique_count = list(unique_values)[0]['uniqueCount']

                res["name"] = key
                res["data_type"] = data_type
                res["not_null"] = not_null
                res["unique"] = unique

            result.append(res)

        return result

    def generate_basic_schema(cls) -> dict:

        client = cls.create_client()
        db = client[cls.db]
        collections = db.list_collection_names()
        final_schema = {}

        for coll in collections:
            schema = extract_pymongo_client_schema(client, cls.db, coll)
            object_data = schema[cls.db][coll]["object"]

            processed_schema = cls.process_object(
                coll_name=coll,
                object_data=object_data,
                parent_key=coll,
                final_schema=final_schema
            )

            final_schema[coll] = processed_schema

        client.close()

        return final_schema

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

            uniqueness = unique_count / total_documents

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
                uniqueness = result / total_documents

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

    def get_primary_key(cls, client: MongoClient, collection: Collection) -> str:
        '''
        Function to get primary key by collection

        priority:
        1. candidate key found in other collection
        2. candidate key have type oid
        3. candidate key is shortest
        4. else
        '''
        pass


mongo = MongoDB(
    host='localhost',
    port=27017,
    db='db_school_2',
    username='root',
    password='rootadmin1234'
)

schema = mongo.generate_basic_schema()

with open("output.json", "w") as json_file:
    json.dump(schema, json_file, indent=4)
