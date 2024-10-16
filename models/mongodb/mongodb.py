import itertools
import json
from typing import Dict

from pydantic import BaseModel
from pymongo import MongoClient
from pymongo_schema.extract import extract_pymongo_client_schema

from models.mongodb.collection import Collection
from models.mongodb.field import Field
from models.type import MongoType


class MongoDB(BaseModel):
    host: str
    port: int
    db: str
    username: str
    password: str
    collections: Dict = {}

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

        basic_schema = cls.generate_basic_schema()

        for coll in basic_schema.keys():
            collection = Collection(
                name=coll
            )
            for field in basic_schema[coll]:
                field = Field(
                    name=field["name"],
                    data_type=field["data_type"],
                    not_null=field["not_null"],
                    unique=field["unique"]
                )

                collection.fields.append(field)

            cls.collections[coll] = collection.fields

    def process_object(
            cls,
            coll_name,
            object_data,
            parent_key="",
            result=None,
            final_schema=None) -> dict:

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
                res["data_type"] = f"{data_type.lower()}.{value['array_type'].lower()}"
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
                res["data_type"] = f"{data_type.lower()}.{value['array_type'].lower()}"
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

                res["name"] = key
                res["data_type"] = data_type
                res["not_null"] = not_null
                res["unique"] = unique

            result.append(res)

        client.close()

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

    def get_candidate_key(cls, coll_name: str) -> dict:

        candidate_key = {}
        temp_candidate_key = {}

        client = cls.create_client()
        db = client[cls.db]

        collection = db[coll_name]
        total_documents = collection.count_documents({})

        candidate_key = []
        temp_candidate_key = []

        for j in cls.collections[coll_name]:

            if j.data_type != "array" or j.data_type != "object":
                if j.unique is True:
                    candidate_key.append(j.name)
                else:
                    temp_candidate_key.append(j.name)

        if len(temp_candidate_key) > 1:

            combinations = []
            for r in range(2, len(temp_candidate_key) + 1):
                combinations.extend(itertools.combinations(temp_candidate_key, r))

            for comb in combinations:
                rem_fields = list(comb)

                inside_query = {}
                for z in rem_fields:
                    inside_query[z] = f'${z}'

                unique_values = collection.aggregate([
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

    def check_key_in_other_collection(
            cls,
            src_key: str,
            src_coll: str) -> bool:

        client = cls.create_client()
        db = client[cls.db]

        collections = list(cls.collections.keys())
        collections.remove(src_coll)

        source_coll = db[src_coll]

        for c in collections:
            fields = cls.collections[c]
            for field in fields:
                pipeline = [
                    {
                        "$project": {
                            f"{src_key}": 1
                        }
                    }
                ]

                values = list(source_coll.aggregate(pipeline))

                for v in values:
                    target_coll = db[c]
                    find = v[src_key]
                    found = list(target_coll.find({f"{field.name}": find}))
                    if len(found) > 0:
                        return True
        client.close()
        return False

    def check_key_type(cls, src_key: str, src_coll: str) -> str:

        fields = cls.collections[src_coll]

        for f in fields:
            if f.name == src_key:
                return f.data_type

    def check_embedding_collection(cls, src_coll: str) -> bool:

        collections = list(cls.collections.keys())
        collections.remove(src_coll)

        for coll in collections:
            for field in cls.collections[coll]:
                if field.name == src_coll and (field.data_type == MongoType.OBJECT or field.data_type == MongoType.ARRAY_OF_OBJECT):
                    return {
                        "name": coll,
                        "data_type": field.data_type
                    }

        return None

    def check_shortest_candidate_key(cls, src_coll: str) -> str:

        candidate_key = cls.get_candidate_key(src_coll)

        shortest = candidate_key[0].split(",")

        for i in range(1, len(candidate_key)):
            key = candidate_key[i].split(",")
            if len(key) < len(shortest):
                shortest = key

        return shortest

    def get_primary_key(cls, coll_name: str) -> str:

        parent_coll = cls.check_embedding_collection(coll_name)

        if parent_coll is None:

            candidate_key = cls.get_candidate_key(coll_name)

            for f in candidate_key:

                if cls.check_key_in_other_collection(f, coll_name) is True:
                    return f

                elif cls.check_key_type(f, coll_name) == "oid":
                    return f

                elif cls.check_shortest_candidate_key(coll_name) == f:
                    return f

        else:

            if parent_coll["data_type"] == MongoType.OBJECT:
                return f"{parent_coll['name']}.{cls.get_primary_key(parent_coll)}"

            else:
                return None


mongo = MongoDB(
    host='localhost',
    port=27017,
    db='db_school_2',
    username='root',
    password='rootadmin1234'
)

mongo.init_collection()

print(mongo.get_primary_key('courses'))

with open("output.json", "w") as json_file:
    json.dump(mongo.dict(), json_file, indent=4)
