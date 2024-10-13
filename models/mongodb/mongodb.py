from typing import List

from bson import ObjectId
from pydantic import BaseModel
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
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

    def process_object(cls, object_data, parent_key="", result=None, final_schema=None) -> dict:
        if result is None:
            result = {}
        if final_schema is None:
            final_schema = {}

        for key, value in object_data.items():
            data_type = value["type"]
            if data_type == "OBJECT":
                nested_result = process_object(
                    value["object"],
                    key,
                    {},
                    final_schema
                )

                if parent_key:
                    nested_result[f"{parent_key}_id"] = "oid"
                final_schema[key] = nested_result
                final_schema[key]["foreign_keys"] = {}
                final_schema[key]["foreign_keys"][
                    f"{parent_key}_id"
                ] = f"{parent_key}._id"
            elif data_type == "ARRAY" and value["array_type"] == "OBJECT":
                nested_result = process_object(
                    value["object"],
                    key,
                    {},
                    final_schema
                )
                if parent_key:
                    nested_result[f"{parent_key}_id"] = "oid"
                final_schema[key] = nested_result
                final_schema[key]["foreign_keys"] = {}
                final_schema[key]["foreign_keys"][
                    f"{parent_key}_id"
                ] = f"{parent_key}._id"
            elif data_type == "ARRAY" and value["array_type"] == "oid":
                result[f"{key}"] = value["array_type"]
            else:
                result[key] = data_type

        return result


    def generate_basic_schema(cls, client: MongoClient) -> dict:

        db = client[db_name]
        collections = db.list_collection_names()
        final_schema = {}

        for coll in collections:
            schema = extract_pymongo_client_schema(client, db_name, coll)
            courses_data = schema[db_name][coll]
            object_data = courses_data["object"]
            processed_schema = process_object(
                object_data,
                coll,
                final_schema=final_schema
            )
            final_schema[coll] = processed_schema

        client.close()
        return final_schema


    def check_oid_in_trgt_coll(cls, client: MongoClient, trgt_coll: Relation, oid: ObjectId):

        db = client[db_name]
        coll = db[trgt_coll.name]

        foreign_key = coll.count_documents({'_id': oid}) > 0

        client.close()

        return foreign_key


    def find_foreign_keys(cls, client: MongoClient, basic_schema: dict):

        db = client[db_name]

        final_schema = {}

        for collection_name, attributes in basic_schema.items():
            if 'foreign_keys' not in attributes:
                attributes['foreign_keys'] = {}

            for attribute, attribute_type in attributes.items():

                if attribute_type == "oid" and attribute != "_id":
                    coll = db[collection_name]
                    oids = coll.distinct(attribute)
                    for oid in oids:
                        found = False
                        for trgt_coll_name in basic_schema:
                            if trgt_coll_name != collection_name:
                                if check_oid_in_trgt_coll(
                                        host=host,
                                        port=port,
                                        username=username,
                                        password=password,
                                        db_name=db_name,
                                        trgt_coll_name=trgt_coll_name,
                                        oid=oid):

                                    if collection_name not in final_schema:
                                        final_schema[
                                            collection_name] = {'foreign_keys': {}}

                                    final_schema[collection_name][
                                        'foreign_keys'][
                                        attribute] = f"{trgt_coll_name}._id"

                                    found = True
                                    break

                        if found:
                            break

        for collection_name, update_info in final_schema.items():
            if collection_name in basic_schema:
                basic_schema[collection_name].update(update_info)
            else:
                basic_schema[collection_name] = update_info

        client.close()

        return basic_schema


    def generate_final_schema(tables: dict) -> dict:

        cleaned_tables = {
            table: dict(attributes)
            for table, attributes in tables.items()
        }
        processed_pairs = set()
        new_tables = {}

        for table1, attributes1 in tables.items():
            for key1, reference1 in attributes1.get("foreign_keys", {}).items():
                ref1 = reference1.split(".")[0]
                if ref1 in tables:
                    table2 = ref1
                    attributes2 = tables[table2]

                    pair = tuple(sorted([table1, table2]))
                    if pair not in processed_pairs:
                        foreign_keys = attributes2.get("foreign_keys", {})
                        for key2, reference2 in foreign_keys.items():
                            ref2 = reference2.split(".")[0]
                            if ref2 == table1:
                                relationship_table_name = f"{pair[0]}_{pair[1]}"
                                new_tables[relationship_table_name] = {
                                    f"{pair[0]}_id": "oid",
                                    f"{pair[1]}_id": "oid",
                                    "foreign_keys": {
                                        f"{pair[0]}_id": f"{pair[0]}._id",
                                        f"{pair[1]}_id": f"{pair[1]}._id"
                                    }
                                }
                                processed_pairs.add(pair)

        for pair in processed_pairs:
            table1, table2 = pair
            if ("foreign_keys" in cleaned_tables[table1] and
                    table2 in cleaned_tables[table1]["foreign_keys"]):
                del cleaned_tables[table1]["foreign_keys"][table2]
                del cleaned_tables[table1][table2]
            if ("foreign_keys" in cleaned_tables[table2] and
                    table1 in cleaned_tables[table2]["foreign_keys"]):
                del cleaned_tables[table2]["foreign_keys"][table1]
                del cleaned_tables[table2][table1]

        cleaned_tables.update(new_tables)

        return cleaned_tables

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
    

    
