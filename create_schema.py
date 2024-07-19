import json

from bson import ObjectId
from pymongo import MongoClient
from pymongo_schema.compare import compare_schemas_bases
from pymongo_schema.export import transform_data_to_file
from pymongo_schema.tosql import mongo_schema_to_mapping
from pymongo_schema.extract import extract_pymongo_client_schema
from pymongo_schema.filter import filter_mongo_schema_namespaces

def generate_basic_schema(host: str, port: int, username: str, password: str, db_name: str):

    client = MongoClient(host=host, port=port, username=username, password=password)

    db = client[db_name]

    collections = db.list_collection_names()

    final_schema = {}

    for coll in collections:

        schema = extract_pymongo_client_schema(client, db_name, coll)

        courses_data = schema[db_name][coll]

        object_data = courses_data["object"]

        result = {}
        for key, value in object_data.items():
            result[key] = value["type"]
        
        final_schema[coll] = result

    client.close()

    return final_schema

def check_oid_in_trgt_coll(host: str, port: int, username: str, password: str, db_name: str, trgt_coll_name:str, oid):

    client = MongoClient(host=host, port=port, username=username, password=password)
    db = client[db_name]
    coll = db[trgt_coll_name]

    foreign_key = coll.count_documents({'_id': oid}) > 0

    client.close()

    return foreign_key

def find_foreign_keys(host: str, port: int, username: str, password: str, db_name: str, final_schema: dict):

    client = MongoClient(host=host, port=port, username=username, password=password)
    db = client[db_name]

    for collection_name, attributes in final_schema.items():
        for attribute, attribute_type in attributes.items():
            if attribute_type == "oid" and attribute != "_id":
                coll = db[collection_name]
                oids = coll.distinct(attribute)
                for oid in oids:
                    found = False
                    for trgt_coll_name in final_schema:
                        if trgt_coll_name != collection_name:
                            if check_oid_in_trgt_coll(host=host, port=port, username=username, password=password, db_name=db_name, trgt_coll_name=trgt_coll_name, oid=oid):
                                # print(f"Foreign key: {collection_name}.{attribute} -> {trgt_coll_name}._id")
                                # TODO : modify the basic schema
                                found = True
                                break
                    if found:
                        break
    
    client.close()