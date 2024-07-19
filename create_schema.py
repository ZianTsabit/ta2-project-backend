import json

from bson import ObjectId
from pymongo import MongoClient
from pymongo_schema.extract import extract_pymongo_client_schema
from pymongo_schema.compare import compare_schemas_bases
from pymongo_schema.export import transform_data_to_file
from pymongo_schema.filter import filter_mongo_schema_namespaces
from pymongo_schema.tosql import mongo_schema_to_mapping

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

    # with open(f"./basic_schema/final_schema.json", "w") as file:
    #         json.dump(final_schema, file, indent=4)

    client.close()

    return final_schema

# def get_oid_in_src_coll(host: str, port: int, username: str, password: str, db_name: str, src_coll_name:str, frg_key_candidate: str):

#     client = MongoClient(host=host, port=port, username=username, password=password)

#     db = client[db_name]

#     collection = db[src_coll_name]
#     document = collection.find_one({}, {frg_key_candidate: 1})

#     client.close()

#     if document:
#         return document[frg_key_candidate]
#     else:
#         return None

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
                print(attribute)
                coll = db[collection_name]
                oids = coll.distinct(attribute)
                for oid in oids:
                    found = False
                    for trgt_coll_name in final_schema:
                        if trgt_coll_name != collection_name:
                            if check_oid_in_trgt_coll(host=host, port=port, username=username, password=password, db_name=db_name, trgt_coll_name=trgt_coll_name, oid=oid):
                                print(f"Foreign key: {collection_name}.{attribute} -> {trgt_coll_name}._id")
                                found = True
                                break
                    if found:
                        break

basic_schema = {
    "students": {
        "_id": "oid",
        "name": "string",
        "address": "string"
    },
    "courses": {
        "_id": "oid",
        "student_id": "oid",
        "label": "string",
        "nbr_hours": "integer",
        "level": "string"
    }
}

find_foreign_keys(host="localhost", port=27017, username="root", password="rootadmin1234", db_name="db_univ", final_schema=basic_schema)