import json

from pymongo import MongoClient
from pymongo_schema.compare import compare_schemas_bases
from pymongo_schema.tosql import mongo_schema_to_mapping
from pymongo_schema.export import transform_data_to_file
from pymongo_schema.extract import extract_pymongo_client_schema
from pymongo_schema.filter import filter_mongo_schema_namespaces

def process_object(object_data, parent_key="", result=None, final_schema=None):
    if result is None:
        result = {}
    if final_schema is None:
        final_schema = {}

    for key, value in object_data.items():
        data_type = value["type"]
        if data_type == "OBJECT":
            nested_result = process_object(value["object"], key, {}, final_schema)
            final_schema[key] = nested_result
        if data_type == "ARRAY" and value["array_type"] == "OBJECT":
            nested_result = process_object(value["object"], key, {}, final_schema)
            final_schema[key] = nested_result
        else:
            result[key] = data_type
    return result

def generate_basic_schema(host: str, port: int, username: str, password: str, db_name: str):
    client = MongoClient(host=host, port=port, username=username, password=password)
    db = client[db_name]
    collections = db.list_collection_names()
    final_schema = {}

    for coll in collections:
        schema = extract_pymongo_client_schema(client, db_name, coll)
        courses_data = schema[db_name][coll]
        object_data = courses_data["object"]
        processed_schema = process_object(object_data, coll, final_schema=final_schema)
        final_schema[coll] = processed_schema

    client.close()
    return final_schema

def check_oid_in_trgt_coll(host: str, port: int, username: str, password: str, db_name: str, trgt_coll_name:str, oid):

    client = MongoClient(host=host, port=port, username=username, password=password)
    db = client[db_name]
    coll = db[trgt_coll_name]

    foreign_key = coll.count_documents({'_id': oid}) > 0

    client.close()

    return foreign_key

def find_foreign_keys(host: str, port: int, username: str, password: str, db_name: str, basic_schema: dict):

    client = MongoClient(host=host, port=port, username=username, password=password)
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
                            if check_oid_in_trgt_coll(host=host, port=port, username=username, password=password, db_name=db_name, trgt_coll_name=trgt_coll_name, oid=oid):

                                if collection_name not in final_schema:
                                    final_schema[collection_name] = {'foreign_keys': {}}
                                
                                final_schema[collection_name]['foreign_keys'][attribute] = trgt_coll_name

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

# client = MongoClient(host="localhost", port=27017, username="root", password="rootadmin1234")

# schema = extract_pymongo_client_schema(client, "db_univ_2", "students")

# with open("./basic_schema/basic_schema_students.json", 'w') as file:
#     json.dump(schema, file, indent=4)