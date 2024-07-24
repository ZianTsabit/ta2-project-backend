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
        elif data_type == "ARRAY" and value["array_type"] == "OBJECT":
            nested_result = process_object(value["object"], key, {}, final_schema)
            final_schema[key] = nested_result
        elif data_type == "ARRAY" and value["array_type"] == "oid":
            result[key] = value["array_type"]
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

basic_schema_using_foreign_keys = {
    "courses": {
        "_id": "oid",
        "label": "string",
        "nbr_hours": "integer",
        "level": "string",
        "student": "oid",
        "foreign_keys": {
            "student": "students"
        }
    },
    "students": {
        "_id": "oid",
        "name": "string",
        "address": "string",
        "course": "oid",
        "foreign_keys": {
            "course": "courses"
        }
    }
}

def generate_final_schema(tables: dict):

    cleaned_tables = {}
    for table, attributes in tables.items():
        cleaned_attributes = {k: v for k, v in attributes.items() if k != 'foreign_keys'}
        cleaned_tables[table] = cleaned_attributes
    
    processed_pairs = set()
    new_tables = {}

    for table1, attributes1 in tables.items():
        for key1, reference1 in attributes1.get("foreign_keys", {}).items():
            if reference1 in tables:
                table2 = reference1
                attributes2 = tables[table2]
                
                pair = tuple(sorted([table1, table2]))
                if pair not in processed_pairs:
                    for key2, reference2 in attributes2.get("foreign_keys", {}).items():
                        if reference2 == table1:
                            relationship_table_name = f"{pair[0]}_{pair[1]}"
                            new_tables[relationship_table_name] = {
                                f"{pair[0]}_id": "oid",
                                f"{pair[1]}_id": "oid",
                                "foreign_keys": {
                                    f"{pair[0]}_id": pair[0],
                                    f"{pair[1]}_id": pair[1]
                                }
                            }
                            processed_pairs.add(pair)

    cleaned_tables.update(new_tables)
    
    return cleaned_tables

def remove_non_id_fields(tables):
    cleaned_tables = {}
    for table, attributes in tables.items():
        cleaned_attributes = {
            k: v for k, v in attributes.items()
            if not (v == "oid" and 'id' not in k.lower())
        }
        cleaned_tables[table] = cleaned_attributes
    return cleaned_tables

final_schema = generate_final_schema(basic_schema_using_foreign_keys)

super_final_schema = remove_non_id_fields(final_schema)

with open("./basic_schema/final_schema_dbuniv2.json", 'w') as file:
    json.dump(super_final_schema, file, indent=4)