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
            result[f"{key}"] = value["array_type"]
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
                                
                                final_schema[collection_name]['foreign_keys'][attribute] = f"{trgt_coll_name}._id"

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

def generate_final_schema(tables: dict):

    cleaned_tables = {table: dict(attributes) for table, attributes in tables.items()}  # Make a deep copy
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
                    for key2, reference2 in attributes2.get("foreign_keys", {}).items():
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
        if "foreign_keys" in cleaned_tables[table1] and table2 in cleaned_tables[table1]["foreign_keys"]:
            del cleaned_tables[table1]["foreign_keys"][table2]
            del cleaned_tables[table1][table2]
        if "foreign_keys" in cleaned_tables[table2] and table1 in cleaned_tables[table2]["foreign_keys"]:
            del cleaned_tables[table2]["foreign_keys"][table1]
            del cleaned_tables[table2][table1]
    
    cleaned_tables.update(new_tables)
    
    return cleaned_tables

basic_schema = generate_basic_schema(host="localhost", port=27017, username="root", password="rootadmin1234", db_name="db_univ_2")

basic_schema_with_foreign_key = find_foreign_keys(host="localhost", port=27017, username="root", password="rootadmin1234", db_name="db_univ_2", basic_schema=basic_schema)

final_schema = generate_final_schema(tables=basic_schema_with_foreign_key)

with open('./basic_schema/final_schema.json', 'w') as file:
    json.dump(final_schema, file, indent=4)