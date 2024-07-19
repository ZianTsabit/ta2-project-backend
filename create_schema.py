import json

from pymongo import MongoClient
from pymongo_schema.extract import extract_pymongo_client_schema
from pymongo_schema.compare import compare_schemas_bases
from pymongo_schema.export import transform_data_to_file
from pymongo_schema.filter import filter_mongo_schema_namespaces
from pymongo_schema.tosql import mongo_schema_to_mapping

def generate_schema(host: str, port: int, username: str, password: str, db_name: str):

    client = MongoClient(host=host, port=port, username=username, password=password)

    db = client[db_name]

    collections = db.list_collection_names()

    for coll in collections:
        
        schema = extract_pymongo_client_schema(client, db_name, coll)

        courses_data = schema[db_name][coll]

        object_data = courses_data["object"]

        result = {}
        for key, value in object_data.items():
            result[key] = value["type"]

        with open(f"./basic_schema/res_{coll}.json", "w") as file:
            json.dump(result, file, indent=4)

generate_schema(host="localhost", port=27017, username="root", password="rootadmin1234", db_name="db_univ")