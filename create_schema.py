import json

from pymongo import MongoClient
from pymongo_schema.extract import extract_pymongo_client_schema
from pymongo_schema.compare import compare_schemas_bases
from pymongo_schema.export import transform_data_to_file
from pymongo_schema.filter import filter_mongo_schema_namespaces
from pymongo_schema.tosql import mongo_schema_to_mapping


client = MongoClient(host="localhost", port=27017, username="root", password="rootadmin1234")

schema = extract_pymongo_client_schema(client, "db_univ", "students")

courses_data = schema["db_univ"]["students"]

object_data = courses_data["object"]

result = {}
for key, value in object_data.items():
    result[key] = value["type"]

with open("res_students.json", "w") as file:
    json.dump(result, file, indent=4)