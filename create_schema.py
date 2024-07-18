import json

from pymongo import MongoClient
from pymongo_schema.extract import extract_pymongo_client_schema

client = MongoClient(host="localhost", port=27017, username="root", password="rootadmin1234")

schema = extract_pymongo_client_schema(client, "db_shop", "clients")

with open("schema.json", "w") as file:
    json.dump(schema, file, indent=4)

