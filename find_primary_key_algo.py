# algorithm for find a primary key
# so the algorithm will list all the available field in a collection
# and then deterimine if a field or a combination of a fields can 
# uniquely identify the data
import json

from pymongo import MongoClient

# TODO: program to count or determine the value of uniqueness of a field
# or a combination of a field
# input: all document in collection
# output: the uniqueness point (unique value/total data)
def unique_fields_by_collection(host: str, port:int, database:str, collection:str, user:str, password:str):
    
    client = MongoClient(host=host, port=int(port), username=user, password=password, serverSelectionTimeoutMS=5000)
    db = client[database]
    coll = db[collection]

    total_documents = coll.count_documents({})
    
    fields = set()

    for document in coll.find():
        fields.update(document.keys())

    unique_values = coll.aggregate([
        {"$group": {
            "_id": "$job"}},
        {"$count": "uniqueCount"} 
    ])

    print(list(unique_values))
    
unique_fields_by_collection("localhost", 27017,"db_univ", "students", "root", "rootadmin1234")

# field or combination of a fields that can unqiuely identify the data
# will be count as a candidate key
# primary key will be chosen from the candidate key
# primary key will be the shortest candidate key and also appear on other collection

# TODO: program to find candidate key

# TODO: program to choose the shortest candidate key

# TODO: program to check if the chosen primary key appear in other collection

