# algorithm for find a primary key
# so the algorithm will list all the available field in a collection
# and then deterimine if a field or a combination of a fields can 
# uniquely identify the data
import json
import itertools

from pymongo import MongoClient

# TODO: program to count or determine the value of uniqueness of a field
# or a combination of a field
# input: all document in collection
# output: the uniqueness point (unique value/total data)
# kalau field tersebut tidak muncul di semua dokumen maka
# uniquecount akan ada dua, karena null
def candidate_key_by_collection(host: str, port:int, database:str, collection:str, user:str, password:str):
    
    total_documents = 0
    valid_fields = []
    candidate_key = []
    temp_candidate_key = []
    fields = set()

    client = MongoClient(host=host, port=int(port), username=user, password=password, serverSelectionTimeoutMS=5000)
    db = client[database]
    coll = db[collection]

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

        combinations = list(itertools.chain.from_iterable(itertools.combinations(temp_candidate_key, r) for r in range(2, len(temp_candidate_key) + 1)))

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

    return candidate_key

can_key = candidate_key_by_collection("localhost", 27017,"db_univ", "students", "root", "rootadmin1234")

print(can_key)

# field or combination of a fields that can unqiuely identify the data
# will be count as a candidate key
# primary key will be chosen from the candidate key
# primary key will be the shortest candidate key and also appear on other collection

# TODO: program to find candidate key

# TODO: program to choose the shortest candidate key

# TODO: program to check if the chosen primary key appear in other collection

