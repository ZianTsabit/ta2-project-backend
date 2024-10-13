import itertools

from pymongo import MongoClient


def get_candidate_key(client: MongoClient, collection: str):
    '''
    Function to get candidate key by collection
    '''
    total_documents = 0
    valid_fields = []
    candidate_key = []
    temp_candidate_key = []
    fields = set()

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

        combinations = []
        for r in range(2, len(temp_candidate_key) + 1):
            combinations.extend(itertools.combinations(temp_candidate_key, r))

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

    client.close()

    return candidate_key


def check_key_in_other_collection(client: MongoClient):
    '''
    Check if the instance of a field found in other collection
    get one instance of a key
    check all field on the target coll if that instance found or not
    '''
    pass


def check_key_type(client: MongoClient, key: str):
    '''
    Check data type of a field
    '''
    pass


def check_shortest_candidate_key(candidate_key: list):
    '''
    Get the shortest candidate key
    '''
    pass


def get_primary_key(client: MongoClient, collection: str):
    '''
    Function to get primary key by collection

    priority:
    1. candidate key found in other collection
    2. candidate key have type oid
    3. candidate key is shortest
    4. else
    '''
    pass
