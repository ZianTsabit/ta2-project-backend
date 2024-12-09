import itertools
from typing import Dict

from fuzzywuzzy import fuzz
from pydantic import BaseModel
from pymongo import MongoClient
from pymongo_schema.extract import extract_pymongo_client_schema

from models.mongodb.cardinalities import Cardinalities
from models.mongodb.collection import Collection
from models.mongodb.field import Field
from models.type import CardinalitiesType, MongoType


class MongoDB(BaseModel):
    host: str
    port: int
    db: str
    username: str
    password: str
    collections: Dict = {}

    def create_client(cls) -> MongoClient:

        client = MongoClient(
            host=cls.host,
            port=cls.port,
            username=cls.username,
            password=cls.password,
            serverSelectionTimeoutMS=5000,
        )

        return client

    def test_connection(cls) -> bool:
        client = None
        try:
            client = cls.create_client()
            client.server_info()
            return True

        except Exception as e:
            print(e)
            return False

        finally:
            if client:
                client.close()

    def init_collection(cls):

        basic_schema = cls.generate_basic_schema()

        for coll in basic_schema.keys():
            collection = Collection(name=coll)
            for field in basic_schema[coll]:
                field = Field(
                    name=field["name"],
                    data_type=field["data_type"],
                    not_null=field["not_null"],
                    unique=field["unique"],
                )

                collection.fields.append(field)

            cls.collections[coll] = collection.fields

    def process_object(
        cls, coll_name, object_data, parent_key="", result=None, final_schema=None
    ) -> dict:

        client = cls.create_client()
        db = client[cls.db]
        collection = db[coll_name]

        if result is None:
            result = []

        if final_schema is None:
            final_schema = {}

        for key, value in object_data.items():

            res = {}
            data_type = value["type"]
            total_documents = value["count"]

            if data_type == "OBJECT":

                nested_result = cls.process_object(
                    coll_name=coll_name,
                    object_data=value["object"],
                    parent_key=key,
                    result=[],
                    final_schema=final_schema,
                )

                final_schema[key] = nested_result

                not_null = False
                count = collection.aggregate(
                    [{"$match": {f"{key}": {"$ne": None}}}, {"$count": "count"}]
                )

                count_values = list(count)[0]["count"]

                if count_values == total_documents:
                    not_null = True

                unique = False
                unique_values = collection.aggregate(
                    [{"$group": {"_id": f"${key}"}}, {"$count": "uniqueCount"}]
                )

                unique_count = list(unique_values)[0]["uniqueCount"]
                uniqueness = unique_count / total_documents

                if uniqueness == 1.0:
                    unique = True

                res["name"] = key
                res["data_type"] = "object"
                res["not_null"] = not_null
                res["unique"] = unique

            elif data_type == "ARRAY" and value["array_type"] == "OBJECT":

                nested_result = cls.process_object(
                    coll_name=coll_name,
                    object_data=value["object"],
                    parent_key=key,
                    result=[],
                    final_schema=final_schema,
                )

                final_schema[key] = nested_result

                not_null = False
                if value["prop_in_object"] == 1.0:
                    not_null = True

                unique = False

                array_size_count = collection.aggregate(
                    [
                        {"$project": {"_id": 0, "arraySize": {"$size": f"${key}"}}},
                        {
                            "$group": {
                                "_id": None,
                                "totalArraySize": {"$sum": "$arraySize"},
                            }
                        },
                    ]
                )

                total_array_document = collection.aggregate(
                    [
                        {
                            "$unwind": {
                                "path": f"${key}",
                                "preserveNullAndEmptyArrays": True,
                            }
                        },
                        {"$project": {"_id": 0, f"{key}": 1}},
                        {"$group": {"_id": f"${key}"}},
                        {"$count": "totalArrayDoc"},
                    ]
                )

                array_size = list(array_size_count)[0]["totalArraySize"]
                total_array = list(total_array_document)[0]["totalArrayDoc"]

                if array_size == total_array:
                    unique = True

                res["name"] = key
                res["data_type"] = f"{data_type.lower()}.{value['array_type'].lower()}"
                res["not_null"] = not_null
                res["unique"] = unique

            elif data_type == "ARRAY" and value["array_type"] != "OBJECT":

                unique = False

                array_size_count = collection.aggregate(
                    [
                        {"$project": {"_id": 0, "arraySize": {"$size": f"${key}"}}},
                        {
                            "$group": {
                                "_id": None,
                                "totalArraySize": {"$sum": "$arraySize"},
                            }
                        },
                    ]
                )

                total_array_document = collection.aggregate(
                    [
                        {
                            "$unwind": {
                                "path": f"${key}",
                                "preserveNullAndEmptyArrays": True,
                            }
                        },
                        {"$project": {"_id": 0, f"{key}": 1}},
                        {"$group": {"_id": f"${key}"}},
                        {"$count": "totalArrayDoc"},
                    ]
                )

                array_size = list(array_size_count)[0]["totalArraySize"]
                total_array = list(total_array_document)[0]["totalArrayDoc"]

                if array_size == total_array:
                    unique = True

                not_null = False
                if value["prop_in_object"] == 1.0:
                    not_null = True

                res["name"] = f"{key}"
                res["data_type"] = f"{data_type.lower()}.{value['array_type'].lower()}"
                res["not_null"] = not_null
                res["unique"] = unique

            else:
                not_null = False
                if coll_name == parent_key:
                    if value["prop_in_object"] == 1.0:
                        not_null = True
                else:
                    pipeline = [{"$unwind": f"${parent_key}"}, {"$count": "count"}]
                    count_values = collection.aggregate(pipeline)
                    res_count = list(count_values)[0]["count"]
                    if round(value["prop_in_object"], 2) == round(
                        res_count / collection.count_documents({}), 2
                    ):
                        not_null = True

                unique = False
                if coll_name == parent_key:
                    unique_values = collection.aggregate(
                        [{"$group": {"_id": f"${key}"}}, {"$count": "uniqueCount"}]
                    )

                    unique_count = list(unique_values)[0]["uniqueCount"]
                    uniqueness = unique_count / total_documents

                    if uniqueness == 1.0:
                        unique = True

                res["name"] = key
                res["data_type"] = data_type
                res["not_null"] = not_null
                res["unique"] = unique

            result.append(res)

        client.close()

        return result

    def generate_basic_schema(cls) -> dict:

        client = cls.create_client()
        db = client[cls.db]
        collections = db.list_collection_names()
        final_schema = {}

        for coll in collections:
            schema = extract_pymongo_client_schema(client, cls.db, coll)
            object_data = schema[cls.db][coll]["object"]

            processed_schema = cls.process_object(
                coll_name=coll,
                object_data=object_data,
                parent_key=coll,
                final_schema=final_schema,
            )

            final_schema[coll] = processed_schema

        client.close()

        return final_schema

    def get_candidate_key(cls, coll_name: str) -> list:

        client = cls.create_client()
        db = client[cls.db]

        collection = db[coll_name]
        total_documents = collection.count_documents({})

        candidate_key = []
        temp_candidate_key = []

        for j in cls.collections[coll_name]:

            if j.unique is True:
                candidate_key.append(j.name)
            else:
                temp_candidate_key.append(j.name)

        if len(temp_candidate_key) > 1:

            combinations = []
            for r in range(2, len(temp_candidate_key) + 1):
                combinations.extend(itertools.combinations(temp_candidate_key, r))

            for comb in combinations:
                rem_fields = list(comb)

                inside_query = {}
                for z in rem_fields:
                    inside_query[z] = f"${z}"

                unique_values = collection.aggregate(
                    [{"$group": {"_id": inside_query}}, {"$count": "uniqueCount"}]
                )

                result = list(unique_values)[0]["uniqueCount"]
                uniqueness = result / total_documents

                if uniqueness == 1.0:
                    candidate_key.append(",".join(rem_fields))

        client.close()

        return candidate_key

    def get_candidate_key_embedded(cls, coll_name: str) -> list:

        client = cls.create_client()
        db = client[cls.db]

        candidate_key = []
        temp_candidate_key = []

        parent = cls.check_parent_collection(coll_name) + f".{coll_name}"
        parent_list = parent.split(".")
        parent_coll = ".".join(parent_list[1:])

        collection = db[parent_list[0]]

        for i in cls.collections[coll_name]:

            total_document_query = [
                {"$group": {"_id": f"${parent_coll}"}},
                {"$count": "total_documents"},
            ]

            total_documents_docs = collection.aggregate(total_document_query)

            total_documents = list(total_documents_docs)[0]["total_documents"]

            pipeline = [
                {"$project": {"_id": 0, f"{i.name}": f"${parent_coll}.{i.name}"}},
                {"$group": {"_id": f"${i.name}"}},
                {"$count": "uniqueCount"},
            ]

            unique_values = collection.aggregate(pipeline)

            unique_count = list(unique_values)[0]["uniqueCount"]
            uniqueness = unique_count / total_documents

            if i.not_null is True:
                if uniqueness == 1.0:
                    candidate_key.append(i.name)
                else:
                    temp_candidate_key.append(i.name)

        if len(temp_candidate_key) > 0:

            combinations = []
            for r in range(2, len(temp_candidate_key) + 1):
                combinations.extend(itertools.combinations(temp_candidate_key, r))

            for comb in combinations:
                rem_fields = list(comb)

                project_query = {}
                project_query["_id"] = 0
                group_query = {}

                for z in rem_fields:
                    project_query[z] = f"${parent_coll}.{z}"
                    group_query[z] = f"${z}"

                pipeline = [
                    {"$project": project_query},
                    {"$group": {"_id": group_query}},
                    {"$count": "uniqueCount"},
                ]

                unique_values = collection.aggregate(pipeline)
                result = list(unique_values)[0]["uniqueCount"]
                uniqueness = result / total_documents

                if uniqueness == 1.0:
                    candidate_key.append(",".join(rem_fields))

        client.close()

        return candidate_key

    def get_candidate_key_array_embedded(cls, coll_name: str) -> list:

        client = cls.create_client()
        db = client[cls.db]

        candidate_key = []
        temp_candidate_key = []

        parent = cls.check_parent_collection(coll_name) + f".{coll_name}"
        parent_list = parent.split(".")
        parent_coll = ".".join(parent_list[1:])

        collection = db[parent_list[0]]

        total_document_pipeline = [
            {"$unwind": f"${parent_coll}"},
            {"$group": {"_id": f"${parent_coll}"}},
        ]

        total_documents = len(list(collection.aggregate(total_document_pipeline)))

        for i in cls.collections[coll_name]:

            pipeline = [
                {"$unwind": f"${coll_name}"},
                {"$group": {"_id": f"${coll_name}"}},
                {"$project": {"_id": 0, f"{i.name}": f"$_id.{i.name}"}},
                {"$group": {"_id": f"${i.name}"}},
                {"$match": {"_id": {"$ne": None}}},
                {"$count": "uniqueCount"},
            ]

            unique_values = collection.aggregate(pipeline)

            unique_count = list(unique_values)[0]["uniqueCount"]
            uniqueness = unique_count / total_documents

            if i.not_null is True:

                if uniqueness == 1.0:
                    candidate_key.append(i.name)
                else:
                    temp_candidate_key.append(i.name)

        if len(temp_candidate_key) > 0:

            combinations = []
            for r in range(2, len(temp_candidate_key) + 1):
                combinations.extend(itertools.combinations(temp_candidate_key, r))

            for comb in combinations:
                rem_fields = list(comb)

                project_query = {}
                project_query["_id"] = 0
                group_query = {}

                for z in rem_fields:
                    project_query[z] = f"${parent_coll}.{z}"
                    group_query[z] = f"${z}"

                pipeline = [
                    {"$unwind": f"${coll_name}"},
                    {"$group": {"_id": f"${coll_name}"}},
                    {"$project": project_query},
                    {"$group": {"_id": group_query}},
                    {"$count": "uniqueCount"},
                ]

                unique_values = collection.aggregate(pipeline)
                result = list(unique_values)[0]["uniqueCount"]
                uniqueness = result / total_documents

                if uniqueness == 1.0:
                    candidate_key.append(",".join(rem_fields))

        client.close()

        return candidate_key

    def check_key_in_other_collection(cls, src_key: str, src_coll: str) -> bool:

        res = {}

        client = cls.create_client()
        db = client[cls.db]

        collections = list(cls.collections.keys())
        collections.remove(src_coll)

        source_coll = db[src_coll]

        for c in collections:
            fields = cls.collections[c]
            for field in fields:
                pipeline = [{"$project": {f"{src_key}": 1}}]

                values = list(source_coll.aggregate(pipeline))

                for v in values:
                    target_coll = db[c]
                    find = v[src_key]
                    found = list(target_coll.find({f"{field.name}": find}))

                    if len(found) > 0:
                        if fuzz.ratio(field.name, src_coll + src_key) > 90:

                            res["collection"] = c
                            res["field"] = field.name
                            res["status"] = True

                            return res

        client.close()

        res["collection"] = None
        res["field"] = None
        res["status"] = False

        return res

    def check_key_type(cls, src_key: str, src_coll: str) -> str:

        fields = cls.collections[src_coll]

        for f in fields:
            if f.name == src_key:
                return f.data_type

    def check_embedding_collection(cls, src_coll: str) -> bool:

        collections = list(cls.collections.keys())
        collections.remove(src_coll)

        for coll in collections:
            for field in cls.collections[coll]:
                if field.name == src_coll and (
                    field.data_type == MongoType.OBJECT
                    or field.data_type == MongoType.ARRAY_OF_OBJECT
                ):
                    return {"name": coll, "data_type": field.data_type}

        return None

    def check_parent_collection(cls, src_coll: str) -> str:

        collections = list(cls.collections.keys())
        if src_coll in collections:
            collections.remove(src_coll)

        for coll in collections:
            for field in cls.collections[coll]:
                if field.name == src_coll and (
                    field.data_type == MongoType.OBJECT
                    or field.data_type == MongoType.ARRAY_OF_OBJECT
                ):

                    deeper_check = cls.check_parent_collection(coll)
                    if deeper_check:
                        return coll + "." + deeper_check
                    return coll

        return ""

    def check_parent_field(cls, field_name: str) -> str:

        collections = list(cls.collections.keys())

        for coll in collections:
            for field in cls.collections[coll]:
                if field.name == field_name and (
                    field.data_type == MongoType.ARRAY_OF_STRING
                    or field.data_type == MongoType.ARRAY_OF_BIG_INT
                    or field.data_type == MongoType.ARRAY_OF_FLOAT
                    or field.data_type == MongoType.ARRAY_OF_NUM
                    or field.data_type == MongoType.ARRAY_OF_DATE
                    or field.data_type == MongoType.ARRAY_OF_OID
                ):
                    return coll, field.data_type

        return "", ""

    def check_shortest_candidate_key(cls, candidate_key) -> str:

        shortest = candidate_key[0].split(",")
        idx = 0

        for i in range(1, len(candidate_key)):
            key = candidate_key[i].split(",")
            if len(key) < len(shortest):
                idx = 0

        return candidate_key[idx]

    def get_primary_key(cls, coll_name: str) -> str:

        parent_coll = cls.check_embedding_collection(coll_name)

        if parent_coll is None:

            candidate_key = cls.get_candidate_key(coll_name)

            for f in candidate_key:
                if cls.check_key_in_other_collection(f, coll_name)["status"] is True:
                    return f

                elif cls.check_key_type(f, coll_name) == "oid":
                    return f

                elif cls.check_shortest_candidate_key(candidate_key) == f:
                    return f

        else:

            if parent_coll["data_type"] == MongoType.OBJECT:

                candidate_key = cls.get_candidate_key_embedded(coll_name)

                for f in candidate_key:

                    if (
                        len(f.split(",")) == 1
                        and cls.check_key_type(f, coll_name) == "oid"
                    ):
                        return f

                    elif cls.check_shortest_candidate_key(candidate_key) == f:
                        return f

            elif parent_coll["data_type"] == MongoType.ARRAY_OF_OBJECT:

                candidate_key = cls.get_candidate_key_array_embedded(coll_name)

                for f in candidate_key:

                    if (
                        len(f.split(",")) == 1
                        and cls.check_key_type(f, coll_name) == "oid"
                    ):
                        return f

                    elif cls.check_shortest_candidate_key(candidate_key) == f:
                        return f

    def mapping_cardinalities(cls, coll_name: str) -> str:

        res = []

        collections = list(cls.collections.keys())
        collections.remove(coll_name)

        for coll in collections:

            field = list(filter(lambda x: x.name == coll_name, cls.collections[coll]))

            if len(field) > 0:

                for f in field:

                    field_2 = list(
                        filter(lambda x: x.name == coll, cls.collections[coll_name])
                    )

                    if len(field_2) > 0:

                        cardinality = Cardinalities(
                            source=coll_name,
                            destination=coll,
                            type=CardinalitiesType.MANY_TO_MANY,
                        )

                        if not any(
                            (
                                (
                                    d.source == cardinality.source
                                    and d.destination == cardinality.destination
                                )
                                or (
                                    d.source == cardinality.destination
                                    and d.destination == cardinality.source
                                )
                            )
                            for d in res
                        ):
                            res.append(cardinality)

                    else:

                        if f.data_type == MongoType.OBJECT:

                            if f.unique is True:

                                cardinality = Cardinalities(
                                    source=coll,
                                    destination=coll_name,
                                    type=CardinalitiesType.ONE_TO_ONE,
                                )

                                if not any(
                                    (
                                        (
                                            d.source == cardinality.source
                                            and d.destination == cardinality.destination
                                        )
                                        or (
                                            d.source == cardinality.destination
                                            and d.destination == cardinality.source
                                        )
                                    )
                                    for d in res
                                ):
                                    res.append(cardinality)

                            else:

                                cardinality = Cardinalities(
                                    source=coll_name,
                                    destination=coll,
                                    type=CardinalitiesType.ONE_TO_MANY,
                                )

                                if not any(
                                    (
                                        (
                                            d.source == cardinality.source
                                            and d.destination == cardinality.destination
                                        )
                                        or (
                                            d.source == cardinality.destination
                                            and d.destination == cardinality.source
                                        )
                                    )
                                    for d in res
                                ):
                                    res.append(cardinality)

                        elif f.data_type == MongoType.ARRAY_OF_OBJECT:

                            if f.unique is True:

                                cardinality = Cardinalities(
                                    source=coll,
                                    destination=coll_name,
                                    type=CardinalitiesType.ONE_TO_MANY,
                                )

                                if not any(
                                    (
                                        (
                                            d.source == cardinality.source
                                            and d.destination == cardinality.destination
                                        )
                                        or (
                                            d.source == cardinality.destination
                                            and d.destination == cardinality.source
                                        )
                                    )
                                    for d in res
                                ):
                                    res.append(cardinality)

                            else:

                                cardinality = Cardinalities(
                                    source=coll,
                                    destination=coll_name,
                                    type=CardinalitiesType.MANY_TO_MANY,
                                )

                                if not any(
                                    (
                                        (
                                            d.source == cardinality.source
                                            and d.destination == cardinality.destination
                                        )
                                        or (
                                            d.source == cardinality.destination
                                            and d.destination == cardinality.source
                                        )
                                    )
                                    for d in res
                                ):
                                    res.append(cardinality)

                        elif (
                            f.data_type == MongoType.ARRAY_OF_STRING
                            or f.data_type == MongoType.ARRAY_OF_BIG_INT
                            or f.data_type == MongoType.ARRAY_OF_FLOAT
                            or f.data_type == MongoType.ARRAY_OF_NUM
                            or f.data_type == MongoType.ARRAY_OF_DATE
                            or f.data_type == MongoType.ARRAY_OF_OID
                        ):

                            cardinality = None
                            if f.unique is True:
                                cardinality = Cardinalities(
                                    source=coll,
                                    destination=coll_name,
                                    type=CardinalitiesType.ONE_TO_MANY,
                                )
                            else:
                                cardinality = Cardinalities(
                                    source=coll,
                                    destination=coll_name,
                                    type=CardinalitiesType.MANY_TO_MANY,
                                )

                            if not any(
                                (
                                    (
                                        d.source == cardinality.source
                                        and d.destination == cardinality.destination
                                    )
                                    or (
                                        d.source == cardinality.destination
                                        and d.destination == cardinality.source
                                    )
                                )
                                for d in res
                            ):
                                res.append(cardinality)

        for f in cls.collections[coll_name]:

            if (
                f.data_type == MongoType.ARRAY_OF_STRING
                or f.data_type == MongoType.ARRAY_OF_BIG_INT
                or f.data_type == MongoType.ARRAY_OF_FLOAT
                or f.data_type == MongoType.ARRAY_OF_NUM
                or f.data_type == MongoType.ARRAY_OF_DATE
                or f.data_type == MongoType.ARRAY_OF_OID
            ):
                cardinality = None
                if f.unique is True:
                    cardinality = Cardinalities(
                        source=coll_name,
                        destination=f.name,
                        type=CardinalitiesType.ONE_TO_MANY,
                    )
                else:
                    cardinality = Cardinalities(
                        source=coll_name,
                        destination=f.name,
                        type=CardinalitiesType.MANY_TO_MANY,
                    )

                if not any(
                    (
                        (
                            d.source == cardinality.source
                            and d.destination == cardinality.destination
                        )
                        or (
                            d.source == cardinality.destination
                            and d.destination == cardinality.source
                        )
                    )
                    for d in res
                ):
                    res.append(cardinality)

            elif f.data_type == MongoType.OBJECT:

                if f.unique is True:

                    cardinality = Cardinalities(
                        source=f.name,
                        destination=coll_name,
                        type=CardinalitiesType.ONE_TO_ONE,
                    )

                    if not any(
                        (
                            (
                                d.source == cardinality.source
                                and d.destination == cardinality.destination
                            )
                            or (
                                d.source == cardinality.destination
                                and d.destination == cardinality.source
                            )
                        )
                        for d in res
                    ):
                        res.append(cardinality)

                else:

                    cardinality = Cardinalities(
                        source=coll_name,
                        destination=f.name,
                        type=CardinalitiesType.ONE_TO_MANY,
                    )

                    if not any(
                        (
                            (
                                d.source == cardinality.source
                                and d.destination == cardinality.destination
                            )
                            or (
                                d.source == cardinality.destination
                                and d.destination == cardinality.source
                            )
                        )
                        for d in res
                    ):
                        res.append(cardinality)

            elif f.data_type == MongoType.ARRAY_OF_OBJECT:

                if f.unique is True:

                    cardinality = Cardinalities(
                        source=coll_name,
                        destination=f.name,
                        type=CardinalitiesType.ONE_TO_MANY,
                    )

                    if not any(
                        (
                            (
                                d.source == cardinality.source
                                and d.destination == cardinality.destination
                            )
                            or (
                                d.source == cardinality.destination
                                and d.destination == cardinality.source
                            )
                        )
                        for d in res
                    ):
                        res.append(cardinality)

                else:

                    cardinality = Cardinalities(
                        source=coll_name,
                        destination=f.name,
                        type=CardinalitiesType.MANY_TO_MANY,
                    )

                    if not any(
                        (
                            (
                                d.source == cardinality.source
                                and d.destination == cardinality.destination
                            )
                            or (
                                d.source == cardinality.destination
                                and d.destination == cardinality.source
                            )
                        )
                        for d in res
                    ):
                        res.append(cardinality)

        primary_key = cls.get_primary_key(coll_name)

        check_key = cls.check_key_in_other_collection(primary_key, coll_name)

        if check_key["status"] is True:

            field = list(
                filter(
                    lambda x: x.name == check_key["field"],
                    cls.collections[check_key["collection"]],
                )
            )

            for f in field:

                if f.unique is True:

                    cardinality = Cardinalities(
                        source=coll_name,
                        destination=check_key["collection"],
                        type=CardinalitiesType.ONE_TO_ONE,
                    )

                    if not any(
                        (
                            (
                                d.source == cardinality.source
                                and d.destination == cardinality.destination
                            )
                            or (
                                d.source == cardinality.destination
                                and d.destination == cardinality.source
                            )
                        )
                        for d in res
                    ):
                        res.append(cardinality)

                else:

                    cardinality = Cardinalities(
                        source=coll_name,
                        destination=check_key["collection"],
                        type=CardinalitiesType.ONE_TO_MANY,
                    )

                    if not any(
                        (
                            (
                                d.source == cardinality.source
                                and d.destination == cardinality.destination
                            )
                            or (
                                d.source == cardinality.destination
                                and d.destination == cardinality.source
                            )
                        )
                        for d in res
                    ):
                        res.append(cardinality)

        return res

    def mapping_all_cardinalities(cls):

        collections = list(cls.collections.keys())
        res = []

        for coll in collections:

            cardinalities = cls.mapping_cardinalities(coll)

            for card in cardinalities:

                if not any(
                    (
                        (d.source == card.source and d.destination == card.destination)
                        or (
                            d.source == card.destination
                            and d.destination == card.source
                        )
                    )
                    for d in res
                ):
                    res.append(card)

        return res

    def get_collections(cls):
        return cls.collections

    def display_schema(cls):

        client = cls.create_client()
        db = client[cls.db]
        collections = db.list_collection_names()
        final_schema = {}

        for coll in collections:
            schema = extract_pymongo_client_schema(client, cls.db, coll)
            object_data = schema[cls.db][coll]["object"]

            final_schema[coll] = object_data

        client.close()

        summary = {}

        for coll in collections:
            summary[coll] = {}
            for key, value in final_schema[coll].items():

                if "array_type" in value:
                    if value["array_type"] == "OBJECT":
                        nested_object_summary = {
                            sub_key: sub_value["type"]
                            for sub_key, sub_value in value["object"].items()
                        }
                        summary[coll][key] = [nested_object_summary]
                    else:
                        summary[coll][key] = [value["array_type"]]

                elif "object" in value:
                    summary[coll][key] = {
                        sub_key: sub_value["type"]
                        for sub_key, sub_value in value["object"].items()
                    }
                else:
                    summary[coll][key] = value["type"]

        return summary

    def get_data_by_collection(
        cls, relation: dict, cardinality_type: CardinalitiesType
    ):

        client = cls.create_client()
        db = client[cls.db]

        coll_name = list(relation.keys())[0]
        fields = list(relation[coll_name].keys())

        colls = coll_name.split("_")

        parent_coll = ""
        if len(colls) < 2:
            parent_coll = cls.check_parent_collection(coll_name)

        parent_field, field_type = cls.check_parent_field(coll_name)

        coll_data_type = None
        if parent_coll != "":
            attr = cls.collections[parent_coll]
            for i in attr:
                if i.name == coll_name:
                    coll_data_type = i.data_type

        data = []

        if len(colls) > 1:

            coll_1 = colls[0]
            coll_2 = colls[1]

            coll_1_parent_coll = cls.check_parent_collection(coll_1)
            coll_1_parent_field = cls.check_parent_field(coll_1)

            coll_2_parent_coll = cls.check_parent_collection(coll_2)
            coll_2_parent_field = cls.check_parent_field(coll_2)

            if coll_2_parent_field[0] == coll_1 and coll_1_parent_field[0] == coll_2:

                project_query = {}
                project_query["_id"] = 0

                for i in fields:

                    if i.split(".")[0] == coll_2:
                        project_query[i.split(".")[-1]] = f"${coll_2}"

                    else:
                        field = i.split(".")[-1]
                        field_name = i.replace(f"{coll_1}.{coll_1}_", "")
                        project_query[field] = f"${field_name}"

                query = [
                    {"$unwind": {"path": f"${coll_2}"}},
                    {"$project": project_query},
                ]

                coll = db[coll_1]

                docs = coll.aggregate(query)

                data = list(docs)

            elif (
                coll_2_parent_field[0] == coll_1
                and coll_1_parent_field[0] == ""
                and coll_1_parent_coll == ""
            ):

                project_query = {}
                project_query["_id"] = 0
                for i in fields:
                    if i.split(".")[0] == coll_2:
                        project_query[coll_2] = "$_id"

                query = [
                    {"$unwind": {"path": f"${coll_2}"}},
                    {"$group": {"_id": f"${coll_2}"}},
                    {"$sort": {"_id": 1}},
                    {"$project": project_query},
                ]

                coll = db[coll_1]
                docs = coll.aggregate(query)
                datas = list(docs)
                value_id = {}
                for i in range(0, len(datas)):
                    value_id[datas[i][coll_2]] = i + 1

                project_query = {}
                project_query["_id"] = 0
                for i in fields:
                    if i.split(".")[0] == coll_2_parent_field[0]:
                        project_query[i.split(".")[-1]] = (
                            f'${i.replace(f"{coll_2_parent_field[0]}.{coll_2_parent_field[0]}_","")}'
                        )
                    else:
                        project_query[f'{i.split(".")[-1]}'] = f'${i.split(".")[0]}'

                query = [
                    {"$unwind": {"path": f"${coll_2}"}},
                    {"$project": project_query},
                ]

                coll = db[coll_1]

                docs = coll.aggregate(query)

                datas = list(docs)

                data = []

                for d in datas:
                    res = {}
                    for f in fields:
                        if f.split(".")[0] == coll_1:
                            res[f.split(".")[-1]] = d[f.split(".")[-1]]
                        else:
                            res[f.split(".")[-1]] = value_id[d[f.split(".")[-1]]]

                    data.append(res)

            elif (
                coll_2_parent_coll == coll_1
                and coll_1_parent_field[0] == ""
                and coll_1_parent_coll == ""
            ):

                project_query = {}
                project_query["_id"] = 0

                for i in fields:

                    if i.split(".")[0] == coll_2_parent_coll:
                        project_query[i.split(".")[-1]] = (
                            f'${i.replace(f"{coll_2_parent_coll}.{coll_2_parent_coll}_","")}'
                        )

                    else:
                        field_name = i.split(".")[-1]
                        coll_name = i.split(".")[0]
                        project_query[field_name] = (
                            f'${coll_name}.{field_name.replace(f"{coll_name}_","")}'
                        )

                query = [
                    {"$unwind": {"path": f"${coll_2}"}},
                    {"$project": project_query},
                ]

                coll = db[coll_1]

                docs = coll.aggregate(query)

                data = list(docs)

            elif coll_2_parent_field[0] == coll_1 and coll_2 in cls.collections:

                project_query = {}
                project_query["_id"] = 0

                for i in fields:

                    if i.split(".")[0] == coll_1:
                        project_query[i.split(".")[-1]] = (
                            f'${i.replace(f"{coll_1}.{coll_1}_","")}'
                        )

                    else:
                        field_name = i.split(".")[-1]
                        coll_name = i.split(".")[0]
                        project_query[field_name] = f"${coll_name}"

                query = [
                    {"$unwind": {"path": f"${coll_2}"}},
                    {"$project": project_query},
                ]

                coll = db[coll_1]

                docs = coll.aggregate(query)

                data = list(docs)

        else:

            if parent_coll != "":

                if coll_data_type is not None and coll_data_type == MongoType.OBJECT:

                    if cardinality_type == CardinalitiesType.ONE_TO_ONE:

                        project_query = {}
                        project_query["_id"] = 0
                        for i in fields:
                            if i.split(".")[0] == parent_coll:
                                project_query[i.split(".")[-1]] = (
                                    f'${i.replace(f"{parent_coll}.{parent_coll}_","")}'
                                )
                            else:
                                project_query[i] = f"${coll_name}.{i}"

                        query = [{"$project": project_query}]

                        coll = db[parent_coll]

                        docs = coll.aggregate(query)

                        data = list(docs)

                    elif cardinality_type is None:

                        project_query = {}
                        project_query["_id"] = 0
                        for i in fields:
                            project_query[i] = f"$_id.{i}"

                        query = [
                            {"$group": {"_id": f"${coll_name}"}},
                            {"$project": project_query},
                        ]

                        coll = db[parent_coll]

                        docs = coll.aggregate(query)

                        data = list(docs)

                elif (
                    coll_data_type is not None
                    and coll_data_type == MongoType.ARRAY_OF_OBJECT
                ):

                    if cardinality_type == CardinalitiesType.ONE_TO_MANY:

                        project_query = {}
                        project_query["_id"] = 0
                        for i in fields:
                            if i.split(".")[0] == parent_coll:
                                project_query[i.split(".")[-1]] = (
                                    f'${i.replace(f"{parent_coll}.{parent_coll}_","")}'
                                )
                            else:
                                project_query[i] = f"${coll_name}.{i}"

                        query = [
                            {"$unwind": {"path": f"${coll_name}"}},
                            {"$project": project_query},
                        ]

                        coll = db[parent_coll]

                        docs = coll.aggregate(query)

                        data = list(docs)

                    elif cardinality_type == CardinalitiesType.MANY_TO_MANY:

                        project_query = {}
                        project_query["_id"] = 0
                        for i in fields:
                            project_query[i] = f"$_id.{i}"

                        query = [
                            {"$unwind": {"path": f"${coll_name}"}},
                            {"$group": {"_id": f"${coll_name}"}},
                            {"$project": project_query},
                        ]

                        coll = db[parent_coll]

                        docs = coll.aggregate(query)

                        data = list(docs)

            elif parent_field is not None and (
                field_type == MongoType.ARRAY_OF_STRING
                or field_type == MongoType.ARRAY_OF_BIG_INT
                or field_type == MongoType.ARRAY_OF_FLOAT
                or field_type == MongoType.ARRAY_OF_NUM
                or field_type == MongoType.ARRAY_OF_DATE
                or field_type == MongoType.ARRAY_OF_OID
            ):

                if cardinality_type == CardinalitiesType.ONE_TO_MANY:

                    project_query = {}
                    project_query["_id"] = 0
                    lookup_query = {}

                    for i in fields:

                        if i.split(".")[0] == parent_field:

                            if coll_name in cls.collections:

                                lookup_query = {
                                    "from": f"{coll_name}",
                                    "localField": f"{coll_name}",
                                    "foreignField": f'{i.replace(f"{parent_field}.{parent_field}_","")}',
                                    "as": f"{coll_name}",
                                }

                            project_query[i.split(".")[-1]] = (
                                f'${i.replace(f"{parent_field}.{parent_field}_","")}'
                            )

                        elif "_value" in i:

                            project_query[i] = f"${coll_name}"

                        else:

                            project_query[i] = f"${coll_name}.{i}"

                    pipeline = []

                    if lookup_query != {}:
                        pipeline.append({"$lookup": lookup_query})

                    pipeline += [
                        {"$unwind": f"${coll_name}"},
                        {"$project": project_query},
                    ]

                    coll = db[parent_field]

                    docs = coll.aggregate(pipeline)

                    data = list(docs)

                elif cardinality_type == CardinalitiesType.MANY_TO_MANY:

                    if coll_name in cls.collections:

                        project_query = {}
                        for i in fields:
                            project_query[i] = f"$_id.{i}"

                        query = [
                            {"$group": {"_id": relation[coll_name]}},
                            {"$project": project_query},
                        ]

                        coll = db[coll_name]

                        docs = coll.aggregate(query)

                        data = list(docs)

                    else:

                        project_query = {}
                        project_query["_id"] = 0
                        for i in fields:
                            if i.split("_")[0] == coll_name:
                                project_query[i] = "$_id"
                            else:
                                project_query[i] = f"${i}"

                        query = [
                            {"$unwind": {"path": f"${coll_name}"}},
                            {"$group": {"_id": f"${coll_name}"}},
                            {"$sort": {"_id": 1}},
                            {"$project": project_query},
                        ]

                        coll = db[parent_field]

                        docs = coll.aggregate(query)

                        datas = list(docs)

                        data = []

                        for i in range(0, len(datas)):
                            value = datas[i]
                            value["id"] = i + 1
                            data.append(value)

                else:

                    if coll_name in cls.collections:

                        project_query = {}
                        for i in fields:
                            project_query[i] = f"$_id.{i}"

                        query = [
                            {"$group": {"_id": relation[coll_name]}},
                            {"$project": project_query},
                        ]

                        coll = db[coll_name]

                        docs = coll.aggregate(query)

                        data = list(docs)

            else:

                if cardinality_type == CardinalitiesType.ONE_TO_MANY:

                    project_query = {}
                    project_query["_id"] = 0
                    for i in fields:
                        field = i.split(".")
                        if len(field) > 1:
                            coll = field[0]
                            field_name = field[-1].replace(f"{coll}_", "")
                            project_query[f"{coll}_{field_name}"] = (
                                f'${f"{coll}.{field_name}"}'
                            )
                        else:
                            project_query[i] = f"${i}"

                    query = [{"$project": project_query}]

                    coll = db[coll_name]

                    docs = coll.aggregate(query)

                    data = list(docs)

                else:

                    project_query = {}
                    for i in fields:
                        project_query[i] = f"$_id.{i}"

                    query = [
                        {"$group": {"_id": relation[coll_name]}},
                        {"$project": project_query},
                    ]

                    coll = db[coll_name]

                    docs = coll.aggregate(query)

                    data = list(docs)

        return data
