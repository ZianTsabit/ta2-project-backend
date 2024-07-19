import bson

PYMONGO_TYPE_TO_TYPE_STRING = {
    list: "ARRAY",
    dict: "OBJECT",
    type(None): "null",
    bool: "boolean",
    int: "integer",
    bson.int64.Int64: "biginteger",
    float: "float",
    str: "string",
    bson.datetime.datetime: "date",
    bson.timestamp.Timestamp: "timestamp",
    bson.dbref.DBRef: "dbref",
    bson.objectid.ObjectId: "oid",
}

MONGO_TO_PSQL_TYPE = {
    'boolean': 'BOOLEAN',
    'integer': 'INT',
    'biginteger': 'BIGINT',
    'float': 'REAL',
    'number': 'DOUBLE PRECISION',
    'date': 'TIMESTAMP',
    'string': 'TEXT',
    'oid': 'TEXT',
    'dbref': 'TEXT'
}