from enum import Enum


class MongoType(str, Enum):
    NULL = 'null'
    BOOL = "boolean"
    INTEGER = "integer"
    BIG_INT = "biginteger"
    FLOAT = "float"
    NUM = "number"
    DATE = "date"
    STRING = "string"
    OID = "oid"
    DB_REF = "dbref"
    OBJECT = "object"
    ARRAY_OF_OBJECT = "array.object"
    ARRAY_OF_STRING = "array.string"
    ARRAY_OF_BIG_INT = "array.biginteger"
    ARRAY_OF_FLOAT = "array.float"
    ARRAY_OF_NUM = "array.number"
    ARRAY_OF_DATE = "array.date"
    ARRAY_OF_OID = "array.oid"


class PsqlType(str, Enum):
    NULL = "NULL"
    BOOL = "BOOLEAN"
    INTEGER = "INT"
    BIG_INT = "BIGINT"
    FLOAT = "REAL"
    NUM = "DOUBLE PRECISION"
    DATE = "TIMESTAMP"
    STRING = "TEXT"
    OID = "TEXT"
    DB_REF = "TEXT"


class MySQLType(str, Enum):
    NULL = "NULL"
    BOOL = "BOOLEAN"
    INTEGER = "INT"
    BIG_INT = "BIGINT"
    FLOAT = "FLOAT"
    NUM = "DOUBLE"
    DATE = "DATETIME"
    STRING = "TEXT"
    OID = "TEXT"
    DB_REF = "TEXT"


class CardinalitiesType(str, Enum):
    ONE_TO_ONE = "one-to-one"
    ONE_TO_MANY = "one-to-many"
    MANY_TO_MANY = "many-to-many"
