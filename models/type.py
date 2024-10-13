from enum import Enum


class MongoType(str, Enum):
    BOOL="boolean"
    INTEGER="integer"
    BIG_INT="biginteger"
    FLOAT="float"
    NUM="number"
    DATE="date"
    STRING="string"
    OID="oid"
    DB_REF="dbref"

class PsqlType(str, Enum):
    BOOL="BOOLEAN"
    INTEGER="INT"
    BIG_INT="BIGINT"
    FLOAT="REAL"
    NUM="DOUBLE PRECISION"
    DATE="TIMESTAMP"
    STRING="TEXT"
    OID="TEXT"
    DB_REF="TEXT"