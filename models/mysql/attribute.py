from pydantic import BaseModel

from models.type import MySQLType


class Attribute(BaseModel):
    name: str
    data_type: MySQLType
    not_null: bool
    unique: bool

    def to_dict(cls):
        return {
            "name": cls.name,
            "data_type": cls.data_type.value,
            "not_null": cls.not_null,
            "unique": cls.unique
        }
