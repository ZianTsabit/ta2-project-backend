from pydantic import BaseModel

from mongosequelizer.type import PsqlType


class Attribute(BaseModel):
    name: str
    data_type: PsqlType
    not_null: bool
    unique: bool

    def to_dict(cls):
        return {
            "name": cls.name,
            "data_type": cls.data_type.value,
            "not_null": cls.not_null,
            "unique": cls.unique,
        }
