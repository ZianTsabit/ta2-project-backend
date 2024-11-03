from typing import List

from pydantic import BaseModel

from models.rdbms.attribute import Attribute


class Relation(BaseModel):
    name: str
    attributes: List[Attribute] = []
    primary_key: Attribute = None
    foreign_key: Attribute = None

    def to_dict(cls):
        return {
            "name": cls.name,
            "attributes": [attr.to_dict() for attr in cls.attributes],
            "primary_key": cls.primary_key.to_dict(),
            "foreign_key": cls.foreign_key.to_dict() if cls.foreign_key else None
        }

# TODO: when generate ddl please process relation that has no foreign key first
# TODO: when the relation is many-to-many please create one relation
