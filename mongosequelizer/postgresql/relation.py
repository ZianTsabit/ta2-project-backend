from typing import Dict, Optional

from pydantic import BaseModel

from mongosequelizer.postgresql.attribute import Attribute


class AttributeObject(BaseModel):
    object: Dict[str, Attribute]

    def to_dict(cls):
        return {"object": cls.object}


class Relation(BaseModel):
    name: str
    attributes: Optional[AttributeObject] = None
    primary_key: Optional[Attribute] = None
    foreign_key: Optional[AttributeObject] = None

    def to_dict(cls):
        return {
            "name": cls.name,
            "attributes": cls.attributes.to_dict(),
            "primary_key": cls.primary_key.to_dict(),
            "foreign_key": cls.foreign_key.to_dict(),
        }
