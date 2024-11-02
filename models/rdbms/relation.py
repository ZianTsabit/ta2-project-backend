from typing import List

from pydantic import BaseModel

from models.rdbms.attribute import Attribute


class Relation(BaseModel):
    name: str
    attributes: List[Attribute]
    primary_key: Attribute
    foreign_key: Attribute

# TODO: if primary key not found, add default id with auto generate or autoincrement and the data type is int
# TODO: when generate ddl please process relation that has no foreign key first
# TODO: when the relation is many-to-many please create one relation
