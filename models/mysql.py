from typing import List

from models.rdbms import Rdbms
from models.relation import Relation


class MySQL(Rdbms):
    engine: str = "mysql"
    relations: List[Relation]
