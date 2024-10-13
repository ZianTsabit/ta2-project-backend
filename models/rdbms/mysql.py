from typing import List

from models.rdbms.rdbms import Rdbms
from models.rdbms.relation import Relation


class MySQL(Rdbms):
    engine: str = "mysql"
    relations: List[Relation]
