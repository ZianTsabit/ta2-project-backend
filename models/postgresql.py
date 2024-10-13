from typing import List

from models.rdbms import Rdbms
from models.relation import Relation


class PostgreSQL(Rdbms):
    engine: str = "postgresql"
    relations: List[Relation]
