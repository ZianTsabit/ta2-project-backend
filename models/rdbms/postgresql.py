from typing import List

from models.rdbms.rdbms import Rdbms
from models.rdbms.relation import Relation


class PostgreSQL(Rdbms):
    engine: str = "postgresql"
    relations: List[Relation]
