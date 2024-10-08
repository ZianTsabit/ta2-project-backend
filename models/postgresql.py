from models.rdbms import Rdbms


class PostgreSQL(Rdbms):
    engine: str = "postgresql"
