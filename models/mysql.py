from models.rdbms import Rdbms


class MySQL(Rdbms):
    engine: str = "mysql"
