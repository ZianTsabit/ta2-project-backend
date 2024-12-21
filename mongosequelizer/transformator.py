from mongosequelizer.mongodb.mongodb import MongoDB
from mongosequelizer.mysql.mysql import MySQL
from mongosequelizer.postgresql.postgresql import PostgreSQL
from mongosequelizer.rdbms.rdbms import Rdbms


class MongoSequelizer:
    def __init__(self, rdbms_type: str, rdbms: Rdbms, mongodb: MongoDB):
        self.rdbms_type = rdbms_type
        self.rdbms = rdbms
        self.mongodb = mongodb

    def generate_ddl(self):
        ddl = ""
        self.mongodb.init_collection()
        collections = self.mongodb.get_collections()
        cardinalities = self.mongodb.mapping_all_cardinalities()

        if self.rdbms_type == "postgresql":
            postgresql = PostgreSQL(
                host=self.rdbms.host,
                port=self.rdbms.port,
                db=self.rdbms.db,
                username=self.rdbms.username,
                password=self.rdbms.password,
            )
            postgresql.process_mapping_cardinalities(
                self.mongodb, collections, cardinalities
            )
            postgresql.process_collection(self.mongodb, collections)

            schema = postgresql.relations["object"]

            ddl = postgresql.generate_ddl(schema)

        elif self.rdbms_type == "mysql":
            mysql = MySQL(
                host=self.rdbms.host,
                port=self.rdbms.port,
                db=self.rdbms.db,
                username=self.rdbms.username,
                password=self.rdbms.password,
            )
            mysql.process_mapping_cardinalities(
                self.mongodb, collections, cardinalities
            )
            mysql.process_collection(self.mongodb, collections)

            schema = mysql.relations["object"]

            ddl = mysql.generate_ddl(schema)

        return ddl

    def implement_ddl(self):
        ddl = ""
        success = False
        self.mongodb.init_collection()
        collections = self.mongodb.get_collections()
        cardinalities = self.mongodb.mapping_all_cardinalities()

        if self.rdbms_type == "postgresql":
            postgresql = PostgreSQL(
                host=self.rdbms.host,
                port=self.rdbms.port,
                db=self.rdbms.db,
                username=self.rdbms.username,
                password=self.rdbms.password,
            )
            postgresql.process_mapping_cardinalities(
                self.mongodb, collections, cardinalities
            )
            postgresql.process_collection(self.mongodb, collections)

            schema = postgresql.relations["object"]

            ddl = postgresql.generate_ddl(schema)

            if ddl != "":
                success = postgresql.execute_query(ddl)

        elif self.rdbms_type == "mysql":
            mysql = MySQL(
                host=self.rdbms.host,
                port=self.rdbms.port,
                db=self.rdbms.db,
                username=self.rdbms.username,
                password=self.rdbms.password,
            )
            mysql.process_mapping_cardinalities(
                self.mongodb, collections, cardinalities
            )
            mysql.process_collection(self.mongodb, collections)

            schema = mysql.relations["object"]

            ddl = mysql.generate_ddl(schema)

            if ddl != "":
                success = mysql.execute_query(ddl)

        return success

    def migrate_data(self):
        success = False
        self.mongodb.init_collection()
        collections = self.mongodb.get_collections()
        cardinalities = self.mongodb.mapping_all_cardinalities()

        if self.rdbms_type == "postgresql":
            postgresql = PostgreSQL(
                host=self.rdbms.host,
                port=self.rdbms.port,
                db=self.rdbms.db,
                username=self.rdbms.username,
                password=self.rdbms.password,
            )
            postgresql.process_mapping_cardinalities(
                self.mongodb, collections, cardinalities
            )
            postgresql.process_collection(self.mongodb, collections)

            success = postgresql.insert_data_by_relation(self.mongodb, cardinalities)

        elif self.rdbms_type == "mysql":
            mysql = MySQL(
                host=self.rdbms.host,
                port=self.rdbms.port,
                db=self.rdbms.db,
                username=self.rdbms.username,
                password=self.rdbms.password,
            )
            mysql.process_mapping_cardinalities(
                self.mongodb, collections, cardinalities
            )
            mysql.process_collection(self.mongodb, collections)

            success = mysql.insert_data_by_relation(self.mongodb, cardinalities)

        return success
