from typing import List

from models.mongodb.cardinalities import Cardinalities
from models.mongodb.collection import Collection
from models.rdbms.attribute import Attribute
from models.rdbms.rdbms import Rdbms
from models.rdbms.relation import Relation
from models.type import MongoType, PsqlType

MONGO_TO_PSQL_TYPE = {
    'boolean': 'BOOLEAN',
    'integer': 'INT',
    'biginteger': 'BIGINT',
    'float': 'REAL',
    'number': 'DOUBLE PRECISION',
    'date': 'TIMESTAMP',
    'string': 'TEXT',
    'oid': 'TEXT',
    'dbref': 'TEXT'
}


class PostgreSQL(Rdbms):
    engine: str = "postgresql"
    relations: List[Relation] = []

    def process_collection(cls, collection: Collection):
        pass

    def process_mapping_cardinalities(cls, cardinalities: List[Cardinalities]):
        pass

    def data_type_mapping(cls, mongo_type: MongoType) -> PsqlType:

        mapping = {
            MongoType.NULL: PsqlType.NULL,
            MongoType.BOOL: PsqlType.BOOL,
            MongoType.INTEGER: PsqlType.INTEGER,
            MongoType.BIG_INT: PsqlType.BIG_INT,
            MongoType.FLOAT: PsqlType.FLOAT,
            MongoType.NUM: PsqlType.NUM,
            MongoType.DATE: PsqlType.DATE,
            MongoType.STRING: PsqlType.STRING,
            MongoType.OID: PsqlType.OID,
            MongoType.DB_REF: PsqlType.DB_REF,
        }

        return mapping.get(mongo_type)

    def generate_ddl(cls) -> str:
        ddl_statements = []

        tables_with_fk = {}
        tables_without_fk = {}

        for table_name, attributes in cls.relations.items():
            if "foreign_keys" in attributes and attributes["foreign_keys"]:
                tables_with_fk[table_name] = attributes
            else:
                tables_without_fk[table_name] = attributes

        for table_name, attributes in tables_without_fk.items():
            ddl_statements.append(cls.create_table_ddl(table_name, attributes))

        for table_name, attributes in tables_with_fk.items():
            ddl_statements.append(cls.create_table_ddl(table_name, attributes))

        return "\n\n".join(ddl_statements)

    def create_table_ddl(cls, table: Relation, attributes: Attribute) -> str:
        columns = []
        primary_key = None
        unique_columns = []
        foreign_keys = []

        for column, data_type in attributes.items():
            if column == "foreign_keys":
                continue
            if data_type in MONGO_TO_PSQL_TYPE:
                column_definition = f"{column} {MONGO_TO_PSQL_TYPE[data_type]}"
                columns.append(column_definition)

                if column == "_id":
                    primary_key = column
                    unique_columns.append(column)
                elif (data_type == 'oid' and column not in attributes.get("foreign_keys", {})):
                    unique_columns.append(column)

            else:
                raise ValueError(f"Unsupported data type: {data_type}")

        foreign_keys_attributes = attributes.get("foreign_keys", {})
        for fk_column, ref in foreign_keys_attributes.items():
            ref_table, ref_column = ref.split(".")
            foreign_keys.append(
                f'''FOREIGN KEY ({fk_column})
                REFERENCES {ref_table} ({ref_column})'''
            )

        table_definition = f"CREATE TABLE {table} (\n"
        table_definition += ",\n".join(columns)

        if primary_key:
            table_definition += f",\nPRIMARY KEY ({primary_key})"

        if unique_columns:
            unique_constraints = ", ".join(unique_columns)
            table_definition += f",\nUNIQUE ({unique_constraints})"

        if foreign_keys:
            table_definition += ",\n"
            table_definition += ",\n".join(foreign_keys)

        table_definition += "\n);"

        return table_definition
