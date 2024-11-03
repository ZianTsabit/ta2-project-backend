import json
from typing import Dict, List

from models.mongodb.cardinalities import Cardinalities
from models.mongodb.collection import Collection
from models.mongodb.mongodb import MongoDB
from models.rdbms.attribute import Attribute
from models.rdbms.rdbms import Rdbms
from models.rdbms.relation import Relation
from models.type import CardinalitiesType, MongoType, PsqlType

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
    relations: Dict = {}

    def process_collection(cls, collection: Collection):
        # TODO: add relation that not exist in cardinalities to cls.relations
        pass

    def process_mapping_cardinalities(cls, cardinalities: List[Cardinalities]):
        # TODO: add relation to cls.relations
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


mongo = MongoDB(
    host='localhost',
    port=27017,
    db='db_school',
    username='root',
    password='rootadmin1234'
)

postgresql = PostgreSQL(
    host='localhost',
    port='5436',
    db='db_univ',
    username='user',
    password='admin#1234'
)

mongo.init_collection()

collections = mongo.get_collections()

cardinalities = mongo.mapping_all_cardinalities()

res = {}

for c in cardinalities:

    cardinality = c.dict()

    source_coll = cardinality["source"]
    source = collections[source_coll]

    dest_coll = cardinality["destination"]
    dest = collections[dest_coll]

    cardinality_type = cardinality["type"]

    source_rel = Relation(
        name=source_coll
    )

    dest_rel = Relation(
        name=dest_coll
    )

    if cardinality_type == CardinalitiesType.ONE_TO_ONE:
        pass

    elif cardinality_type == CardinalitiesType.ONE_TO_MANY:

        primary_key_source = mongo.get_primary_key(source_coll)

        for f in source:

            if f.name != dest_coll:

                attr = Attribute(
                    name=f.name,
                    data_type=postgresql.data_type_mapping(f.data_type),
                    not_null=f.not_null,
                    unique=f.unique
                )

                if primary_key_source is not None and attr.name == primary_key_source:
                    source_rel.primary_key = attr

                source_rel.attributes.append(attr)

        if primary_key_source is None:
            primary_key_attr = Attribute(
                name="id",
                data_type=PsqlType.SERIAL,
                not_null=True,
                unique=True
            )
            source_rel.primary_key = primary_key_attr
            source_rel.attributes.append(primary_key_attr)

        primary_key_dest = mongo.get_primary_key(dest_coll)

        for f in dest:
            attr = Attribute(
                name=f.name,
                data_type=postgresql.data_type_mapping(f.data_type),
                not_null=f.not_null,
                unique=f.unique
            )

            if primary_key_dest is not None and attr.name == primary_key_dest:
                dest_rel.primary_key = attr

            dest_rel.attributes.append(attr)

        if primary_key_dest is None:
            primary_key_attr = Attribute(
                name="id",
                data_type=PsqlType.SERIAL,
                not_null=True,
                unique=True
            )
            dest_rel.primary_key = primary_key_attr
            dest_rel.attributes.append(primary_key_attr)

        foreign_key = Attribute(
            name=f'{source_coll}.{source_rel.primary_key.name}',
            data_type=source_rel.primary_key.data_type,
            not_null=source_rel.primary_key.not_null,
            unique=source_rel.primary_key.unique
        )

        dest_rel.attributes.append(foreign_key)

        dest_rel.foreign_key = foreign_key

    elif cardinality_type == CardinalitiesType.MANY_TO_MANY:
        pass

    # TODO: check duplicate name relation

    res[source_rel.name] = source_rel
    res[dest_rel.name] = dest_rel

postgresql.relations = res

data_dict = {k: v.to_dict() for k, v in postgresql.relations.items()}

with open("relations.json", "w") as file:
    json.dump(data_dict, file)
