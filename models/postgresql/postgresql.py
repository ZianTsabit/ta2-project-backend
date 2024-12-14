import logging
from datetime import datetime
from typing import Dict, List

import psycopg2
from bson import ObjectId
from psycopg2 import OperationalError

from models.mongodb.cardinalities import Cardinalities
from models.mongodb.mongodb import MongoDB
from models.postgresql.attribute import Attribute
from models.postgresql.relation import Relation
from models.rdbms.rdbms import Rdbms
from models.type import CardinalitiesType, MongoType, PsqlType


class PostgreSQL(Rdbms):
    engine: str = "postgresql"
    relations: Dict = {}

    def create_engine_url(cls) -> str:

        return f"postgresql+psycopg2://{cls.username}:{cls.password}@{cls.host}:{cls.port}/{cls.db}"

    def create_connection(cls):
        connection = psycopg2.connect(
            host=cls.host,
            port=cls.port,
            database=cls.db,
            user=cls.username,
            password=cls.password,
        )

        return connection

    def test_connection(cls) -> bool:
        connection = None
        try:
            connection = cls.create_connection()
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            connection.close()
            return True

        except OperationalError as e:
            print(e)
            return False

    def execute_query(cls, query: str) -> bool:
        logging.info(query)
        try:
            conn = cls.create_connection()
            with conn.cursor() as cursor:
                queries = [q.strip() for q in query.split(";") if q.strip()]
                for q in queries:
                    print(f"Executing: {q};")
                    cursor.execute(q)
            conn.commit()
            return True
        except OperationalError as e:
            print(f"OperationalError: {e}")
            return False
        except Exception as e:
            print(f"An error occurred: {e}")
            return False

    def process_collection(cls, mongo: MongoDB, collections: dict):

        collection_names = list(collections.keys())

        for coll in collection_names:

            if coll not in cls.relations:

                new_rel = Relation(name=coll)

                primary_key = mongo.get_primary_key(coll)

                for f in collections[coll]:
                    attr = Attribute(
                        name=f.name,
                        data_type=cls.data_type_mapping(f.data_type),
                        not_null=f.not_null,
                        unique=f.unique,
                    )

                    if primary_key is not None and attr.name == primary_key:
                        new_rel.primary_key = attr

                    new_rel.attributes.append(attr)

                if primary_key is None:
                    primary_key_attr = Attribute(
                        name="id", data_type=PsqlType.SERIAL, not_null=True, unique=True
                    )
                    new_rel.primary_key = primary_key_attr
                    new_rel.attributes.append(primary_key_attr)

                cls.relations[new_rel.name] = new_rel

    def process_mapping_cardinalities(
        cls, mongo: MongoDB, collections: dict, cardinalities: List[Cardinalities]
    ):

        res = {}

        for c in cardinalities:

            cardinality = c.dict()

            source_coll = cardinality["source"]
            source = collections[source_coll]

            dest_coll = cardinality["destination"]

            dest = None
            if dest_coll in collections:
                dest = collections[dest_coll]

            cardinality_type = cardinality["type"]

            source_rel = Relation(name=source_coll)

            dest_rel = Relation(name=dest_coll)

            if cardinality_type == CardinalitiesType.ONE_TO_ONE:

                primary_key_source = mongo.get_primary_key(source_coll)

                for f in source:

                    if f.name != dest_coll:

                        attr = Attribute(
                            name=f.name,
                            data_type=cls.data_type_mapping(f.data_type),
                            not_null=f.not_null,
                            unique=f.unique,
                        )

                        if (
                            primary_key_source is not None
                            and attr.name == primary_key_source
                        ):

                            source_rel.primary_key = attr

                        source_rel.attributes.append(attr)

                    elif f.name == dest_coll and f.data_type == "object":
                        dest_rel.attributes.append(
                            Attribute(
                                name=f"{source_rel.name}.{source_rel.name}_{source_rel.primary_key.name}",
                                data_type=source_rel.primary_key.data_type,
                                not_null=source_rel.primary_key.not_null,
                                unique=source_rel.primary_key.unique,
                            )
                        )

                        dest_rel.foreign_key.append(
                            Attribute(
                                name=f"{source_rel.name}.{source_rel.name}_{source_rel.primary_key.name}",
                                data_type=source_rel.primary_key.data_type,
                                not_null=source_rel.primary_key.not_null,
                                unique=source_rel.primary_key.unique,
                            )
                        )

                if primary_key_source is None:

                    primary_key_attr = Attribute(
                        name="id", data_type=PsqlType.SERIAL, not_null=True, unique=True
                    )
                    source_rel.primary_key = primary_key_attr
                    source_rel.attributes.append(primary_key_attr)

                primary_key_dest = mongo.get_primary_key(dest_coll)

                check_key_source = mongo.check_key_in_other_collection(
                    source_rel.primary_key.name, source_coll
                )

                for f in dest:

                    psql_data_type = cls.data_type_mapping(f.data_type)

                    if f.name != source_coll and psql_data_type is not None:

                        attr = Attribute(
                            name=f.name,
                            data_type=cls.data_type_mapping(f.data_type),
                            not_null=f.not_null,
                            unique=f.unique,
                        )

                        if (
                            primary_key_dest is not None
                            and attr.name == primary_key_dest
                        ):
                            dest_rel.primary_key = attr

                        if (
                            check_key_source["status"] is True
                            and check_key_source["field"] == attr.name
                        ):

                            foreign_key_attr = Attribute(
                                name=f"{source_coll}.{attr.name}",
                                data_type=attr.data_type,
                                not_null=attr.not_null,
                                unique=attr.unique,
                            )

                            dest_rel.foreign_key.append(foreign_key_attr)

                        dest_rel.attributes.append(attr)

                if primary_key_dest is None:

                    primary_key_attr = Attribute(
                        name="id", data_type=PsqlType.SERIAL, not_null=True, unique=True
                    )
                    dest_rel.primary_key = primary_key_attr
                    dest_rel.attributes.append(primary_key_attr)

                if primary_key_dest is not None and dest_rel.primary_key is None:

                    primary_key_attr = Attribute(
                        name=f"{primary_key_dest}",
                        data_type=PsqlType.NULL,
                        not_null=True,
                        unique=True,
                    )
                    dest_rel.primary_key = primary_key_attr

            elif cardinality_type == CardinalitiesType.ONE_TO_MANY:

                primary_key_source = mongo.get_primary_key(source_coll)

                if primary_key_source is None:
                    primary_key_attr = Attribute(
                        name="id", data_type=PsqlType.SERIAL, not_null=True, unique=True
                    )
                    source_rel.primary_key = primary_key_attr
                    source_rel.attributes.append(primary_key_attr)

                elif len(primary_key_source.split(",")) > 1:
                    primary_key_attr = Attribute(
                        name=primary_key_source,
                        data_type=PsqlType.NULL,
                        not_null=False,
                        unique=True,
                    )
                    source_rel.primary_key = primary_key_attr

                for f in source:

                    data_type = f.data_type
                    psql_data_type = cls.data_type_mapping(data_type)

                    if f.name != dest_coll and psql_data_type is not None:

                        attr = Attribute(
                            name=f.name,
                            data_type=psql_data_type,
                            not_null=f.not_null,
                            unique=f.unique,
                        )

                        if (
                            primary_key_source is not None
                            and attr.name == primary_key_source
                        ):
                            source_rel.primary_key = attr

                        source_rel.attributes.append(attr)

                    elif (
                        f.name == dest_coll
                        and f.data_type.split(".")[0] == "array"
                        and dest is None
                    ):

                        dest_rel.attributes.append(
                            Attribute(
                                name=f"{source_rel.name}.{source_rel.name}_{source_rel.primary_key.name}",
                                data_type=source_rel.primary_key.data_type,
                                not_null=source_rel.primary_key.not_null,
                                unique=False,
                            )
                        )

                        dest_rel.attributes.append(
                            Attribute(
                                name=f"{dest_rel.name}_value",
                                data_type=cls.data_type_mapping(
                                    f.data_type.split(".")[1]
                                ),
                                not_null=f.not_null,
                                unique=f.unique,
                            )
                        )

                        dest_rel.primary_key = Attribute(
                            name=f"{source_rel.name}_{source_rel.primary_key.name}, {dest_rel.name}_value",
                            data_type=PsqlType.NULL,
                            not_null=True,
                            unique=True,
                        )

                        dest_rel.foreign_key.append(
                            Attribute(
                                name=f"{source_rel.name}.{source_rel.name}_{source_rel.primary_key.name}",
                                data_type=source_rel.primary_key.data_type,
                                not_null=source_rel.primary_key.not_null,
                                unique=False,
                            )
                        )

                    elif (
                        f.name == dest_coll
                        and f.data_type.split(".")[0] == "array"
                        and dest is not None
                    ):
                        print("test")
                        dest_rel.attributes.append(
                            Attribute(
                                name=f"{source_rel.name}.{source_rel.name}_{source_rel.primary_key.name}",
                                data_type=source_rel.primary_key.data_type,
                                not_null=source_rel.primary_key.not_null,
                                unique=False,
                            )
                        )

                        dest_rel.foreign_key.append(
                            Attribute(
                                name=f"{source_rel.name}.{source_rel.name}_{source_rel.primary_key.name}",
                                data_type=source_rel.primary_key.data_type,
                                not_null=source_rel.primary_key.not_null,
                                unique=False,
                            )
                        )

                if dest is not None:

                    primary_key_dest = mongo.get_primary_key(dest_coll)

                    check_key_source = {
                        "collection": None,
                        "field": None,
                        "status": False,
                    }

                    if len(primary_key_source.split(",")) == 0:

                        check_key_source = mongo.check_key_in_other_collection(
                            source_rel.primary_key.name, source_coll
                        )

                    for f in dest:

                        psql_data_type = cls.data_type_mapping(f.data_type)

                        if f.name != source_coll and psql_data_type is not None:

                            attr = Attribute(
                                name=f.name,
                                data_type=cls.data_type_mapping(f.data_type),
                                not_null=f.not_null,
                                unique=f.unique,
                            )

                            if (
                                primary_key_dest is not None
                                and attr.name == primary_key_dest
                            ):
                                dest_rel.primary_key = attr

                            if (
                                check_key_source["status"] is True
                                and check_key_source["field"] == attr.name
                            ):

                                foreign_key_attr = Attribute(
                                    name=f"{source_coll}.{attr.name}",
                                    data_type=attr.data_type,
                                    not_null=attr.not_null,
                                    unique=attr.unique,
                                )

                                dest_rel.foreign_key.append(foreign_key_attr)

                            dest_rel.attributes.append(attr)

                    if primary_key_dest is None:

                        primary_key_attr = Attribute(
                            name="id",
                            data_type=PsqlType.SERIAL,
                            not_null=True,
                            unique=True,
                        )
                        dest_rel.primary_key = primary_key_attr
                        dest_rel.attributes.append(primary_key_attr)

                    if (
                        len(dest_rel.foreign_key) < 1
                        and len(primary_key_source.split(",")) == 0
                    ):
                        foreign_key = Attribute(
                            name=f"{source_coll}.{source_coll}_{source_rel.primary_key.name}",
                            data_type=source_rel.primary_key.data_type,
                            not_null=source_rel.primary_key.not_null,
                            unique=False,
                        )

                        dest_rel.attributes.append(foreign_key)

                        dest_rel.foreign_key.append(foreign_key)

                    else:

                        primary_keys = (
                            primary_key_source.replace("(", "")
                            .replace(")", "")
                            .split(",")
                        )

                        if len(primary_keys) > 1:

                            frg_key = frg_key = ",".join(
                                [f"{source_coll}_{key}" for key in primary_keys]
                            )

                            for key in primary_keys:

                                field = mongo.get_field(key, source_coll)

                                attr = Attribute(
                                    name=f"{source_coll}.{source_coll}_{key}",
                                    data_type=cls.data_type_mapping(field.data_type),
                                    not_null=field.not_null,
                                    unique=False,
                                )

                                dest_rel.attributes.append(attr)

                            foreign_key = Attribute(
                                name=f"{source_coll}.({frg_key})",
                                data_type=cls.data_type_mapping(field.data_type),
                                not_null=field.not_null,
                                unique=False,
                            )

                            dest_rel.foreign_key.append(foreign_key)

                        else:

                            check_key = mongo.check_key_in_other_collection(
                                primary_keys[0], source_coll
                            )

                            if (
                                check_key["status"] is True
                                and check_key["collection"] == dest_coll
                            ):

                                foreign_key = Attribute(
                                    name=f"{source_coll}.{check_key['field']}",
                                    data_type=PsqlType.NULL,
                                    not_null=True,
                                    unique=False,
                                )

                                dest_rel.foreign_key.append(foreign_key)

            elif cardinality_type == CardinalitiesType.MANY_TO_MANY:

                primary_key_source = mongo.get_primary_key(source_coll)

                for f in source:

                    psql_data_type = cls.data_type_mapping(f.data_type)

                    if primary_key_source is None:
                        primary_key_attr = Attribute(
                            name="id",
                            data_type=PsqlType.SERIAL,
                            not_null=True,
                            unique=True,
                        )

                        source_rel.primary_key = primary_key_attr
                        source_rel.attributes.append(primary_key_attr)

                    if f.name != dest_coll and psql_data_type is not None:

                        attr = Attribute(
                            name=f.name,
                            data_type=cls.data_type_mapping(f.data_type),
                            not_null=f.not_null,
                            unique=f.unique,
                        )

                        if (
                            primary_key_source is not None
                            and attr.name == primary_key_source
                        ):
                            source_rel.primary_key = attr

                        source_rel.attributes.append(attr)

                    elif (
                        f.name == dest_coll
                        and f.data_type.split(".")[0] == "array"
                        and dest is None
                    ):

                        dest_primary_key_attr = Attribute(
                            name="id",
                            data_type=PsqlType.SERIAL,
                            not_null=True,
                            unique=True,
                        )

                        dest_rel.primary_key = dest_primary_key_attr
                        dest_rel.attributes.append(dest_primary_key_attr)

                        dest_rel.attributes.append(
                            Attribute(
                                name=f"{dest_rel.name}_value",
                                data_type=cls.data_type_mapping(
                                    f.data_type.split(".")[1]
                                ),
                                not_null=f.not_null,
                                unique=f.unique,
                            )
                        )

                if dest is not None:

                    primary_key_dest = mongo.get_primary_key(dest_coll)

                    if primary_key_dest is None:
                        primary_key_attr = Attribute(
                            name="id",
                            data_type=PsqlType.SERIAL,
                            not_null=True,
                            unique=True,
                        )
                        dest_rel.primary_key = primary_key_attr
                        dest_rel.attributes.append(primary_key_attr)

                    for f in dest:
                        psql_data_type = cls.data_type_mapping(f.data_type)
                        if f.name != source_coll and psql_data_type is not None:

                            attr = Attribute(
                                name=f.name,
                                data_type=cls.data_type_mapping(f.data_type),
                                not_null=f.not_null,
                                unique=f.unique,
                            )

                            if (
                                primary_key_dest is not None
                                and attr.name == primary_key_dest
                            ):
                                dest_rel.primary_key = attr

                            dest_rel.attributes.append(attr)

                new_relation = Relation(name=f"{source_coll}_{dest_coll}")

                new_relation.attributes.append(
                    Attribute(
                        name=f"{source_rel.name}.{source_rel.name}_{source_rel.primary_key.name}",
                        data_type=source_rel.primary_key.data_type,
                        not_null=source_rel.primary_key.not_null,
                        unique=False,
                    )
                )

                new_relation.attributes.append(
                    Attribute(
                        name=f"{dest_rel.name}.{dest_rel.name}_{dest_rel.primary_key.name}",
                        data_type=dest_rel.primary_key.data_type,
                        not_null=dest_rel.primary_key.not_null,
                        unique=False,
                    )
                )

                new_relation.primary_key = Attribute(
                    name=f"{source_rel.name}_{source_rel.primary_key.name}, {dest_rel.name}_{dest_rel.primary_key.name}",
                    data_type=PsqlType.NULL,
                    not_null=True,
                    unique=True,
                )

                new_relation.foreign_key.append(
                    Attribute(
                        name=f"{dest_rel.name}.{dest_rel.name}_{dest_rel.primary_key.name}",
                        data_type=dest_rel.primary_key.data_type,
                        not_null=dest_rel.primary_key.not_null,
                        unique=dest_rel.primary_key.unique,
                    )
                )

                new_relation.foreign_key.append(
                    Attribute(
                        name=f"{source_rel.name}.{source_rel.name}_{source_rel.primary_key.name}",
                        data_type=source_rel.primary_key.data_type,
                        not_null=source_rel.primary_key.not_null,
                        unique=source_rel.primary_key.unique,
                    )
                )

                res[new_relation.name] = new_relation

            res[source_rel.name] = source_rel
            res[dest_rel.name] = dest_rel

        cls.relations = res

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

        if mongo_type in mapping:
            return mapping.get(mongo_type)

        return None

    def generate_ddl(cls, schema):

        ddl_statements = []
        foreign_key_statements = []

        for table_name, table in schema.items():
            if not table["foreign_key"]:
                ddl_statements.append(cls.create_table_ddl(table)[0])
                foreign_key_statements.append(cls.create_table_ddl(table)[1])
            else:
                ddl_statements.append(cls.create_table_ddl(table)[0])
                foreign_key_statements.append(cls.create_table_ddl(table)[1])

        ddl_statements.extend(foreign_key_statements)

        return "\n\n".join(ddl_statements)

    def create_table_ddl(cls, table: dict):

        relations = cls.relations

        ddl_create_table = f'CREATE TABLE {table["name"]} (\n'

        columns = []
        for attr in table["attributes"]:
            column_line = f'    {attr["name"].split(".")[-1]} {attr["data_type"]}'
            if attr["not_null"]:
                column_line += " NOT NULL"
            if attr["unique"]:
                column_line += " UNIQUE"
            columns.append(column_line)

        primary_key = table.get("primary_key")
        if primary_key:
            pk_cols = primary_key["name"]
            columns.append(f"    PRIMARY KEY ({pk_cols})")

        ddl_create_table += ",\n".join(columns) + "\n"
        ddl_create_table += ");"

        ddl_alter_table = ""

        for fk in table.get("foreign_key", []):

            primary_key = relations[fk["name"].split(".")[0]].primary_key.name

            if len(primary_key.split(",")) < 2:
                ddl_alter_table += f'\nALTER TABLE {table["name"]} ADD CONSTRAINT fk_{table["name"]}_{fk["name"].split(".")[1].replace("(","").replace(")","")}\n'
                ddl_alter_table += f'    FOREIGN KEY ({fk["name"].split(".")[1].replace("(","").replace(")","")}) REFERENCES {fk["name"].split(".")[0]}({relations[fk["name"].split(".")[0]].primary_key.name});'
            else:
                coll = fk["name"].split(".")[0]
                key = fk["name"].split(".")[1].replace(f"{coll}_", "")
                ddl_alter_table += f'\nALTER TABLE {table["name"]} ADD CONSTRAINT fk_{table["name"]}_{key.replace("(","").replace(")","").replace(",","_")}\n'
                ddl_alter_table += f'    FOREIGN KEY {fk["name"].split(".")[1]} REFERENCES {fk["name"].split(".")[0]}{key};'

        return ddl_create_table, ddl_alter_table

    def insert_data_by_relation(
        cls, mongodb: MongoDB, cardinalities: List[Cardinalities]
    ):

        schema = {k: v.to_dict() for k, v in cls.relations.items()}

        def find_dependencies(table_name):
            dependencies = []
            for table, data in schema.items():
                for fk in data.get("foreign_key", []):
                    if fk["name"].startswith(table_name):
                        dependencies.append(table)
            return dependencies

        def get_table_creation_order():

            all_tables = list(schema.keys())
            creation_order = []

            while all_tables:
                for table in all_tables:

                    dependencies = find_dependencies(table)
                    if not dependencies or all(
                        dep in creation_order for dep in dependencies
                    ):
                        creation_order.append(table)
                        all_tables.remove(table)
                        break

            return creation_order[::-1]

        def convert_to_timestamp(value):
            if isinstance(value, datetime):
                return str(datetime.fromtimestamp(value.timestamp()))
            return value

        creation_order = get_table_creation_order()

        for i in creation_order:

            relation = cls.relations[i]
            res = {}
            res[relation.name] = {}

            for attr in relation.attributes:
                res[relation.name][f"{attr.name}"] = f"${attr.name}"

            cardinality_type = None
            for card in cardinalities:
                if card.destination == relation.name:
                    field = mongodb.get_field(relation.name, card.source)
                    if field:
                        cardinality_type = card.type
            print(res)
            datas = mongodb.get_data_by_collection(res, cardinality_type)

            for data in datas:

                transformed_data = {
                    key: (
                        str(value)
                        if isinstance(value, ObjectId)
                        else convert_to_timestamp(value)
                    )
                    for key, value in data.items()
                }

                columns = ", ".join(transformed_data.keys())
                values = ", ".join(
                    [
                        f"'{value}'" if isinstance(value, str) else str(value)
                        for value in transformed_data.values()
                    ]
                )

                insert_query = (
                    f"INSERT INTO {relation.name} ({columns}) VALUES ({values});"
                )

                cls.execute_query(insert_query)

        return True
