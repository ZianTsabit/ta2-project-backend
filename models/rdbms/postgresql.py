from typing import Dict, List

from models.mongodb.cardinalities import Cardinalities
from models.mongodb.mongodb import MongoDB
from models.rdbms.attribute import Attribute
from models.rdbms.rdbms import Rdbms
from models.rdbms.relation import Relation
from models.type import CardinalitiesType, MongoType, PsqlType


class PostgreSQL(Rdbms):
    engine: str = "postgresql"
    relations: Dict = {}

    def process_collection(cls, mongo: MongoDB, collections: dict):

        collection_names = list(collections.keys())

        for coll in collection_names:

            if coll not in cls.relations():

                new_rel = Relation(
                    name=coll
                )

                primary_key = mongo.get_primary_key(coll)

                for f in collections[coll]:
                    attr = Attribute(
                        name=f.name,
                        data_type=cls.data_type_mapping(f.data_type),
                        not_null=f.not_null,
                        unique=f.unique
                    )

                    if primary_key is not None and attr.name == primary_key:
                        new_rel.primary_key = attr

                    new_rel.attributes.append(attr)

                if primary_key is None:
                    primary_key_attr = Attribute(
                        name="id",
                        data_type=PsqlType.SERIAL,
                        not_null=True,
                        unique=True
                    )
                    new_rel.primary_key = primary_key_attr
                    new_rel.attributes.append(primary_key_attr)

                cls.relations[new_rel.name] = new_rel

    def process_mapping_cardinalities(cls, mongo: MongoDB, collections: dict, cardinalities: List[Cardinalities]):

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

                primary_key_source = mongo.get_primary_key(source_coll)

                for f in source:

                    if f.name != dest_coll:

                        attr = Attribute(
                            name=f.name,
                            data_type=cls.data_type_mapping(f.data_type),
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

                check_key_source = mongo.check_key_in_other_collection(source_rel.primary_key.name, source_coll)

                for f in dest:
                    attr = Attribute(
                        name=f.name,
                        data_type=cls.data_type_mapping(f.data_type),
                        not_null=f.not_null,
                        unique=f.unique
                    )

                    if primary_key_dest is not None and attr.name == primary_key_dest:
                        dest_rel.primary_key = attr

                    if check_key_source["status"] is True and check_key_source["field"] == attr.name:

                        foreign_key_attr = Attribute(
                            name=f'{source_coll}.{attr.name}',
                            data_type=attr.data_type,
                            not_null=attr.not_null,
                            unique=attr.unique
                        )

                        dest_rel.foreign_key.append(foreign_key_attr)

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

            elif cardinality_type == CardinalitiesType.ONE_TO_MANY:

                primary_key_source = mongo.get_primary_key(source_coll)

                for f in source:

                    psql_data_type = cls.data_type_mapping(f.data_type)
                    if f.name != dest_coll and psql_data_type is not None:

                        attr = Attribute(
                            name=f.name,
                            data_type=cls.data_type_mapping(f.data_type),
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

                    psql_data_type = cls.data_type_mapping(f.data_type)
                    if f.name != source_coll and psql_data_type is not None:

                        attr = Attribute(
                            name=f.name,
                            data_type=cls.data_type_mapping(f.data_type),
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

                dest_rel.foreign_key.append(foreign_key)

            elif cardinality_type == CardinalitiesType.MANY_TO_MANY:

                primary_key_source = mongo.get_primary_key(source_coll)

                for f in source:
                    psql_data_type = cls.data_type_mapping(f.data_type)
                    if f.name != dest_coll and psql_data_type is not None:

                        attr = Attribute(
                            name=f.name,
                            data_type=cls.data_type_mapping(f.data_type),
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
                    psql_data_type = cls.data_type_mapping(f.data_type)
                    if f.name != source_coll and psql_data_type is not None:

                        attr = Attribute(
                            name=f.name,
                            data_type=cls.data_type_mapping(f.data_type),
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

                new_relation = Relation(
                    name=f"{source_coll}_{dest_coll}"
                )
                new_relation.attributes.append(
                    Attribute(
                        name=f"{source_rel.name}.{source_rel.primary_key.name}",
                        data_type=source_rel.primary_key.data_type,
                        not_null=source_rel.primary_key.not_null,
                        unique=source_rel.primary_key.unique
                    )
                )

                new_relation.attributes.append(
                    Attribute(
                        name=f"{dest_rel.name}.{dest_rel.primary_key.name}",
                        data_type=dest_rel.primary_key.data_type,
                        not_null=dest_rel.primary_key.not_null,
                        unique=dest_rel.primary_key.unique
                    )
                )

                new_relation.primary_key = Attribute(
                    name=f"{source_rel.name}.{source_rel.primary_key.name}, {dest_rel.name}.{dest_rel.primary_key.name}",
                    data_type=PsqlType.NULL,
                    not_null=True,
                    unique=True
                )

                for attr in new_relation.attributes:
                    new_relation.foreign_key.append(attr)

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
                ddl_statements.append(cls.create_table_ddl(table))
            else:
                foreign_key_statements.append(cls.create_table_ddl(table))

        ddl_statements.extend(foreign_key_statements)

        return "\n\n".join(ddl_statements)

    def create_table_ddl(cls, table: dict):

        ddl = f'CREATE TABLE {table["name"]} (\n'

        columns = []
        for attr in table["attributes"]:
            column_line = f'    {attr["name"]} {attr["data_type"]}'
            if attr["not_null"]:
                column_line += " NOT NULL"
            if attr["unique"]:
                column_line += " UNIQUE"
            columns.append(column_line)

        primary_key = table.get("primary_key")
        if primary_key:
            pk_cols = primary_key["name"]
            columns.append(f'    PRIMARY KEY ({pk_cols})')

        ddl += ",\n".join(columns) + "\n"
        ddl += ");"

        for fk in table.get("foreign_key", []):
            ddl += f'\nALTER TABLE {table["name"]} ADD CONSTRAINT fk_{table["name"]}_{fk["name"].replace(".", "_")}\n'
            ddl += f'    FOREIGN KEY ({fk["name"]}) REFERENCES {fk["name"].split(".")[0]}(_id);'

        return ddl
