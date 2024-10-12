import json


def generate_ddl(tables):
    ddl_statements = []

    tables_with_fk = {}
    tables_without_fk = {}

    for table_name, attributes in tables.items():
        if "foreign_keys" in attributes and attributes["foreign_keys"]:
            tables_with_fk[table_name] = attributes
        else:
            tables_without_fk[table_name] = attributes

    for table_name, attributes in tables_without_fk.items():
        ddl_statements.append(create_table_ddl(table_name, attributes))

    for table_name, attributes in tables_with_fk.items():
        ddl_statements.append(create_table_ddl(table_name, attributes))

    return "\n\n".join(ddl_statements)


def create_table_ddl(table_name, attributes):
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
            elif (data_type == 'oid' and
                    column not in attributes.get("foreign_keys", {})):
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

    table_definition = f"CREATE TABLE {table_name} (\n"
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


def read_json_file(file_path: str):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data


def write_ddl_to_file(ddl_script, file_path):
    with open(file_path, 'w') as file:
        file.write(ddl_script)
