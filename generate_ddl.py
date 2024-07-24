import json

# data type conversion rule
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

def generate_ddl(tables):
    ddl_statements = []

    for table_name, attributes in tables.items():
        columns = []
        foreign_keys = []
        
        for column, data_type in attributes.items():
            if column == "foreign_keys":
                continue
            if data_type in MONGO_TO_PSQL_TYPE:
                columns.append(f"{column} {MONGO_TO_PSQL_TYPE[data_type]}")
            else:
                raise ValueError(f"Unsupported data type: {data_type}")

        foreign_keys_attributes = attributes.get("foreign_keys", {})
        for fk_column, ref in foreign_keys_attributes.items():
            ref_table, ref_column = ref.split(".")
            foreign_keys.append(f"FOREIGN KEY ({fk_column}) REFERENCES {ref_table} ({ref_column})")
        
        table_definition = f"CREATE TABLE {table_name} (\n"
        table_definition += ",\n".join(columns)
        if foreign_keys:
            table_definition += ",\n"
            table_definition += ",\n".join(foreign_keys)
        table_definition += "\n);"
        
        ddl_statements.append(table_definition)
    
    return "\n\n".join(ddl_statements)