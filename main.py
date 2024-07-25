import psycopg2

from flask_cors import CORS
from pymongo import MongoClient
from psycopg2 import OperationalError
from flask import Flask, request, jsonify
from pymongo.errors import ServerSelectionTimeoutError

from generate_schema import *
from generate_ddl import *

app = Flask(__name__)
CORS(app)

def mongo_test_connection(host, port, database, user, password):
    client = None
    try:
        client = MongoClient(host=host, port=int(port), username=user, password=password, serverSelectionTimeoutMS=5000)
        db = client[database]
        db.command("ping")
        return {"success": True, "message": "connection success"}
    except ServerSelectionTimeoutError:
        return {"success": False, "message": "connection failed"}
    except Exception as e:
        return {"success": False, "message": str(e)}
    finally:
        if client:
            client.close()

def postgre_test_connection(host, port, database, user, password):
    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        connection.close()
        return {"success": True, "message": "connection success"}
    except OperationalError as e:
        return {"success": False, "message": "connection failed"}

# TODO : ETL Job Start from the table that has no reference other table

def generate_schema_from_mongo_to_postgres(
        mongoHost, 
        mongoPort, 
        mongoDatabase, 
        mongoUser, 
        mongoPassword,
        postgreHost,
        postgrePort,
        postgreDatabase,
        postgreUser,
        postgrePassword):

    try:

        basic_schema = generate_basic_schema(host=mongoHost, 
                                             port=int(mongoPort), 
                                             username=mongoUser, 
                                             password=mongoPassword, 
                                             db_name=mongoDatabase)

        basic_schema_with_foreign_key = find_foreign_keys(host=mongoHost, 
                                                          port=int(mongoPort), 
                                                          username=mongoUser, 
                                                          password=mongoPassword, 
                                                          db_name=mongoDatabase, 
                                                          basic_schema=basic_schema)

        final_schema = generate_final_schema(tables=basic_schema_with_foreign_key)

        postgreConn = psycopg2.connect(
            host=postgreHost,
            port=postgrePort,
            database=postgreDatabase,
            user=postgreUser,
            password=postgrePassword
        )
        
        postgre_ddl = generate_ddl(final_schema)

        with postgreConn.cursor() as cursor:
            cursor.execute(postgre_ddl)
            postgreConn.commit()

        return {"success": True, "message": "Schema creation successful"}
    
    except OperationalError as e:
        return {"success": False, "message": f"Database operation failed: {e}"}
    
    except Exception as e:
        return {"success": False, "message": f"An error occurred: {e}"}

@app.route('/mongo-test-connection', methods=['POST'])
def mongo_test_connection_route():
    data = request.json
    host = data.get('host')
    port = data.get('port')
    database = data.get('database')
    user = data.get('user')
    password = data.get('password')
    result = mongo_test_connection(host=host, port=port, database=database, user=user, password=password)
    return jsonify(result)

@app.route('/postgre-test-connection', methods=['POST'])
def postgre_test_connection_route():
    data = request.json
    host = data.get('host')
    port = data.get('port')
    database = data.get('database')
    user = data.get('user')
    password = data.get('password')
    result = postgre_test_connection(host, port, database, user, password)
    return jsonify(result)

@app.route('/generate-schema-from-mongo-to-postgres', methods=['POST'])
def generate_schema_from_mongo_to_postgres_route():
    data = request.json
    mongoHost = data.get('mongo_host')
    mongoPort = data.get('mongo_port')
    mongoDatabase = data.get('mongo_database')
    mongoUser = data.get('mongo_user')
    mongoPassword = data.get('mongo_password')
    postgreHost = data.get('postgre_host')
    postgrePort = data.get('postgre_port')
    postgreDatabase = data.get('postgre_database')
    postgreUser = data.get('postgre_user')
    postgrePassword = data.get('postgre_password')

    result = generate_schema_from_mongo_to_postgres(
        mongoHost, 
        mongoPort, 
        mongoDatabase, 
        mongoUser, 
        mongoPassword,
        postgreHost,
        postgrePort,
        postgreDatabase,
        postgreUser,
        postgrePassword)

    return jsonify(result)

if __name__ == '__main__':
    app.run(port=8000)