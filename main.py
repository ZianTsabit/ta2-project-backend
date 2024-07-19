from flask_cors import CORS
from pymongo import MongoClient
from psycopg2 import OperationalError
from flask import Flask, request, jsonify
from pymongo.errors import ServerSelectionTimeoutError

import psycopg2

app = Flask(__name__)
CORS(app)

def mongo_test_connection(uri, database):
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=2000)
        db = client[database]
        db.command("ping")
        return {"success": True, "message": "connection success"}
    except ServerSelectionTimeoutError:
        return {"success": False, "message": "connection failed"}
    except Exception as e:
        return {"success": False, "message": "connection failed"}

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

@app.route('/mongo-test-connection', methods=['POST'])
def mongo_test_connection_route():
    data = request.json
    uri = data.get('uri')
    database = data.get('database')
    result = mongo_test_connection(uri, database)
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

# TODO : Schema Generation From MongoDB to PostreSQL
# TODO : Instantiate the Schema in PostgreSQL Destination
# TODO : ETL Job Start from the table that has no reference other table 

if __name__ == '__main__':
    app.run(port=9000)