from flask_cors import CORS
from pymongo import MongoClient
from flask import Flask, request, jsonify
import psycopg2
from psycopg2 import OperationalError

app = Flask(__name__)
CORS(app)

def mongo_test_connection(uri, database):
    try:
        client = MongoClient(uri)
        db = client[database]
        # The ping command is used to test the connection to the server
        db.command("ping")
        return {"success": True, "message": "MongoDB connection successful"}
    except Exception as e:
        return {"success": False, "message": str(e)}

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
        return {"success": True, "message": "PostgreSQL connection successful"}
    except OperationalError as e:
        return {"success": False, "message": str(e)}

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

if __name__ == '__main__':
    app.run(port=9000)