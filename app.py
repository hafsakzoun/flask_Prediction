from flask import Flask, jsonify, render_template
from pymongo import MongoClient
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['BigData']
collection = db['prediction']

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/data/1', methods=['GET'])
def get_data_1():
    data = list(collection.find({"prediction": 1}, {"_id": 0, "state": 1, "account_length": 1, "area_code": 1, "international_plan": 1, "voice_mail_plan": 1}))
    return jsonify(data)

@app.route('/data/0', methods=['GET'])
def get_data_0():
    data = list(collection.find({"prediction": 0}, {"_id": 0, "state": 1, "account_length": 1, "area_code": 1, "international_plan": 1, "voice_mail_plan": 1}))
    return jsonify(data)

@app.route('/count_predictions', methods=['GET'])
def count_predictions():
    count_0 = collection.count_documents({"prediction": 0})
    count_1 = collection.count_documents({"prediction": 1})
    return jsonify({"prediction_0": count_0, "prediction_1": count_1})

@app.route('/states', methods=['GET'])
def get_states():
    states = collection.distinct("state")
    return jsonify(states)

if __name__ == '__main__':
    app.run(debug=True)
