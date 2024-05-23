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

@app.route('/total_clients', methods=['GET'])
def total_clients():
    total = collection.count_documents({})
    return jsonify({"total_clients": total})

@app.route('/states', methods=['GET'])
def get_states():
    states = collection.distinct("state")
    return jsonify(states)

@app.route('/unsubscribing_rate', methods=['GET'])
def unsubscribing_rate():
    total_customers = collection.count_documents({})
    unsubscribing_customers = collection.count_documents({"prediction": 1})
    
    if total_customers == 0:
        return jsonify({"rate": 0})
    
    rate = (unsubscribing_customers / total_customers) * 100.0
    rounded_rate = round(rate, 2)  # Round to two decimal places
    return jsonify({"rate": rounded_rate})

@app.route('/subscribing_rate', methods=['GET'])
def subscribing_rate():
    total_customers = collection.count_documents({})
    subscribing_customers = collection.count_documents({"prediction": 0}) 
    
    if total_customers == 0:
        return jsonify({"rate": 0})
    
    rate = (subscribing_customers / total_customers) * 100
    rounded_rate = round(rate, 2)  # Round to two decimal places
    return jsonify({"rate": rounded_rate})


if __name__ == '__main__':
    app.run(debug=True)
