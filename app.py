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

@app.route('/staying_customers', methods=['GET'])
def staying_customers():
    staying = collection.count_documents({"prediction": 0})  # Assuming 0 represents staying customers
    return jsonify({"staying": staying})

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

@app.route('/churn_probabilities', methods=['GET'])
def churn_probabilities():
    data = list(collection.find({}, {"_id": 0, "probability": 1}))
    probabilities = [item["probability"][1] for item in data]  # Assuming the second value is the churn probability
    return jsonify(probabilities)

@app.route('/churn_data', methods=['GET'])
def churn_data():
    data = list(collection.find({"prediction": 1}, {"_id": 0, "prediction": 1, "probability": 1}))
    return jsonify(data)

@app.route('/churn_data2', methods=['GET'])
def churn_data2():
    data = list(collection.find({"prediction": 0}, {"_id": 0, "prediction": 1, "probability": 1}))
    return jsonify(data)

@app.route('/rates', methods=['GET'])
def get_rates():
    total_customers = collection.count_documents({})
    unsubscribing_customers = collection.count_documents({"prediction": 1})
    subscribing_customers = collection.count_documents({"prediction": 0})
    
    if total_customers == 0:
        unsubscribing_rate = 0
        subscribing_rate = 0
    else:
        unsubscribing_rate = round((unsubscribing_customers / total_customers) * 100, 2)
        subscribing_rate = round((subscribing_customers / total_customers) * 100, 2)
    
    return jsonify({
        "subscribing_rate": subscribing_rate,
        "unsubscribing_rate": unsubscribing_rate
    })

@app.route('/predictions_count_by_state', methods=['GET'])
def predictions_count_by_state():
    pipeline = [
        {"$match": {"prediction": 1}},
        {"$group": {"_id": "$state", "count": {"$sum": 1}}}
    ]
    result = list(collection.aggregate(pipeline))
    predictions_by_state = {item['_id']: item['count'] for item in result}
    return jsonify(predictions_by_state)

@app.route('/predictions_count_by_state2', methods=['GET'])
def predictions_count_by_state2():
    pipeline = [
        {"$match": {"prediction": 0}},
        {"$group": {"_id": "$state", "count": {"$sum": 1}}}
    ]
    result = list(collection.aggregate(pipeline))
    predictions_by_state = {item['_id']: item['count'] for item in result}
    return jsonify(predictions_by_state)

@app.route('/churn_rate_by_customer_service_calls', methods=['GET'])
def churn_rate_by_customer_service_calls():
    pipeline = [
        {"$group": {
            "_id": "$customer_service_calls",
            "total": {"$sum": 1},
            "churned": {"$sum": {"$cond": [{"$eq": ["$prediction", 1]}, 1, 0]}}
        }},
        {"$project": {
            "churn_rate": {"$multiply": [{"$divide": ["$churned", "$total"]}, 100]}
        }}
    ]
    result = list(collection.aggregate(pipeline))
    churn_rate_by_service_calls = {item['_id']: round(item['churn_rate'], 2) for item in result}
    return jsonify(churn_rate_by_service_calls)

@app.route('/churn_rate_by_international_plan', methods=['GET'])
def churn_rate_by_international_plan():
    pipeline = [
        {"$group": {
            "_id": "$international_plan",
            "total": {"$sum": 1},
            "churned": {"$sum": {"$cond": [{"$eq": ["$prediction", 1]}, 1, 0]}}
        }},
        {"$project": {
            "churn_rate": {"$multiply": [{"$divide": ["$churned", "$total"]}, 100]}
        }}
    ]
    result = list(collection.aggregate(pipeline))
    churn_rate_by_intl_plan = {item['_id']: round(item['churn_rate'], 2) for item in result}
    return jsonify(churn_rate_by_intl_plan)

@app.route('/churn_rate_by_account_length', methods=['GET'])
def churn_rate_by_account_length():
    pipeline = [
        {"$bucketAuto": {
            "groupBy": "$account_length",
            "buckets": 10,  # Adjust the number of buckets as needed
            "output": {
                "total": {"$sum": 1},
                "churned": {"$sum": {"$cond": [{"$eq": ["$prediction", 1]}, 1, 0]}},
                "min_account_length": {"$min": "$account_length"},
                "max_account_length": {"$max": "$account_length"}
            }
        }},
        {"$project": {
            "churn_rate": {"$multiply": [{"$divide": ["$churned", "$total"]}, 100]},
            "range": {"$concat": [{"$toString": "$min_account_length"}, "-", {"$toString": "$max_account_length"}]}
        }}
    ]
    result = list(collection.aggregate(pipeline))
    churn_rate_by_account_length = {item['range']: round(item['churn_rate'], 2) for item in result}
    return jsonify(churn_rate_by_account_length)



if __name__ == '__main__':
    app.run(debug=True)
