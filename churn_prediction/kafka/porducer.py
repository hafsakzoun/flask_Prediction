from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from pyspark.ml.feature import MinMaxScaler, StringIndexer, VectorAssembler
from kafka import KafkaProducer
import json
import time


# Create the SparkSession
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 100)

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')


# Define the schema
input_schema = StructType([
    StructField("state", StringType(), True),
    StructField("account_length", LongType(), True),
    StructField("area_code", LongType(), True),
    StructField("international_plan", StringType(), True),
    StructField("voice_mail_plan", StringType(), True),
    StructField("number_vmail_messages", LongType(), True),
    StructField("total_day_minutes", DoubleType(), True),
    StructField("total_day_calls", LongType(), True),
    StructField("total_day_charge", DoubleType(), True),
    StructField("total_eve_minutes", DoubleType(), True),
    StructField("total_eve_calls", LongType(), True),
    StructField("total_eve_charge", DoubleType(), True),
    StructField("total_night_minutes", DoubleType(), True),
    StructField("total_night_calls", LongType(), True),
    StructField("total_night_charge", DoubleType(), True),
    StructField("total_intl_minutes", DoubleType(), True),
    StructField("total_intl_calls", LongType(), True),
    StructField("total_intl_charge", DoubleType(), True),
    StructField("customer_service_calls", DoubleType(), True),
    StructField("churn", StringType(), True)
])

# Load the data
data_path = "churn/model/churn-bigml-20.csv"
df = spark.read.format('csv').option('header', True).schema(input_schema).load(data_path)
# Drop rows with missing values
df = df.dropna()

# Drop duplicate rows
df = df.dropDuplicates()
df = df.withColumnRenamed("churn", "label")


# Define transformers
indexer = StringIndexer(inputCols=['state', 'international_plan', 'voice_mail_plan', 'label'],
                        outputCols=['state_index', 'international_plan_index', 'voice_mail_plan_index', 'label_index'])

features_cols = ['account_length', 'area_code', 'number_vmail_messages', 'total_day_minutes', 'total_day_calls',
                 'total_day_charge', 'total_eve_minutes', 'total_eve_calls', 'total_eve_charge', 'total_night_minutes',
                 'total_night_calls', 'total_night_charge', 'total_intl_minutes', 'total_intl_calls',
                 'total_intl_charge', 'customer_service_calls']
assembler = VectorAssembler(inputCols=features_cols, outputCol="raw_features")

# Apply transformers

df = indexer.fit(df).transform(df)
df = assembler.transform(df)


# Scale numerical features
scaler = StandardScaler(inputCol="raw_features", outputCol="features")
scaler_model = scaler.fit(df)
df = scaler_model.transform(df)


for row in df.collect():

    row_dict = {
        "state": row.state,
        "account_length": row.account_length,
        "area_code": row.area_code,
        "international_plan": row.international_plan,
        "voice_mail_plan": row.voice_mail_plan,
        "number_vmail_messages": row.number_vmail_messages,
        "total_day_minutes": row.total_day_minutes,
        "total_day_calls": row.total_day_calls,
        "total_day_charge": row.total_day_charge,
        "total_eve_minutes": row.total_eve_minutes,
        "total_eve_calls": row.total_eve_calls,
        "total_eve_charge": row.total_eve_charge,
        "total_night_minutes": row.total_night_minutes,
        "total_night_calls": row.total_night_calls,
        "total_night_charge": row.total_night_charge,
        "total_intl_minutes": row.total_intl_minutes,
        "total_intl_calls": row.total_intl_calls,
        "total_intl_charge": row.total_intl_charge,
        "customer_service_calls": row.customer_service_calls,
        "features": row.features.tolist(),
        "label_index": row.label_index
                                        }
    json_data = json.dumps(row_dict)  # Convertir la ligne en format JSON
    producer.send('mytopic', value=json_data.encode('utf-8'))  # Envoyer le message au topic

producer.close()