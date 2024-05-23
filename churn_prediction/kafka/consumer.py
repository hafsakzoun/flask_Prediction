from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, ArrayType, StructField
from pyspark.sql import SparkSession
import os
from pyspark.ml.classification import RandomForestClassificationModel
import findspark
import logging
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import udf
from pyspark.ml.linalg import Vectors, VectorUDT, DenseVector
from pymongo import MongoClient


os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def convert_vector_to_list(vector):
    if isinstance(vector, DenseVector):
        return vector.toArray().tolist()
    else:
        return vector


def save_to_mongodb(predictions):
    try:
        # Connect to MongoDB
        client = MongoClient("mongodb://localhost:27017/")
        db = client.churn
        collection = db.predictions

        # Convert Spark DataFrame to Pandas DataFrame
        predictions_pd = predictions.toPandas()

        # Convert DenseVector to list in the Pandas DataFrame
        for col_name in predictions_pd.columns:
            if predictions_pd[col_name].apply(lambda x: isinstance(x, DenseVector)).any():
                predictions_pd[col_name] = predictions_pd[col_name].apply(convert_vector_to_list)

        # Convert Pandas DataFrame to a list of dictionaries
        records = predictions_pd.to_dict("records")

        # Insert records into MongoDB
        if records:
            collection.insert_many(records)
            logger.info("Successfully saved predictions to MongoDB")
        else:
            logger.info("No records to save to MongoDB")

    except Exception as e:
        logger.error(f"Error saving to MongoDB: {e}", exc_info=True)


def hamid(kafka_df, epoch_id):
    try:
        logger.info("Loading RandomForest model...")
        rfmodel = RandomForestClassificationModel.load('churn/model/rf_model')

        logger.info("Converting features array to VectorUDT format...")
        # UDF to convert array of doubles to Vector
        array_to_vector_udf = udf(lambda array: Vectors.dense(array), VectorUDT())
        kafka_df = kafka_df.withColumn("features", array_to_vector_udf(col("features")))

        logger.info("Selecting features and label columns...")
        test_selected = kafka_df.select("features", "label_index")
        
        logger.info("Transforming data using the model...")
        predictions = rfmodel.transform(test_selected)

        logger.info("Selecting probability and other columns...")
        # Select the probability column and rename it if necessary
        probabilities = predictions.select("features", "label_index", "prediction","probability")

        logger.info("Joining probability with original DataFrame...")
        enriched_predictions = kafka_df.join(probabilities, on=["features", "label_index"], how="inner")
        
        logger.info("Dropping features column...")
        # Drop the features column
        enriched_predictions = enriched_predictions.drop("features")

        logger.info("Saving predictions to MongoDB...")
        save_to_mongodb(enriched_predictions)
        
    except Exception as e:
        logger.error(f"Error processing batch: {e}", exc_info=True)

if __name__ == '__main__':
    findspark.init()
    # Define the schema for the incoming JSON data from Kafka
    schema = StructType([
        StructField("state", StringType(), True),
        StructField("account_length", StringType(), True),
        StructField("area_code", StringType(), True),
        StructField("international_plan", StringType(), True),
        StructField("voice_mail_plan", StringType(), True),
        StructField("number_vmail_messages", StringType(), True),
        StructField("total_day_minutes", StringType(), True),
        StructField("total_day_calls", StringType(), True),
        StructField("total_day_charge", StringType(), True),
        StructField("total_eve_minutes", StringType(), True),
        StructField("total_eve_calls", StringType(), True),
        StructField("total_eve_charge", StringType(), True),
        StructField("total_night_minutes", StringType(), True),
        StructField("total_night_calls", StringType(), True),
        StructField("total_night_charge", StringType(), True),
        StructField("total_intl_minutes", StringType(), True),
        StructField("total_intl_calls", StringType(), True),
        StructField("total_intl_charge", StringType(), True),
        StructField("customer_service_calls", StringType(), True),
        StructField("features", ArrayType(DoubleType()), True),
        StructField("label_index", StringType(), True)
    ])

    spark = SparkSession.builder \
        .getOrCreate()
    
    spark.conf.set("spark.sql.debug.maxToStringFields", 100)

    # Read data from Kafka topic as a streaming DataFrame
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "mytopic") \
        .option("header","true") \
        .load()

    # Convert value column from Kafka message into JSON string
    kafka_df = kafka_df \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    query = kafka_df.writeStream \
    .queryName("predictions") \
    .foreachBatch(hamid) \
    .start()

    query.awaitTermination()