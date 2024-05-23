from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from pyspark.ml.classification import LinearSVC, RandomForestClassifier, GBTClassifier
import pickle
import joblib
from pyspark.ml.classification import LinearSVCModel, RandomForestClassificationModel, GBTClassificationModel


# Load the saved model from the file




# Create the SparkSession
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 100)
loaded_model = RandomForestClassificationModel.load('churn/model/rf_model')

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
df = df.withColumnRenamed("churn", "label")

# Drop rows with missing values
df = df.dropna()

# Drop duplicate rows
df = df.dropDuplicates()

# Convert column names to lowercase and replace spaces with underscores
df = df.toDF(*(c.lower().replace(' ', '_') for c in df.columns))

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

# print(df)

# Select features and label
train_selected = df.select("features", "label_index")

print(train_selected)
# train_selected.show(truncate=False)


# # Now you can use this loaded_model for making predictions
# # For example:
prediction = loaded_model.transform(train_selected)
prediction.show()