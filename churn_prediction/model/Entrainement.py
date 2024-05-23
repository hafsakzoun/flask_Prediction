from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType
from pyspark.ml.classification import LinearSVC, RandomForestClassifier, GBTClassifier
import pickle
import joblib


# Create the SparkSession
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 100)

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
data_path = "churn-bigml-80.csv"
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
train_data, test_data = df.randomSplit([0.7, 0.3], seed=42)
train_data = indexer.fit(train_data).transform(train_data)
train_data = assembler.transform(train_data)

test_data = indexer.fit(test_data).transform(test_data)
test_data = assembler.transform(test_data)

# Scale numerical features
scaler = StandardScaler(inputCol="raw_features", outputCol="features")
scaler_model = scaler.fit(train_data)

train_data = scaler_model.transform(train_data)
test_data = scaler_model.transform(test_data)

# Select features and label
train_selected = train_data.select("features", "label_index")
test_selected = test_data.select("features", "label_index")
train_selected.show(truncate=False)

# Create the SVM model
svm = LinearSVC(featuresCol="features", labelCol="label_index")

# Create the Random Forest model
rf = RandomForestClassifier(featuresCol="features", labelCol="label_index")

# Create the Gradient Boosting model
gbt = GBTClassifier(featuresCol="features", labelCol="label_index")

# Fit the models
svm_model = svm.fit(train_selected)
rf_model = rf.fit(train_selected)
gbt_model = gbt.fit(train_selected)

# Make predictions on the test set
svm_predictions = svm_model.transform(test_selected)
rf_predictions = rf_model.transform(test_selected)
gbt_predictions = gbt_model.transform(test_selected)

# Evaluate model performance
evaluator = MulticlassClassificationEvaluator(labelCol="label_index", metricName="accuracy")

# Evaluate SVM model
svm_accuracy = evaluator.evaluate(svm_predictions)
print("SVM Accuracy:", svm_accuracy)

# Evaluate Random Forest model
rf_accuracy = evaluator.evaluate(rf_predictions)
print("Random Forest Accuracy:", rf_accuracy)

# Evaluate Gradient Boosting model
gb_accuracy = evaluator.evaluate(gbt_predictions)
print("Gradient Boosting Accuracy:", gb_accuracy)

rf_model.save("rf_model1")

# if rf_accuracy > svm_accuracy and rf_accuracy > gb_accuracy:
#     joblib.dump(rf_model, "random_forest_model.pkl")
# elif svm_accuracy > rf_accuracy and svm_accuracy > gb_accuracy:
#     joblib.dump(svm_model, "svm_model.pkl")
# else:
#     joblib.dump(gbt_model, "gbt_model.pkl")

