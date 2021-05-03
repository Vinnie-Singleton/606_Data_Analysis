# spark modeling: classification with decision trees
#https://spark.apache.org/docs/2.3.0/ml-classification-regression.html#decision-tree-classifier
# pyspark.ml

from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# pyspark.sql
from pyspark.sql import SparkSession
import pyspark.sql.functions as fun
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType, IntegerType

# set up the SparkSession
conf = spark.sparkContext._conf.setAll(\
  [('spark.master', 'local[16]'), \
  ('spark.executor.memory', '16g'), \
  ('spark.app.name', 'nyctaxi'), \
  ('spark.cores.max', '16'), \
  ('spark.driver.memory','124g')])
sc = SparkSession.builder.config(conf=conf).getOrCreate()

#Need to define schema for NYC taxi data.
schema = StructType([ \
    StructField('VendorID', IntegerType(), True), \
    StructField("tpep_pickup_datetime",StringType(),True), \
    StructField("tpep_dropoff_datetime",StringType(),True), \
    StructField("passenger_count", IntegerType(), True), \
    StructField("trip_distance", DoubleType(), True), \
    StructField('RateCodeID', IntegerType(), True), \
    StructField('store_and_fwd_flag', StringType(), True), \
    StructField("PULocationID",IntegerType(),True), \
    StructField("DOLocationID", IntegerType(), True), \
    StructField("payment_type", IntegerType(), True), \
    StructField("fare_amount", DoubleType(), True), \
    StructField("extra", DoubleType(), True), \
    StructField("mta_tax", DoubleType(), True), \
    StructField("tip_amount", DoubleType(), True), \
    StructField("tolls_amount", DoubleType(), True), \
    StructField("improvement_surcharge", DoubleType(), True), \
    StructField("total_amount", DoubleType(), True), \
    StructField("congestion_surcharge", DoubleType(), True)
  ])
  
# Specify the full path
df = sc.read.format("csv").options(header='True').schema(schema).load("/projectnb2/ct-shbioinf/dan606/nyctaxi/trip\ data/yellow*2019*01*")
df.printSchema()

# handle dates AND time
# Convert to corrct dattime format so we can manipulate it using functions
df=df.withColumn('pickup_time', fun.to_timestamp('tpep_pickup_datetime', "yyyy-MM-dd HH:mm:ss"))
df=df.withColumn('pickup_hour', fun.hour("pickup_time"))

## ML: classification with Decision Trees

# Predicting the 'payment_type' value from other features of the Taxi data
# https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf

# Specify the independent vars
pred_col = ["trip_distance", "pickup_hour", "passenger_count"]

# Specify the var we want to predict
# In this case trip categories
# 1=Standard rate
# 2=JFK
# 3=Newark
# 4=Nassau or Westchester
# 5=Negotiated fare
# 6=Group ride
resp_var = 'RateCodeID'

# Drop NA values from df
dffeat = df.na.drop()

# Store the rows as a vector in a new column called 'features'
vector_assembler = VectorAssembler(inputCols=pred_col, outputCol='features')

#Create pipeline and pass the vector assembler as the one and only stage (this can be added to)
pipeline = Pipeline(stages=[
           vector_assembler
])

# Run the pipeline (in this case vectorizing the rows)
df_transformed = pipeline.fit(dffeat).transform(dffeat)

# Store the features (the vectorized dependent vars) and the target var which has been renamed 'label'
df_input = df_transformed.select(resp_var, 'features').withColumnRenamed(resp_var, 'label')

# Transforms string fields into numbers, "Apple" -> 0.0, "Banana" -> 1.0, "Pear" -> 2.0
# In this case it is saying that the label column should be treated as categorical
labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(df_input)


# Automatically identify categorical features, and index them.
# We specify maxCategories so features with > 4 distinct values are treated as continuous.
# For example, trip distance will have more then 4 distinct values so will be treadt as continuous
# while pasenger count will have a maximum of 4 (unless you strap someone to the roof).
featureIndexer =\
    VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(df_input)

# Split the data into training and test sets (30% held out for testing)
(trainingData, testData) = df_input.randomSplit([0.7, 0.3])

# Train a DecisionTree model.
dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")

# Chain indexers and tree in a Pipeline
pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dt])

# Train model.  This also runs the indexers.
model = pipeline.fit(trainingData)

# Make predictions.
predictions = model.transform(testData)

# Select example rows to display.
predictions.select("prediction", "indexedLabel", "features").show(10)

# Show predictions that are 1 to makes sure there are some of these values
predictions.filter(predictions["prediction"]!=0.0).show(10)
# Look at the correlations
predictions.select("prediction", "indexedLabel", "features").corr('prediction', 'indexedLabel')


# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
    
 # print out the accuracy of the model   
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g " % (1.0 - accuracy))

treeModel = model.stages[2]
# summary only
print(treeModel)
