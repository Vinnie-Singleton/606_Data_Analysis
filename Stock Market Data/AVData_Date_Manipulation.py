#pyspark code for avdata exploration
# append: 'export PYSPARK_PYTHON=python3' to .bashrc

from pyspark.sql import SparkSession
import pyspark.sql.functions as fun

# Set up the spark session and specify that there will be 8 workers (cores) used to process data.
spark = SparkSession.builder \
    .master("local[8]") \
    .appName("avdata") \
    .getOrCreate()
    
# warning: It is a little slower to read the data with inferSchema='True'. 
#Consider defining schema.

# Read the csv and have the data types infered based on the data in the csv
df = spark.read.format("csv").options(header='True',inferSchema='True').load("avdata/*")
df.printSchema()

# get a count of rows
df.count()

# get sourcefile name from input_file_name()

# Create a column called path which will hold the file path
df = df.withColumn("path", fun.input_file_name())

#regex to extract text after the last / or \
regex_str = "[\/]([^\/]+[^\/]+)$"

# Create a column called sourcefile that exctracts the information after the last forward slash
# i.e. my/path/AMC -> AMC
df = df.withColumn("sourcefile", fun.regexp_extract("path",regex_str,1))
df.show()

#######################################################################
# handle dates and times

# Convert the timestamp column to a date
df=df.withColumn('timestamp', fun.to_date("timestamp"))
df.show(2)

# now we should be able to convert or extract date features from timestamp

# Extract the day of the month from the timestamp column
df.withColumn('dayofmonth', fun.dayofmonth("timestamp")).show(2)

# Extract the month from the timestamp column
df.withColumn('month', fun.month("timestamp")).show(2)

# Extract the year from the timestamp column
df.withColumn('year', fun.year("timestamp")).show(2)

# Extract the day of the year from the timestamp column
df.withColumn('dayofyear', fun.dayofyear("timestamp")).show(2) 

# calculate the difference from the current date ('days_ago')
# datediff takes two dates and subtracts the first date from the 
# second returning the number of days between the two
df.withColumn('days_ago', fun.datediff(fun.current_date(), "timestamp")).show()

#group_by
# summarize within group data

# Counts the number of records for 99 of the stock symbols
df.groupBy("sourcefile").count().show(99)

# Gets the minimum open value for 99 of the stock symbols
df.groupBy("sourcefile").min('open').show(99)

# Gets the average open value for 99 of the stock symbols
df.groupBy("sourcefile").mean('open').show(99)

# Gets the maximum open value for 99 of the stock symbols
df.groupBy("sourcefile").max('open','close').show(99)

#window functions
from pyspark.sql.window import Window

# Creates a new column called days_ago which holds the number of days difference between
# today's date and the timestamp in the file
df=df.withColumn('days_ago', fun.datediff(fun.current_date(), "timestamp"))

# Reorganize the data by stock symbol and then order by days_ago
windowSpec = Window.partitionBy("sourcefile").orderBy("days_ago")

#see also lead

# Store the value in the "open" column 14 days ago in the "lag" column based on the data stored
# and ordered by the column days_ago from windowSpec
dflag=df.withColumn("lag",fun.lag("open",14).over(windowSpec))
# Show the stock symbol, the opening value 14 days from this opening, and the opening value of this record
dflag.select('sourcefile', 'lag', 'open').show(99)

# Calculate the difference between the opening stock price 14 days ago and this opening. Show 99 records.
dflag.withColumn('twoweekdiff', fun.col('lag') - fun.col('open')).show(99)
