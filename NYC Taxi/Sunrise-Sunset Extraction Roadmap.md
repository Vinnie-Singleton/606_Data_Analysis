## Plan
1. Read Sunset/Sunrise File
2. Separate columns
3. Get the minimum and maximum Sunrise and Sunset times
4. Get the total number of daylight HH:MM:SS for each day in the range of MinSunrise-MaxSunrise and MaxSunset-MaxSunset
5. Get the total number of rides in both ranges for every day
6. Correlate the number of rides to the amount of daylight in that day

The sunset/sunrise file is stored as DATE,SUNRISE,SUNSET using the format:
MM/DD/YYYY,HH:MM:SS,HH:MM:SS

## Pseudo Code

### Reading the file (The file does not contain headers)
[Reference](https://stackoverflow.com/questions/44558139/how-to-read-csv-without-header-and-name-them-with-names-while-reading-in-pyspark)

```
from pyspark.sql.types import StructType, StructField, IntegerType

schema = StructType([
  StructField("Date", DateType(), True),
  StructField("Sunrise", StringType(), True),
  StructField("Sunset", StringType(), True)])

df = spark.read.csv("sunrise_sunset_times.csv",header=False,schema=schema)
```

### Reading the file (The file does contain headers)

```
df = spark.read.csv("sunrise_sunset_times.csv",header=True)
```


### To parse the times
```
# Format = 07:19:59
val file = df("Sunrise")
val hour = file.map(line => line.split(":")[0])
val min = file.map(line => line.split(":")[1])
val sec = file.map(line => line.split(":")[2])
```

### Getting the minimum and maximum sunrise and sunset times
```
val time = hour+min+sec
val min_sunrise = df.groupBy("Sunrise", “time”).min(“time”)
val max_sunrise = df.groupBy("Sunrise", “time”).max(“time”)

val min_sunset = df.groupBy("Sunset", “time”).min(“time”)
val max_sunset = df.groupBy("Sunset", “time”).max(“time”)
```


### Get the amount of daylight/darkness in both ranges for every day
```
val morning_darkness = sunrise – min_sunrise
val morning_light = max_sunrise – sunrise

val evening_darkness = max_sunset – sunset
val evening_light = sunset – min_sunset
```

### Get number of rides in each range
```
val num_rides_in_morning = count(when(col("tpep_pickup_datetime”) <= max_sunrise AND col("tpep_pickup_datetime”) >= min_sunrise, True))
val num_rides_in_evening = count(when(col("tpep_pickup_datetime”) <= max_sunset AND col("tpep_pickup_datetime”) >= min_sunset, True))
```

### Computing correlation 
[Reference](https://stackoverflow.com/questions/37618977/pyspark-computing-correlation)

```
df.stat.coor(“num_rides_in_morning”, “morning_darkness”)
df.stat.coor(“num_rides_in_evening”, “evening_darkness”)
```
