import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains


spark = SparkSession.builder.appName('Airlines_DataSet').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


flights1=spark.read.option("header",True).csv("ProjectP2/flights.csv")
airport1=spark.read.option("header",True).csv("ProjectP2/airports.csv")
airlines1=spark.read.option("header",True).csv("ProjectP2/airlines.csv")

schema = StructType([
       StructField("YEAR",IntegerType(),True),
       StructField("MONTH",IntegerType(),True),
       StructField("DAY",IntegerType(),True),
       StructField("DAY_OF_WEEK",IntegerType(),True),
       StructField("AIRLINE",StringType(),True),
       StructField("FLIGHT_NUMBER",StringType(),True),

       StructField("TAIL_NUMBER",StringType(),True),
       StructField("ORIGIN_AIRPORT",StringType(),True),
       StructField("DESTINATION_AIRPORT",StringType(),True),
       StructField("SCHEDULED_DEPARTURE",IntegerType(),True),
       StructField("DEPARTURE_TIME",IntegerType(),True),
       StructField("DEPARTURE_DELAY",IntegerType(),True),

       StructField("TAXI_OUT",IntegerType(),True),
       StructField("WHEELS_OFF",IntegerType(),True),
       StructField("SCHEDULED_TIME",IntegerType(),True),
       StructField("ELAPSED_TIME",IntegerType(),True),
       StructField("AIR_TIME",IntegerType(),True),
       StructField("DISTANCE",IntegerType(),True),

       StructField("WHEELS_ON",IntegerType(),True),
       StructField("TAXI_IN",IntegerType(),True),
       StructField("SCHEDULED_ARRIVAL",IntegerType(),True),
       StructField("ARRIVAL_TIME",IntegerType(),True),
       StructField("ARRIVAL_DELAY",IntegerType(),True),
       StructField("DIVERTED",IntegerType(),True),

       StructField("CANCELLED",IntegerType(),True),
       StructField("CANCELLATION_REASON",StringType(),True),
       StructField("AIR_SYSTEM_DELAY",IntegerType(),True),
       StructField("SECURITY_DELAY",IntegerType(),True),
       StructField("AIRLINE_DELAY",IntegerType(),True),
       StructField("LATE_AIRCRAFT_DELAY",IntegerType(),True),
       StructField("WEATHER_DELAY",IntegerType(),True),

    ])


flights2 = spark.read.format("csv") \
       .option("header", True) \
       .schema(schema) \
       .load("ProjectP2/flights.csv")

flights1.createOrReplaceTempView('flights')
airlines1.createOrReplaceTempView('airlines')

#Q16
print("Query 16 : Joining the tables with IATA code  -> flights with airports")
flights_with_airports=flights1.join(airport1,flights1.ORIGIN_AIRPORT == airport1.IATA_CODE, "inner")
flights_with_airports.show(15)
print(flights_with_airports.columns)
print(flights_with_airports.count())

#17
print("Query 17 : Display airline details where arrival delay, airline, air time and tail number is not null")
data = flights1.select('MONTH', 'DAY_OF_WEEK', 'AIRLINE', 'TAIL_NUMBER', 'DESTINATION_AIRPORT', 'AIR_TIME', 'DISTANCE', 'ARRIVAL_DELAY',)
data1 = data.filter("ARRIVAL_DELAY is not NULL and AIRLINE is not NULL and AIR_TIME is not NULL and TAIL_NUMBER is not NULL")
data1.show(15)
data1.count()


#18
print("Query 18 : Display flight delays with new column:label")
data2 = flights1.select('MONTH', 'DAY_OF_WEEK', 'AIRLINE', 'TAIL_NUMBER', 'DESTINATION_AIRPORT', 'AIR_TIME', 'DISTANCE', 'ARRIVAL_DELAY',)
data3 = data2.withColumn("is_late", data2.ARRIVAL_DELAY > 0)
data3 = data3.withColumn("is_late", data3.is_late.cast("integer"))
data3 = data3.withColumnRenamed("is_late", 'label')
data3.show(15)


#19
print("Query 19 : Count of flights with delay, no delay and null data")
data3.groupBy('label').count().show()


#20
query = "SELECT ORIGIN_AIRPORT, DESTINATION_AIRPORT, COUNT(*) as N FROM flights GROUP BY ORIGIN_AIRPORT, DESTINATION_AIRPORT"
flight_counts = spark.sql(query).show()

#21
query="SELECT f.AIRLINE,a.AIRLINE_CODE,a.AIRLINE from flights as f   JOIN airlines as a  on a.AIRLINE_CODE=f.AIRLINE LIMIT 10"
flights3 = spark.sql(query)
flights3.show()





 
