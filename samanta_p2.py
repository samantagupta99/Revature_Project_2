  
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains


spark = SparkSession.builder.appName('Airlines_DataSet').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#1
flights1=spark.read.option("header",True).csv("Project_2/flights.csv")
flights1.printSchema()
#Printing the Flight dataset(First 10 Row)
flights1.show(truncate=False,n=10)

#2
#airport1=spark.read.option("header",True).csv("Project_2/airports.csv")
#airport1.printSchema()
#Printing the Airport dataset(First 10 Row)
#airport1.show(truncate=False,n=10)

#3
#airlines1=spark.read.option("header",True).csv("Project_2/airlines.csv")
#airlines1.printSchema()
#Printing the Airlines dataset(First 10 Row)
#airlines1.show(truncate=False,n=10)


# Define custom schema
# schema = StructType([
#       StructField("YEAR",IntegerType(),True),
#       StructField("MONTH",IntegerType(),True),
#       StructField("DAY",IntegerType(),True),
#       StructField("DAY_OF_WEEK",IntegerType(),True),
#       StructField("AIRLINE",StringType(),True),
#       StructField("FLIGHT_NUMBER",StringType(),True),

#       StructField("TAIL_NUMBER",StringType(),True),
#       StructField("ORIGIN_AIRPORT",StringType(),True),
#       StructField("DESTINATION_AIRPORT",StringType(),True),
#       StructField("SCHEDULED_DEPARTURE",IntegerType(),True),
#       StructField("DEPARTURE_TIME",IntegerType(),True),
#       StructField("DEPARTURE_DELAY",IntegerType(),True),

#       StructField("TAXI_OUT",IntegerType(),True),
#       StructField("WHEELS_OFF",IntegerType(),True),
#       StructField("SCHEDULED_TIME",IntegerType(),True),
#       StructField("ELAPSED_TIME",IntegerType(),True),
#       StructField("AIR_TIME",IntegerType(),True),
#       StructField("DISTANCE",IntegerType(),True),

#       StructField("WHEELS_ON",IntegerType(),True),
#       StructField("TAXI_IN",IntegerType(),True),
#       StructField("SCHEDULED_ARRIVAL",IntegerType(),True),
#       StructField("ARRIVAL_TIME",IntegerType(),True),
#       StructField("ARRIVAL_DELAY",IntegerType(),True),
#       StructField("DIVERTED",IntegerType(),True),

#       StructField("CANCELLED",IntegerType(),True),
#       StructField("CANCELLATION_REASON",StringType(),True),
#       StructField("AIR_SYSTEM_DELAY",IntegerType(),True),
#       StructField("SECURITY_DELAY",IntegerType(),True),
#       StructField("AIRLINE_DELAY",IntegerType(),True),
#       StructField("LATE_AIRCRAFT_DELAY",IntegerType(),True),
#       StructField("WEATHER_DELAY",IntegerType(),True),

#    ])


#    flights2 = spark.read.format("csv") \
#       .option("header", True) \
#       .schema(schema) \
#       .load("Project_2/flights.csv")
#print("Read csv with schema...")
#flights2.printSchema()
#flights2.show()


#flights1.createOrReplaceTempView('flights')