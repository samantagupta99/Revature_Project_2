import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains


spark = SparkSession.builder.appName('Airlines_DataSet').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


# Define custom schema
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
      .load("Project_2/flights.csv")
print("Read csv with schema...")
flights2.printSchema()
flights2.show()




#11 Find the shortest flight from PDX in terms of distance.
b=flights2.filter(flights2.ORIGIN_AIRPORT == 'PDX').groupBy().min('DISTANCE')
b.show()

#12 Find the longest flight from SEA in terms of air time.
flights2.filter(flights2.ORIGIN_AIRPORT == 'SEA').groupBy().max('AIR_TIME').show()


#13 Group the flights by tailnum and find Number of flights each plane made.
by_plane = flights2.groupby("tail_number")
by_plane.count().show(10)

#14 Group the flights by origin_airport and Average duration of flights from SEA to PDX.
#by_origin = flights2.groupBy("ORIGIN_AIRPORT")
#by_origin.avg("AIR_TIME").show(10)

by_origin = flights2.filter(flights2.ORIGIN_AIRPORT == "SEA").filter(flights2.DESTINATION_AIRPORT == "PDX").groupBy("ORIGIN_AIRPORT","DESTINATION_AIRPORT")
by_origin.avg("AIR_TIME").show(10)

#15 Group the data by month and dest and find Average departure delay.
by_month_dest = flights2.groupBy('MONTH', 'DESTINATION_AIRPORT')
by_month_dest.avg('DEPARTURE_DELAY').show(10)



