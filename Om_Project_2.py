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

#4
#query = "SELECT AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT, SCHEDULED_DEPARTURE FROM flights LIMIT 10"

#flights2 = spark.sql(query)
#flights2.show()

#5
#query = "SELECT ORIGIN_AIRPORT, DESTINATION_AIRPORT, COUNT(*) as N FROM flights GROUP BY ORIGIN_AIRPORT, DESTINATION_AIRPORT"
#flight_counts = spark.sql(query).show()

#6
#flights1.filter(flights1.AIR_TIME > 120).show(3)

#7
#temp = flights1.select(flights1.ORIGIN_AIRPORT, flights1.DESTINATION_AIRPORT, flights1.AIRLINE)
#temp.show()


#8
# filterA = flights1.ORIGIN_AIRPORT == "SEA"
# filterB = flights1.DESTINATION_AIRPORT == "PDX"
# df1 = temp.filter(filterA).filter(filterB)
# df1.show()


#9
# flight3 = flights2.withColumn('duration_hrs', flights2.AIR_TIME/60.)
# flight3.select('duration_hrs').show(10)


#10
# avg_speed = (flights1.DISTANCE/(flights1.AIR_TIME/60)).alias("avg_speed")
# speed1 = flights1.select('TAIL_NUMBER', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT', avg_speed)
# speed1.show()


#11
#flights2.filter(flights2.ORIGIN_AIRPORT == 'PDX').groupBy().min('DISTANCE').show()


#12
#flights2.filter(flights2.ORIGIN_AIRPORT == 'SEA').groupBy().max('AIR_TIME').show()


#13
# by_plane = flights2.groupBy("TAIL_NUMBER")
# by_plane.count().show(10)

#14
#by_origin = flights2.groupBy("ORIGIN_AIRPORT")
#by_origin.avg("AIR_TIME").show(10)


#15
#by_month_dest = flights2.groupBy('MONTH', 'DESTINATION_AIRPORT')
#by_month_dest.avg('DEPARTURE_DELAY').show(10)


#16
#flights_with_airports=flights1.join(airport1,flights1.ORIGIN_AIRPORT == airport1.IATA_CODE, "inner")
#flights_with_airports.show(15)
#print(flights_with_airports.columns)
#print(flights_with_airports.count())

#17
#model_data = flights1.select('MONTH', 'DAY_OF_WEEK', 'AIRLINE', 'TAIL_NUMBER', 'DESTINATION_AIRPORT', 'AIR_TIME', 'DISTANCE', 'ARRIVAL_DELAY',)
#model_data = model_data.filter("ARRIVAL_DELAY is not NULL and AIRLINE is not NULL and AIR_TIME is not NULL and TAIL_NUMBER is not NULL")
#model_data.show(15)
#model_data.count()


#18
#model_data = flights1.select('MONTH', 'DAY_OF_WEEK', 'AIRLINE', 'TAIL_NUMBER', 'DESTINATION_AIRPORT', 'AIR_TIME', 'DISTANCE', 'ARRIVAL_DELAY',)
#model_data = model_data.withColumn("is_late", model_data.ARRIVAL_DELAY > 0)
#model_data = model_data.withColumn("is_late", model_data.is_late.cast("integer"))
#model_data = model_data.withColumnRenamed("is_late", 'label')
#model_data.show(15)


#19
#model_data.groupBy('label').count().show()


#20
#print((flights1.count(), len(flights1.columns)))
#flights1.show(10)

