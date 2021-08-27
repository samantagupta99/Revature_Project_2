import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains


spark = SparkSession.builder.appName('Airlines_DataSet').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#1
flights1=spark.read.option("header",True).csv("Project2/flights.csv")
#flights1.printSchema()
#Printing the Flight dataset(First 10 Row)
#flights1.show(truncate=False,n=10)
#flights1.createOrReplaceTempView('flights')


#2
#airport1=spark.read.option("header",True).csv("Project2/airports.csv")
#airport1.printSchema()
#Printing the Airport dataset(First 10 Row)
#airport1.show(truncate=False,n=10)


#3
#airlines1=spark.read.option("header",True).csv("Project2/airlines.csv")
#airlines1.printSchema()
#Printing the Airlines dataset(First 10 Row)
#airlines1.show(truncate=False,n=10)


#4
#print((flights1.count(), len(flights1.columns)))
#flights1.show(5)


#5
#flights1.createOrReplaceTempView('flights')
#query = "SELECT AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT, SCHEDULED_DEPARTURE FROM flights LIMIT 10"
#flights2 = spark.sql(query)
#flights2.show()
