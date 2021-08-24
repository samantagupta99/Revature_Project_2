import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains


spark = SparkSession.builder.appName('Airlines_DataSet').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#flight=spark.read.option("header",True).csv("Project_2/flights.csv")
#flight.printSchema()
#Printing the Flight dataset(First 10 Row)
#flight.show(truncate=False,n=10)

#airport=spark.read.option("header",True).csv("Project_2/airports.csv")
#airport.printSchema()
#Printing the Airport dataset(First 10 Row)
#airport.show(truncate=False,n=10)


flight=spark.read.option("header",True).csv("Project_2/airlines.csv")
flight.printSchema()
#Printing the Airlines dataset(First 10 Row)
flight.show(truncate=False,n=10)

