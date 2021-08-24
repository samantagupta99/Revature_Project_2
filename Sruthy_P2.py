import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import col,array_contains


spark = SparkSession.builder.appName('Airlines_DataSet').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

flights=spark.read.option("header",True).csv("ProjectP2/flights.csv")

#Q17  FLIIGHTS WITH A DELAY
model_data = flights.select('MONTH', 'DAY_OF_WEEK', 'AIRLINE', 'TAIL_NUMBER', 'DESTINATION_AIRPORT', 'AIR_TIME', 'DISTANCE', 'ARRIVAL_DELAY',)
model_data = model_data.withColumn("is_late", model_data.ARRIVAL_DELAY > 0)
model_data = model_data.withColumn("is_late", model_data.is_late.cast("integer"))
model_data = model_data.withColumnRenamed("is_late", 'label')
model_data.show(15)

#Q18  FIND FLIGHTS WITH DELAY, NO DELAY, NULL
model_data.groupBy('label').count().show()

#Q16  FILTER (CHOSEN) COLUMSN WITHOUT NULL 
model_data = flights.select('MONTH', 'DAY_OF_WEEK', 'AIRLINE', 'TAIL_NUMBER', 'DESTINATION_AIRPORT', 'AIR_TIME', 'DISTANCE', 'ARRIVAL_DELAY',)
model_data = model_data.filter("ARRIVAL_DELAY is not NULL and AIRLINE is not NULL and AIR_TIME is not NULL and TAIL_NUMBER is not NULL")
model_data.show(15)
model_data.count()




 
