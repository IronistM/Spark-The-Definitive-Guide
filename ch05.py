# chapter 5
# Read in data -----------------------------------------------------------------
df = spark.read.format("json").load("/data/flight-data/json/2015-summary.json")
# We discussed that a DataFame will have columns, and we use a schema to
# define them. Let’s take a look at the schema on our current DataFrame:
df.printSchema()
#
spark.read.format("json").load("/data/flight-data/json/2015-summary.json").schema

# Manual scehma definition -----------------------------------------------------
from pyspark.sql.types import StructField, StructType, StringType, LongType

myManualSchema = StructType([
  StructField("DEST_COUNTRY_NAME", StringType(), True),
  StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
  StructField("count", LongType(), False, metadata={"hello":"world"})
])
df = spark.read.format("json").schema(myManualSchema)\
  .load("/data/flight-data/json/2015-summary.json")

# Programmatically access columns ----------------------------------------------
spark.read.format("json").load("/data/flight-data/json/2015-summary.json")
  .columns

#
df = spark.read.format("json").load("/data/flight-data/json/2015-summary.json")
df.createOrReplaceTempView("dfTable")

# in Python
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType, StringType, LongType
myManualSchema = StructType([
  StructField("some", StringType(), True),
  StructField("col", StringType(), True),
  StructField("names", LongType(), False)
])
myRow = Row("Hello", None, 1)
myDf = spark.createDataFrame([myRow], myManualSchema)
myDf.show()

#  Select columns --------------------------------------------------------------
from pyspark.sql.functions import expr, col, column
df.select(
    expr("DEST_COUNTRY_NAME"),
    col("DEST_COUNTRY_NAME"),
    column("DEST_COUNTRY_NAME"))\
  .show(2)
