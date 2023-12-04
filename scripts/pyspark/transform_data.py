
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType,DecimalType, ArrayType
from pyspark.sql.functions import get_json_object, col,arrays_zip,split, posexplode,lit
import pyspark.sql.functions as F 



spark = SparkSession.builder \
    .appName('raw_data') \
    .config('spark.driver.extraClassPath','mssql-jdbc-12.4.2.jre11.jar:sqljdbc_auth.dll') \
    .getOrCreate()

# schema cretaion

schema = StructType([
    StructField("latitude",DecimalType(),True), \
    StructField("longitude",DecimalType(),True), \
    StructField("generationtime_ms",DecimalType(),True), \
    StructField("utc_offset_seconds",DecimalType(),True), \
    StructField("timezone",StringType(),True), \
    StructField("timezone_abbreviation",StringType(),True), \
    StructField("elevation",DecimalType(),True), \
    StructField("daily_units",StructType([
        StructField("time",StringType(),True), \
        StructField("temperature_2m_max",StringType(),True) ])), \
    StructField("daily",StructType([
        StructField("time",ArrayType(StringType(),True)), \
        StructField("temperature_2m_max",ArrayType(StringType(),True)) ]))
])

# data load from .json

df = spark.read \
    .option('multiLine', True) \
    .schema(schema) \
    .json('./files/data.json') \
 
# turning arrays into columns & joining them to get 1 DF

date = df.select(posexplode(df['daily.time']))
temp = df.select(posexplode(df['daily.temperature_2m_max']))

df2 = date.join(temp, date.pos == temp.pos)

# renaming columns & dropping unnecessery ones

df2 = df2.withColumnRenamed(df2.columns[1], 'date')\
        .withColumnRenamed(df2.columns[3], 'temp')


df2 = df2.drop(df2.columns[0],df2.columns[2] )




