
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType,DecimalType, ArrayType
from pyspark.sql.functions import posexplode,lit,to_date, current_timestamp
import pyspark.sql.functions as F 



spark = SparkSession\
    .builder \
    .appName('raw_data_processing') \
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
        StructField("temperature_2m_max",DecimalType(),True) ])), \
    StructField("daily",StructType([
        StructField("time",ArrayType(StringType(),True)), \
        StructField("temperature_2m_max",ArrayType(StringType(),True)) ]))
])

# data load from .json

df = spark.read \
    .option('multiLine', True) \
    .schema(schema) \
    .json('./data/raw/input_data.json') \
 
# turning arrays into columns & joining them to get 1 DF

date = df.select(posexplode(df['daily.time']))
temp = df.select(posexplode(df['daily.temperature_2m_max'])).withColumnRenamed('col', 'temp')



date_renamed = date.withColumn('city', lit('Wroclaw'))\
         .withColumn('date', to_date('col','yyyy-MM-dd'))\
         .drop('col')


df_joined = date_renamed.join(temp, date_renamed.pos == temp.pos)

df_final = df_joined.withColumn('ingestion_date', current_timestamp())\
                    .drop(df_joined.columns[3])
                        

# write data as table in Parquet format, for reporting purpose

path = 'file:///***/data/processed/'

df_final.write.option("path",path).saveAsTable("wroclaw_temperature",format="parquet",mode="overwrite")



