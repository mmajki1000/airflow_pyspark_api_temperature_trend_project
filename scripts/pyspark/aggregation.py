from pyspark.sql import SparkSession
from pyspark.sql.functions import year, avg,min, max,col,round
import matplotlib.pyplot as plt


spark = SparkSession \
        .builder \
        .appName('aggregations') \
        .getOrCreate()

path = 'file:///***/data/processed/'
df = spark.read.parquet(path)

df2 = df.withColumn('year',year('date'))\
        .drop('ingestion_date')\
        

df_final = df2.groupBy('city','year')\
        .agg(round(avg('temperature'),0).alias('avg_temp'),\
             round(min('temperature'),0).alias('min_temp'),\
             round(max('temperature'),0).alias('max_temp')
             )\
        .orderBy('year')


# plotting avg_temp

y = [val.avg_temp for val in df_final.select('avg_temp').collect()]
x = [val.year for val in df_final.select('year').collect()]

plt.plot(x, y)

plt.ylabel('avg_temp')
plt.xlabel('year')
plt.title('Avg temperature over years in Wroclaw')

# plt.show()
plt.savefig('avg_temp_wroclaw')

path_save = 'file:///***/data/presentation_&_viz/'
df_final.write.option("path",path_save).saveAsTable("wroclaw_temperature",format="parquet",mode="overwrite")