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
        .filter(col('year') != '2023')
        

df_final = df2.groupBy('city','year')\
        .agg(round(avg('temperature'),0).alias('avg_temp'),\
             round(min('temperature'),0).alias('min_temp'),\
             round(max('temperature'),0).alias('max_temp')
             )\
        .orderBy('year')


# plotting temperatures

year_date = [val.year for val in df_final.select('year').collect()]
avg_temp = [val.avg_temp for val in df_final.select('avg_temp').collect()]
min_temp = [val.min_temp for val in df_final.select('min_temp').collect()]
max_temp = [val.max_temp for val in df_final.select('max_temp').collect()]

# as 3 charts

fig, axs = plt.subplots(1,3, figsize=(9,6), sharey=True)

axs[0].plot(year_date, avg_temp, label='avg_temp')
axs[1].plot(year_date, min_temp, label='min_temp')
axs[2].plot(year_date, max_temp, label='max_temp')

fig.suptitle('Temperatures over years in Wroclaw')

plt.show()
plt.savefig('Temperatures_Wroclaw_1')

# as 1 chart

fig, ax = plt.subplots()

ax.plot(year_date, avg_temp, label='avg_temp')
ax.plot(year_date, min_temp, label='min_temp')
ax.plot(year_date, max_temp, label='max_temp')

fig.legend()
plt.title('Temperature over years in Wroclaw')
plt.show()
plt.savefig('Temperatures_Wroclaw_2')

path_save = 'file:///***/data/presentation_&_viz/'
df_final.write.option("path",path_save).saveAsTable("wroclaw_temperature",format="parquet",mode="overwrite")