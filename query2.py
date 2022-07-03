from pyspark.sql import SparkSession, Window
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import *

# Initialize spark session
spark = SparkSession.builder.getOrCreate()

# Load .csv from the DBFS location
df = spark.read.option("header", True).csv(
    "/FileStore/tables/Monthly_Recycling_and_Waste_Collection_Statistics-2.csv")

# Type cast TOTAL (IN TONS) column from string to integer type and create a new
# column with the name TOTAL (IN TONS).

df = df.withColumn("TOTAL (IN TONS)",
                   df["TOTAL (IN TONS)"].cast(IntegerType()))

# Type casting DATE column from to type Date.
df = df.withColumn('date',
                   to_date(unix_timestamp(col('DATE'),
                                          'MM/dd/yyyy').cast("timestamp")))

print(df.count())

# Querying the Dataframe

mean_df = df.groupBy(year("date"), "TYPE").sum("TOTAL (IN TONS)")

mean_df = mean_df.orderBy("year(date)")

mean_df.show()


waste_names = ["Scrap Metal", "Sidewalk Debris", "Misc. Recycling", "Haz Waste", "Yard Waste"]

waste_df = mean_df.filter("TYPE == 'Scrap Metal'")

for name in waste_names:
    waste_temp = mean_df.filter("TYPE == '{}'".format(name))
    waste_df = waste_df.merge]\

waste_df.show()


# w = Window().partitionBy('year(date)')

# w = Window().partitionBy('year[date]')




