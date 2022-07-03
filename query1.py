from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

# Initialize spark session
spark = SparkSession.builder.getOrCreate()

# Load .csv from the DBFS location
df = spark.read.option("header", True).csv(
    "/FileStore/tables/Monthly_Recycling_and_Waste_Collection_Statistics-2.csv")

# Type cast TOTAL (IN TONS) column from string to integer type and create a new
# column with the name TOTAL_IN_TONS.

data_df = df.withColumn("TOTAL_IN_TONS",
                        df["TOTAL (IN TONS)"].cast(IntegerType()))

# Calculating the average waste per type
mean_df = data_df.groupBy("TYPE").mean("TOTAL_IN_TONS")

# Showing the datadrame
mean_df.show()
