# Databricks notebook source
# MAGIC %md 
# MAGIC #imports
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, unix_timestamp
from pyspark.sql.functions import year, month,dayofmonth,dayofweek
from pyspark.sql.types import IntegerType
from datetime import datetime
from pyspark.sql.functions import col
from pyspark.sql.functions import when
from pyspark.sql.functions import hour

# COMMAND ----------

#Connection configuration
spark.conf.set(
"fs.azure.account.key.yassineessadidatalake.blob.core.windows.net", "h86GBzEpa+ThpzecSIcdLHOhT8FMLIx37mko3qGirDhVBAEd+Ug5QsQmkCsyF9nT5WEHYZVD/cZF+AStlYlQrQ==")

spark_df = spark.read.format('csv').option('header', True).load("wasbs://data@yassineessadidatalake.blob.core.windows.net/public_transport_data/raw/*.csv")

display(spark_df)

# COMMAND ----------



#Add columns year, month, day, and day of the week
#spark_df  = spark.createDataFrame(spark_df, ["Date"])
spark_df = spark_df.withColumn("Date", col("Date").cast("Date"))
spark_df = spark_df.withColumn("Year", year(spark_df["Date"]).cast(IntegerType()))
spark_df = spark_df.withColumn("Month", month(spark_df["Date"]).cast(IntegerType()))
spark_df = spark_df.withColumn("DayOfMonth", dayofmonth(spark_df["Date"]).cast(IntegerType()))
spark_df = spark_df.withColumn("DayOfWeek", dayofweek(spark_df["Date"]).cast(IntegerType()))

display(spark_df)



# COMMAND ----------


#spark_df.select('DepartureTime','ArrivalTime').show()

spark_df = spark_df.withColumn("Duration_Minutes", ((col("ArrivalTime").cast("timestamp") - col("DepartureTime").cast("timestamp")) / 60).cast("int"))
spark_df = spark_df.withColumn("Duration_Hours", ((col("ArrivalTime").cast("timestamp") - col("DepartureTime").cast("timestamp")) / 3600).cast("int"))


display(spark_df)

# COMMAND ----------

display(spark_df.where(col("Duration_Minutes").isNotNull()))

# COMMAND ----------

spark_df = spark_df.withColumn("Retard",when(spark_df["Delay"] <= 0, 'Pas de Retard').when(spark_df["Delay"] <= 10, "Retard Court").when(spark_df["Delay"] <= 20, "Retard Moyen").otherwise( 'Long Retard'))

# COMMAND ----------

display(spark_df)

# COMMAND ----------

spark_df.where(spark_df.>4).show()

# COMMAND ----------

spark_df = spark_df.withColumn("hours_DepartureTime", hour(spark_df["DepartureTime"]))

# COMMAND ----------

from pyspark.sql.functions import sum, avg, max

result = spark_df.groupBy("hours_DepartureTime").agg(
    avg("Passengers").alias("sum_Passengers_per_Hours")
)
display(result)
