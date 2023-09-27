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
from pyspark.sql.functions import sum, avg, max

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformation / Cleaning 

# COMMAND ----------

#Connection configuration
spark.conf.set(
    "fs.azure.account.key.yassineessadistorageg2.blob.core.windows.net", "Par6PN6r2BUU9Z4Kzd4ITeN/l4SniXsOR6/Rrtup6LxocPLhWpzv5IxyynGRfT6rOixSc0QH2GUr+AStkS4mXQ==")
def GetFilesByMonth(Month):
    spark.conf.set(
    "fs.azure.account.key.yassineessadistorageg2.blob.core.windows.net", "Par6PN6r2BUU9Z4Kzd4ITeN/l4SniXsOR6/Rrtup6LxocPLhWpzv5IxyynGRfT6rOixSc0QH2GUr+AStkS4mXQ==")
    
    file_path = f"wasbs://data@yassineessadistorageg2.blob.core.windows.net/public_transport_data/raw/Year=2023/{Month}*.csv"
    spark_df = spark.read.format('csv').option('header', True).load(file_path)

    # Add Year Month , Day and convert Date Columns
    spark_df = spark_df.withColumn("Date", col("Date").cast("Date"))
    spark_df = spark_df.withColumn("Year", year(spark_df["Date"]).cast(IntegerType()))
    spark_df = spark_df.withColumn("Month", month(spark_df["Date"]).cast(IntegerType()))
    spark_df = spark_df.withColumn("DayOfMonth", dayofmonth(spark_df["Date"]).cast(IntegerType()))
    spark_df = spark_df.withColumn("DayOfWeek", dayofweek(spark_df["Date"]).cast(IntegerType()))

    # Add Duration Between each Arrival
    spark_df = spark_df.withColumn("Duration_Minutes", ((col("ArrivalTime").cast("timestamp") - col("DepartureTime").cast("timestamp")) / 60).cast("int"))
    spark_df = spark_df.withColumn("Duration_Hours", ((col("ArrivalTime").cast("timestamp") - col("DepartureTime").cast("timestamp")) / 3600).cast("int"))

    # Add rows contains null after calc the Duration time :

    spark_df = spark_df.where(col("Duration_Minutes").isNotNull())

    # Delay Analysis:
    spark_df = spark_df.withColumn("Retard",when(spark_df["Delay"] <= 0, 'Pas de Retard').when(spark_df["Delay"] <= 10, "Retard Court").when(spark_df["Delay"] <= 20, "Retard Moyen").otherwise( 'Long Retard'))
    #Anlytics
    spark_df = spark_df.withColumn("hours_DepartureTime", hour(spark_df["DepartureTime"]))

    spark_df.coalesce(1).write.partitionBy("Year","Month").format("csv").option('header', True).mode("append").save("wasbs://data@yassineessadistorageg2.blob.core.windows.net/public_transport_data/processed/")



# COMMAND ----------

# MAGIC %md
# MAGIC # Automation / WorkFlows

# COMMAND ----------

raw = "wasbs://data@yassineessadistorageg2.blob.core.windows.net/public_transport_data/raw/Year=2023"
processed = "wasbs://data@yassineessadistorageg2.blob.core.windows.net/public_transport_data/processed/Year=2023"

processed_count = 0

files_raw = dbutils.fs.ls(raw)
files_processed = [x.name for x in dbutils.fs.ls(processed)]
for r in files_raw:
    if processed_count == 2:
        break
    if r.name not in files_processed:
        GetFilesByMonth(r.name)
        processed_count += 1

