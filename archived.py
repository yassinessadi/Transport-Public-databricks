# Databricks notebook source
# Databricks notebook source
from datetime import datetime
from pyspark.sql.functions import dayofmonth

#Connection configuration
spark.conf.set(
    "fs.azure.account.key.yassineessadistorageg2.blob.core.windows.net", "Par6PN6r2BUU9Z4Kzd4ITeN/l4SniXsOR6/Rrtup6LxocPLhWpzv5IxyynGRfT6rOixSc0QH2GUr+AStkS4mXQ==")

raw = "wasbs://data@yassineessadistorageg2.blob.core.windows.net/public_transport_data/raw/Year=2023"
archived = "wasbs://data@yassineessadistorageg2.blob.core.windows.net/public_transport_data/raw/Year=2023"

raw_files = dbutils.fs.ls(raw)
archived_files = dbutils.fs.ls(archived)

#Archived data to folder archive
for r in raw_files:
    modification_time_ms = r.modificationTime
    modification_time = datetime.fromtimestamp(modification_time_ms / 1000)  # Divide by 1000 to convert milliseconds to seconds
    datenow = datetime.now()
    duration =  datenow - modification_time
    if(duration.days > 15):
        #Archive data
        dbutils.fs.cp(r.path,'wasbs://data@yassineessadistorageg2.blob.core.windows.net/public_transport_data/archived/'+r.name,recurse=True)
        #Delete data after archive
        dbutils.fs.rm(r.path, recurse=True)
        print('Folder : '+r.name+' archived with successfuly!')

#Deleted data from folder archive
for r in archived_files:
    modification_time_ms = r.modificationTime
    modification_time = datetime.fromtimestamp(modification_time_ms / 1000)  # Divide by 1000 to convert milliseconds to seconds
    datenow = datetime.now()
    duration =  datenow - modification_time
    if(duration.days > 30):
        #Delete data from folder archive
        dbutils.fs.rm(r.path, recurse=True)
        print('Folder : '+r.name+' deleted with successfuly!')



