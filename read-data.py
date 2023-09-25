# Databricks notebook source
#Connection configuration
spark.conf.set(
"fs.azure.account.key.yassineessadidatalakeg2.blob.core.windows.net", "gWYEfszXt9mbYAwRbZP0hE3Bo1rZUFJoFw71LWPsENPoEPb5CzWeN28ukbQV6/o3vm6mlyg31lim+ASt3uGX5A==")

df = spark.read.format('csv').option('header', True).load("wasbs://data@yassineessadidatalakeg2.blob.core.windows.net/public_transport_data/raw/public-transport-data.csv")

display(df)
