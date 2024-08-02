# Databricks notebook source
from pyspark.sql.types import *
my_schema = StructType([
    StructField("Id",IntegerType()),
    StructField("Name",StringType()),
    StructField("Gender",StringType()),
    StructField("Salary",IntegerType()),
    StructField("Country",StringType()),
    StructField("Date",StringType()),

])

(
    spark.
    readStream.
    schema(my_schema).
    csv("dbfs:/mnt/hexawaredatabricks/raw/stream_in/",header=True).
    writeStream.
    option("checkpointLocation","dbfs:/mnt/hexawaredatabricks/raw/checkpoint/uma/stream").
    trigger(once=True).
    table("uzdatabricks.bronze.stream")

)
