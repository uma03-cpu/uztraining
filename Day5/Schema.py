# Databricks notebook source
# MAGIC %fs ls dbfs:/databricks-datasets/asa/airlines/
# MAGIC  

# COMMAND ----------

str_schema = "id int, name string, mobile string"

# COMMAND ----------

from pyspark.sql.type import *
ps_schema =StructType([StructField("id",)])
