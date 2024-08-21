# Databricks notebook source
# MAGIC %run "/Workspace/Users/uma.zanwar@outlook.com/Databricks training/Hexware Training/Day5/Includes"

# COMMAND ----------

users_schema=StructType([StructField("Year",IntegerType()),
                         StructField("first_name",StringType()),
                         StructField("County",StringType()),
                         StructField("Gender",StringType()),
                         StructField("Count",IntegerType())
])

# COMMAND ----------

df =spark.read.csv(f"{input}Baby_Names.csv",header=True,schema=users_schema)

# COMMAND ----------

df.count()
 

# COMMAND ----------


df.groupBy("Year").count().orderBy("Year").display()
 
