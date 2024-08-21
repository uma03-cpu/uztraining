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
 

# COMMAND ----------

df.write.saveAsTable("uzdatabricks.bronze.baby_name")
df.write.save("dbfs:/mnt/hexawaredatabricks/raw/output_files/uma/baby_names")
df.write.partitionBy("Year").save("dbfs:/mnt/hexawaredatabricks/raw/output_files/uma/baby_names_year")
df.write.partitionBy("Year","Gender").save("dbfs:/mnt/hexawaredatabricks/raw/output_files/uma/baby_names_year_gender")
