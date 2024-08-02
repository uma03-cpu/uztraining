# Databricks notebook source
# MAGIC %run "/Workspace/Users/uma.zanwar@outlook.com/Databricks training/Hexware Training/Day5/Includes"

# COMMAND ----------

df=spark.read.json("dbfs:/mnt/hexawaredatabricks/raw/input_files/adobe_sample.json",multiLine=True)
 

# COMMAND ----------

df1=df.withColumn("batters",explode("batters.batter"))\
.withColumn("batters_id",col("batters.id"))\
.withColumn("batters_type",col("batters.type"))\
.drop("batters")\
.withColumn("topping",explode("topping"))\
.withColumn("topping_id",col("topping.id"))\
.withColumn("topping_type",col("topping.type"))\
.drop("topping")

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.createOrReplaceTempView("adobe_sample")

# COMMAND ----------

spark.sql("select * from adobe_sample ").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from adobe_sample

# COMMAND ----------

df1.where("batters_type='Chocolate' and topping_id=5001" ).display()
 

# COMMAND ----------

df1.sort(col("topping_id").desc(),"batters_type").show()
