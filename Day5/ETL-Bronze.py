# Databricks notebook source
# MAGIC %run "/Workspace/Users/uma.zanwar@outlook.com/Databricks training/Hexware Training/Day5/Includes"

# COMMAND ----------

dbutils.widgets.text("environment"," ")
w_text = dbutils.widgets.get("environment")

# COMMAND ----------

df =spark.read.csv(f"{input}1000_richest_people_in_the_world.csv",header=True,inferSchema=True)

# COMMAND ----------

df.display()

# COMMAND ----------

df1 = add_ingestion(df)

# COMMAND ----------

df1.columns

# COMMAND ----------

new_col_list = ['Name', 'Country', 'Industry', 'Net_Worth_in_billions', 'Company','ingestion_date']

# COMMAND ----------

df2 = df1.toDF(*new_col_list)

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

df2.write.mode("overwrite").save(f"{output}uma/richest")

# COMMAND ----------

df3 = df2.withColumn("environment",lit(w_text))

# COMMAND ----------

df3.display()

# COMMAND ----------

df3.write.mode("overwrite").option("mergeSchema",True).save(f"{output}uma/richest")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta.`dbfs:/mnt/hexawaredatabricks/raw/output_files/uma/richest`

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog uzdatabricks;
# MAGIC create schema if not exists bronze;
# MAGIC use schema bronze;

# COMMAND ----------

df3.write.mode("overwrite").option("mergeSchema",True).saveAsTable("richest_bronze")

# COMMAND ----------


