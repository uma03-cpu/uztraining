# Databricks notebook source
(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation","dbfs:/mnt/db/raw/schemalocation/uma/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/db/raw/checkpoint/uma/autoloader")
.trigger(once=True)
.table("uzdatabricks.bronze.autoloader")
)

# COMMAND ----------

(spark
.readStream
.format("cloudFiles")
.option("cloudFiles.format","csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaEvolutionMode","rescue")
.option("cloudFiles.schemaLocation","dbfs:/mnt/db/raw/schemalocation/uma/autoloader")
.load("dbfs:/mnt/hexawaredatabricks/raw/stream_in/")
.writeStream
.option("checkpointLocation","dbfs:/mnt/db/raw/checkpoint/umaz/autoloader")
.option("mergeSchema",True)
.table("uzdatabricks.bronze.autoloader")
)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uzdatabricks.bronze.autoloader

# COMMAND ----------


