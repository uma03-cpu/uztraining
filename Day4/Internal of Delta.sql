-- Databricks notebook source
Internals
1. parquet files (data)
+
_delta_logs (metadata)
1. crc
2. json
3. parquet_checkpoint

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/mnt/uzhexaware/

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS people10m (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  ssn STRING,
  salary INT
) LOCATION 'dbfs:/mnt/uzhexaware/hexaware/pepple10m'

-- COMMAND ----------

describe history people10m

-- COMMAND ----------

{"commitInfo":{"timestamp":1722330413812,"userId":"2950883358196978","userName":"naval@datamasterconsultingin.onmicrosoft.com","operation":"CREATE TABLE","operationParameters":{"partitionBy":"[]","description":null,"isManaged":"false","properties":"{\"delta.enableDeletionVectors\":\"true\"}","statsOnLoad":false},"notebook":{"notebookId":"1944968013109850"},"clusterId":"0720-061852-wd0b5onj","isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{},"tags":{"restoresDeletedRows":"false"},"engineInfo":"Databricks-Runtime/14.3.x-scala2.12","txnId":"08360905-ebec-4127-92f3-65749dd6f8d9"}}
{"metaData":{"id":"ab161361-a808-4a68-8f36-a169add19d98","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"firstName\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"middleName\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"lastName\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"gender\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"birthDate\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ssn\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"salary\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{"delta.enableDeletionVectors":"true"},"createdTime":1722330412669}}
{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}


-- COMMAND ----------

Every transcation you make on delta table there is log generated. 

-- COMMAND ----------

INsert into people10m values (1,'a','b','z','M','2024-07-30','123',1000)

-- COMMAND ----------

describe history people10m

-- COMMAND ----------

INsert into people10m values (2,'a','b','z','M','2024-07-30','123',1000),
                                                 (3,'a','b','z','M','2024-07-30','123',2000)

-- COMMAND ----------

{"commitInfo":{"timestamp":1722331111537,"userId":"2950883358196978","userName":"naval@datamasterconsultingin.onmicrosoft.com","operation":"WRITE","operationParameters":{"mode":"Append","statsOnLoad":false,"partitionBy":"[]"},"notebook":{"notebookId":"1944968013109850"},"clusterId":"0720-061852-wd0b5onj","readVersion":1,"isolationLevel":"WriteSerializable","isBlindAppend":true,"operationMetrics":{"numFiles":"1","numOutputRows":"2","numOutputBytes":"2282"},"tags":{"restoresDeletedRows":"false"},"engineInfo":"Databricks-Runtime/14.3.x-scala2.12","txnId":"76c907d0-6301-45a4-95c0-b9de11a7cd17"}}
{"add":{"path":"part-00000-5089ec7b-5c4a-4c8c-9270-8c86e73ed4eb-c000.snappy.parquet","partitionValues":{},"size":2282,"modificationTime":1722331111000,"dataChange":true,"stats":"{\"numRecords\":2,\"minValues\":{\"id\":2,\"firstName\":\"a\",\"middleName\":\"b\",\"lastName\":\"z\",\"gender\":\"M\",\"birthDate\":\"2024-07-30T00:00:00.000Z\",\"ssn\":\"123\",\"salary\":1000},\"maxValues\":{\"id\":3,\"firstName\":\"a\",\"middleName\":\"b\",\"lastName\":\"z\",\"gender\":\"M\",\"birthDate\":\"2024-07-30T00:00:00.000Z\",\"ssn\":\"123\",\"salary\":2000},\"nullCount\":{\"id\":0,\"firstName\":0,\"middleName\":0,\"lastName\":0,\"gender\":0,\"birthDate\":0,\"ssn\":0,\"salary\":0},\"tightBounds\":true}","tags":{"INSERTION_TIME":"1722331111000000","MIN_INSERTION_TIME":"1722331111000000","MAX_INSERTION_TIME":"1722331111000000","OPTIMIZE_TARGET_SIZE":"268435456"}}}


-- COMMAND ----------

delete from people10m where id= 1

-- COMMAND ----------

select * from people10m

-- COMMAND ----------

delete from people10m where id= 2

-- COMMAND ----------

update people10m
SET salary=5000
where id= 3

-- COMMAND ----------

INsert into people10m values (5,'a','b','z','M','2024-07-30','123',1000);
INsert into people10m values (8,'a','b','z','M','2024-07-30','123',1000);
INsert into people10m values (9,'a','b','z','M','2024-07-30','123',2000);
INsert into people10m values (27,'a','b','z','F','2024-07-30','123',1000);
INsert into people10m values (25,'a','b','z','M','2024-07-30','123',5000);
INsert into people10m values (21,'a','b','z','M','2024-07-30','123',1000);
INsert into people10m values (21,'a','b','z','F','2024-07-30','123',1000);
INsert into people10m values (222,'a','b','z','M','2024-07-30','123',5000);
INsert into people10m values (236,'a','b','z','M','2024-07-30','123',1000);
INsert into people10m values (26,'a','b','z','F','2024-07-30','123',1500);

-- COMMAND ----------

optimize people10m
zorder by (id)

-- COMMAND ----------

select * from people10m

-- COMMAND ----------

describe history people10m

-- COMMAND ----------

select * from people10m version as of 11

-- COMMAND ----------

select * from people10m timestamp as of '2024-07-30T11:29:30.000+00:00'

-- COMMAND ----------

delete from people10m

-- COMMAND ----------

select * from people10m

-- COMMAND ----------

Restore table people10m version as of 11

-- COMMAND ----------

vacuum people10m

-- COMMAND ----------

Retention period is 7 days= 168 hours

-- COMMAND ----------

vacuum people10m retain 0 hours

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

vacuum people10m retain 0 hours dry run

-- COMMAND ----------

vacuum people10m retain 0 hours 

-- COMMAND ----------

select * from people10m version as of 3

-- COMMAND ----------

describe history people10m
