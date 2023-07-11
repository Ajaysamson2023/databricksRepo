# Databricks notebook source
dbutils.fs.mount(
    source = 'wasbs://datastructure@ajaystorageaccount.blob.core.windows.net/',
    mount_point = '/mnt/ajaymountstorage_1',
    extra_configs = {'fs.azure.account.key.ajaystorageaccount.blob.core.windows.net':'2q6DfhwMdltlaKkOYmH5V3W2GpSGoEIPbmxDwsgCOwVeGE0E8xMFAxVu6+NII2svCdP4qbGVYl2r+AStbxzRLw=='})

# COMMAND ----------

df=spark.read.option('header','True').csv('/mnt/ajaymountstorage/Bronze/sample_csv.csv')
display(df)

# COMMAND ----------

df = spark.read.option("multiline", "true").json("/mnt/ajaymountstorage/Bronze/output_sample_json.json")
display(df)

# COMMAND ----------

from pyspark.sql.functions import explode_outer

df2= df.select("id", explode_outer("projects").alias("p")) \
    .select("id", "p.name", "p.status")
display(df2)

# COMMAND ----------

