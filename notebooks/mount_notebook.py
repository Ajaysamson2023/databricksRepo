# Databricks notebook source
dbutils.fs.help('mount')

# COMMAND ----------

dbutils.fs.mount(
    source = 'wasbs://ajaymountcontainer@ajaystorageaccount.blob.core.windows.net/',
    mount_point = '/mnt/ajblobstorage',
    extra_configs = {'fs.azure.account.key.ajaystorageaccount.blob.core.windows.net':'2q6DfhwMdltlaKkOYmH5V3W2GpSGoEIPbmxDwsgCOwVeGE0E8xMFAxVu6+NII2svCdP4qbGVYl2r+AStbxzRLw=='})

# COMMAND ----------

dbutils.fs.ls('/mnt/ajblobstorage')

# COMMAND ----------

dbutils.fs.cp('/mnt/ajblobstorage/transaction.csv','/mnt/ajblobstorage/data/transaction.csv')

# COMMAND ----------

dbutils.fs.cp('/mnt/ajblobstorage/user.csv','/mnt/ajblobstorage/data/user.csv')

# COMMAND ----------

dbutils.fs.mount(
    source = 'wasbs://ajaymountcontainer@ajaystorageaccount.blob.core.windows.net/',
    mount_point = '/mnt/ajayblobstorage1',
    extra_configs = {'fs.azure.account.key.ajaystorageaccount.blob.core.windows.net':'2q6DfhwMdltlaKkOYmH5V3W2GpSGoEIPbmxDwsgCOwVeGE0E8xMFAxVu6+NII2svCdP4qbGVYl2r+AStbxzRLw=='})

# COMMAND ----------

dbutils.fs.ls('/mnt/ajayblobstorage1')

# COMMAND ----------

dbutils.fs.cp('/mnt/ajayblobstorage1/user.csv','/mnt/ajayblobstorage1/input/user.csv')

# COMMAND ----------

dbutils.fs.cp('/mnt/ajayblobstorage1/transaction.csv','/mnt/ajayblobstorage1/input/transaction.csv')

# COMMAND ----------

dbutils.fs.mount(
    source = 'wasbs://ajaymountcontainer@ajaystorageaccount.blob.core.windows.net/',
    mount_point = '/mnt/ajayblobstorage2',
    extra_configs = {'fs.azure.sas.ajaymountcontainer.ajaystorageaccount.blob.core.windows.net':'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2023-07-03T18:46:53Z&st=2023-07-03T10:46:53Z&spr=https&sig=bLHoSJBQQCDTDKFKdeBXlBlQF3LBLc6eZMMv8zuyq0c%3D'})

# COMMAND ----------

dbutils.fs.ls('/mnt/ajayblobstorage2')

# COMMAND ----------

dbutils.fs.help('mount')

# COMMAND ----------

df=spark.read.csv('/mnt/ajayblobstorage1/user.csv',header=True)
df.show()

# COMMAND ----------

dbutils.fs.unmount('/mnt/ajayblobstorage1') 

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.refreshMounts()

# COMMAND ----------

dbutils.fs.updateMount(
    source = 'wasbs://ajaymountcontainer@ajaystorageaccount.blob.core.windows.net/',
    mount_point = '/mnt/ajayblobstorage2',
    extra_configs = {'fs.azure.sas.ajaymountcontainer.ajaystorageaccount.blob.core.windows.net':'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2023-07-03T18:46:53Z&st=2023-07-03T10:46:53Z&spr=https&sig=bLHoSJBQQCDTDKFKdeBXlBlQF3LBLc6eZMMv8zuyq0c%3D'})

# COMMAND ----------

