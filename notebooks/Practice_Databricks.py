# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.data.help()
dbutils.fs.help()
dbutils.widgets.help()
dbutils.notebook.help()

# COMMAND ----------

dbutils.help('fs')

# COMMAND ----------

dbutils.data.help('summarize')

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/Product.csv","dbfs:/FileStore/cp_cmd/Product.csv")

# COMMAND ----------

dbutils.fs.head("dbfs:/FileStore/textFile1.txt")

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/new_directory")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/new_directory")

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/cp_cmd")

# COMMAND ----------

dbutils.fs.mv()