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

display(dbutils.fs.ls('/'))

# COMMAND ----------

 df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/ajay.k@diggibyte.com/user.csv") 

# COMMAND ----------

display(df1)

# COMMAND ----------

column=["id","name"]
data=[(1,"Rajesh"),(2,"Bala"),(3,"Sunil")]
df=spark.createDataFrame(data,column)
display(df)

# COMMAND ----------

dbutils.data.summarize(df)

# COMMAND ----------

dbutils.fs.help("cp")

# COMMAND ----------

dbutils.fs.cp('/FileStore/shared_uploads/ajay.k@diggibyte.com/user.csv','/FileStore/shared_uploads/ajay.k@diggibyte.com/user_data/user.csv')

# COMMAND ----------

dbutils.fs.head('/FileStore/shared_uploads/ajay.k@diggibyte.com/user_data/user.csv',25)

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/data_user/')

# COMMAND ----------

dbutils.fs.rm('/FileStore/data_user/')

# COMMAND ----------

display(dbutils.fs.ls('/FileStore/'))

# COMMAND ----------

dbutils.fs.mv('/FileStore/cp_cmd/Product.csv','/FileStore/shared_uploads/ajay.k@diggibyte.com')

# COMMAND ----------

dbutils.fs.put('/FileStore/cp_cmd/demo_1.txt','Hello world! This is Databricks Course practice')

# COMMAND ----------

dbutils.fs.head('/FileStore/cp_cmd/demo_1.txt')

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

dbutils.notebook.help('exit')

# COMMAND ----------

firstvalues='This is Databricks Practicing exit commands'
dbutils.notebook.exit(firstvalues)

# COMMAND ----------

lastvalues='This is the last value'
print(lastvalues)

# COMMAND ----------

 dbutils.notebook.help('run')

# COMMAND ----------

dbutils.notebook.run('FileStore/shared_uploads/ajay.k@diggibyte.com/Product.csv',60)

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.combobox(name='FruitsCB',defaultValue='Apple',choices=['Apple','Orange','Mango','Banana'],label='Fruits ComboBox')

# COMMAND ----------

dbutils.widgets.dropdown(name='FruitsDD',defaultValue='Apple',choices=['Apple','Orange','Mango','Banana'],label='Fruits Dropdown')

# COMMAND ----------

dbutils.widgets.multiselect(name='FruitsMS',defaultValue='Apple',choices=['Apple','Orange','Mango','Banana'],label='Fruits Multi-select')

# COMMAND ----------

dbutils.widgets.text(name='FruitsTB',defaultValue='Apple',label='Fruits Text Box')

# COMMAND ----------

dbutils.widgets.get('FruitsCB')
dbutils.widgets.get('FruitsMS')

# COMMAND ----------

dbutils.widgets.getArgument('FruitsMS1','error:This widgets is not available')


# COMMAND ----------

dbutils.widgets.remove('FruitsMS1')


# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

