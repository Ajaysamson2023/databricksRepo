-- Databricks notebook source
-- MAGIC %sql
-- MAGIC Create WIDGET TEXT firstname DEFAULT 'Sunil'

-- COMMAND ----------

CREATE WIDGET DROPDOWN gender DEFAULT 'male'CHOICES SELECT 'male'

-- COMMAND ----------

REMOVE WIDGET gender

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC data=[(1,'sunil','male'),(2,'santhosh','male'),(3,'Annu','female'),(4,'Raji','female')]
-- MAGIC column=['id','name','gender']
-- MAGIC df=spark.createDataFrame(data,column)
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC df.createOrReplaceTempView('persons')

-- COMMAND ----------

SELECT * from persons

-- COMMAND ----------

CREATE WIDGET DROPDOWN genderDD DEFAULT 'male' CHOICES SELECT DISTINCT gender from persons

-- COMMAND ----------

REMOVE WIDGET gender

-- COMMAND ----------



-- COMMAND ----------

SELECT * FROM persons WHERE gender=getargument('genderDD')

-- COMMAND ----------

SELECT * from persons WHERE gender=('$genderDD')

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC data=[(1,'prakash','male'),(2,'ramesh','male'),(3,'arun','female'),(4,'sathya','female'),(5,'mahesh','male'),(6,'Abitha','female'),(7,'pandiyan','male'),(8,'Siva kumar','male')]
-- MAGIC column=['id','name','gender']
-- MAGIC df=spark.createDataFrame(data,column)
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC df.createOrReplaceTempView('Datas_person')

-- COMMAND ----------

SELECT * FROM Datas_person

-- COMMAND ----------

CREATE WIDGET DROPDOWN genderDD DEFAULT 'male' CHOICES SELECT  gender from Datas_person

-- COMMAND ----------

SELECT * from persons WHERE gender=('$genderDD')

-- COMMAND ----------

SELECT * from persons WHERE gender=('$genderDD')