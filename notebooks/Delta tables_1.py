# Databricks notebook source
METHOD_1:DELTA TABLE USING PYSPARK

# COMMAND ----------

from delta.tables import *

DeltaTable.create(spark)\
    .tableName("SRM_College_Details")\
        .addColumn("class_id","INT")\
            .addColumn("Name","STRING")\
                .addColumn("Age","INT")\
                    .addColumn("Dept","STRING")\
                        .addColumn("College","STRING")\
                            .property("Description","College Students Details")\
                                .location("/FileStore/shared_uploads/ajay.k@diggibyte.com/delta_table_1")\
                                    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from srm_college_details

# COMMAND ----------

from delta.tables import *

DeltaTable.createIfNotExists(spark)\
    .tableName("SRM_College_Details")\
        .addColumn("class_id","INT")\
            .addColumn("Name","STRING")\
                .addColumn("Age","INT")\
                    .addColumn("Dept","STRING")\
                        .addColumn("College","STRING")\
                            .execute()

# COMMAND ----------

from delta.tables import *

DeltaTable.createOrReplace(spark)\
    .tableName("SRM_College_Details")\
        .addColumn("class_id","INT")\
            .addColumn("Name","STRING")\
                .addColumn("Age","INT")\
                    .addColumn("Dept","STRING")\
                        .addColumn("College","STRING")\
                            .property("Description","College Students Details")\
                                .location("/FileSt
                                          ore/shared_uploads/ajay.k@diggibyte.com/delta_table_1")\
                                    .execute()

# COMMAND ----------

METHOD_2: DELTA TABLE USING SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE STUDENTS_DETAILS(
# MAGIC   class_id INT,
# MAGIC   Name STRING,
# MAGIC   Age INT,
# MAGIC   Dept STRING,
# MAGIC   College STRING) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS  STUDENTS_DETAILS(
# MAGIC   class_id INT,
# MAGIC   Name STRING,
# MAGIC   Age INT,
# MAGIC   Dept STRING,
# MAGIC   College STRING) USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE STUDENTS_DETAILS(
# MAGIC   class_id INT,
# MAGIC   Name STRING,
# MAGIC   Age INT,
# MAGIC   Dept STRING,
# MAGIC   College STRING)USING DELTA

# COMMAND ----------

METHOD_3:DELTA TABLE USING DATAFRAME

# COMMAND ----------

student_schema=["Class_id","Name","Age","Dept","College"]
student_data=[(100,"Raji",23,"CS","SRM"),(200,"Suresh",24,"CS","VIT"),
              (300,"Ram",25,"BCA","LOYOLA"),(400,"Mani",26,"CS","SRM")]

df=spark.createDataFrame(data=student_data,schema=student_schema)
display(df)


# COMMAND ----------

df.write.format("delta").saveAsTable("Student_Details_1")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from Student_Details_1

# COMMAND ----------

