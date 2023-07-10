# Databricks notebook source
from delta.tables import *

DeltaTable.create(spark)\
    .tableName("DBC_College")\
        .addColumn("class_id","INT")\
            .addColumn("Name","STRING")\
                .addColumn("Age","INT")\
                    .addColumn("Dept","STRING")\
                        .property("Description","College Students Details")\
                            .location("/FileStore/shared_uploads/ajay.k@diggibyte.com/delta_tables_3")\
                                .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from DBC_College

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into DBC_College values(100,"Ramesh",23,"MCA","DBC")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from DBC_College

# COMMAND ----------

display(spark.sql("select * from DBC_College "))

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

student_data=[(200,"Mani",24,"MBA")]

student_schema=StructType([
    StructField("class_id",IntegerType(),True),\
    StructField("Name",StringType(),True),\
    StructField("Age",IntegerType(),True),\
    StructField("College",StringType(),True)\
    ])
df=spark.createDataFrame(data=student_data,schema=student_schema)
display(df)

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("DBC_College")

# COMMAND ----------

display(spark.sql("select * from DBC_College "))

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

student_data=[(300,"Sara",24,"MCA")]

student_schema=StructType([
    StructField("class_id",IntegerType(),True),\
    StructField("Name",StringType(),True),\
    StructField("Age",IntegerType(),True),\
    StructField("College",StringType(),True)\
    ])
df=spark.createDataFrame(data=student_data,schema=student_schema)
display(df)

# COMMAND ----------



# COMMAND ----------

from delta.tables import *

DeltaTable.create(spark)\
    .tableName("DBC_College")\
        .addColumn("class_id","INT")\
            .addColumn("Name","STRING")\
                .addColumn("Age","INT")\
                    .addColumn("Dept","STRING")\
                        .property("Description","College Students Details")\
                            .location("/FileStore/shared_uploads/ajay.k@diggibyte.com/delta_tables_3")\
                                .execute()
 

# COMMAND ----------

# MAGIC  %sql
# MAGIC select * from DBC_College

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into DBC_College values(400,"Bala",33,"MSC")

# COMMAND ----------

# MAGIC  %sql
# MAGIC select * from DBC_College

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

student_data=[(500,"Arun",24,"BBA")]

student_schema=StructType([
    StructField("class_id",IntegerType(),True),\
    StructField("Name",StringType(),True),\
    StructField("Age",IntegerType(),True),\
    StructField("College",StringType(),True)\
    ])
df=spark.createDataFrame(data=student_data,schema=student_schema)
display(df)

# COMMAND ----------

df.write.format("delta").mode("append").saveAsTable("DBC_College")

# COMMAND ----------

display(spark.sql("select * from DBC_College "))

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

student_data=[(500,"Arun",24,"BBA","DBC",)]

student_schema=StructType([
    StructField("class_id",IntegerType(),True),\
    StructField("Name",StringType(),True),\
    StructField("Age",IntegerType(),True),\
    StructField("dept",StringType(),True),\
    StructField("College",StringType(),True),\
    ])
df=spark.createDataFrame(data=student_data,schema=student_schema)
display(df)

# COMMAND ----------

display(spark.sql("select * from DBC_College "))


# COMMAND ----------

df.write.insertInto("DBC_College",overwrite=False)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from DBC_College

# COMMAND ----------

df.createOrReplaceTempView("delta_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_data

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into DBC_College
# MAGIC select * from delta_data
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  DBC_College
# MAGIC

# COMMAND ----------

spark.sql("insert into DBC_College select * from delta_data")

# COMMAND ----------

