# Databricks notebook source
from delta.tables import *

DeltaTable.create(spark)\
    .tableName("delta_table_1")\
        .addColumn("emp_id","INT")\
            .addColumn("emp_name","STRING")\
                .addColumn("gender","STRING")\
                    .addColumn("Dept","STRING")\
                        .property("description","Table created for practice purpose")\
                            .location("/FileStore/shared_uploads/ajay.k@diggibyte.com/delta_table")\
                                .execute()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/shared_uploads/ajay.k@diggibyte.com/delta_table

# COMMAND ----------

# MAGIC %fs
# MAGIC head /FileStore/shared_uploads/ajay.k@diggibyte.com/delta_table/_delta_log/00000000000000000000.json

# COMMAND ----------

display(spark.read.format("delta").load("/FileStore/shared_uploads/ajay.k@diggibyte.com/delta_table"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from delta_table_1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta_table_1 values(001,"Raja","MALE","IT");

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta_table_1 values(002,"Surya","MALE","employee");
# MAGIC insert into delta_table_1 values(003,"Vijaya","MALE","HR");

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into delta_table_1 values(004,"suresh","MALE","CEO");
# MAGIC insert into delta_table_1 values(005,"mahi","FEMALE","employee");
# MAGIC insert into delta_table_1 values(006,"abilash","MALE","employee");
# MAGIC insert into delta_table_1 values(007,"Raji","FEMALE","employee");
# MAGIC insert into delta_table_1 values(008,"Hari","MALE","employee");
# MAGIC insert into delta_table_1 values(009,"Bala","MALE","employee");

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from delta_table_1  where emp_id=009

# COMMAND ----------

