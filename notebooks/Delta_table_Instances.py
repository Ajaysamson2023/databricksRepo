# Databricks notebook source
#DELTA TABLE INSTANCE
#Creating delta table:
from delta.tables import *

DeltaTable.create(spark)\
    .tableName("KR_Vegetable_Market")\
        .addColumn("Vegetable_id","INT")\
            .addColumn("Vegetable_name","STRING")\
                .addColumn("Prize","INT")\
                    .property("description","Fresh Vegetables Marketing")\
                        .location("/FileStore/shared_uploads/ajay.k@diggibyte.com/delta_table_instances")\
                            .execute()

# COMMAND ----------

# MAGIC %sql 
# MAGIC /*Inserting values into delta table*/
# MAGIC insert into KR_Vegetable_Market values(100,"Potato",30);
# MAGIC insert into KR_Vegetable_Market values(200,"Carrot",25);
# MAGIC insert into KR_Vegetable_Market values(300,"Beetroot",40);
# MAGIC insert into KR_Vegetable_Market values(400,"Beans",30);
# MAGIC insert into KR_Vegetable_Market values(500,"Tomato",30);

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from KR_Vegetable_Market

# COMMAND ----------

deltaInstance1=DeltaTable.forPath(spark,"/FileStore/shared_uploads/ajay.k@diggibyte.com/delta_table_instances")

# COMMAND ----------

display(deltaInstance1.toDF())

# COMMAND ----------

# MAGIC %sql 
# MAGIC delete from KR_Vegetable_Market where 
# MAGIC Vegetable_id=400

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from KR_Vegetable_Market

# COMMAND ----------

deltaInstance1.delete("Vegetable_id=300")

# COMMAND ----------



# COMMAND ----------

#DELTA TABLE INSTANCE
#Creating delta table:
from delta.tables import *

DeltaTable.createIfNotExists(spark)\
    .tableName("KR_Vegetable_Market")\
        .addColumn("Vegetable_id","INT")\
            .addColumn("Vegetable_name","STRING")\
                .addColumn("Prize","INT")\
                    .property("description","Fresh Vegetables Marketing")\
                        .location("/FileStore/shared_uploads/ajay.k@diggibyte.com/delta_table_instances")\
                            .execute()

# COMMAND ----------

deltaInstance1=DeltaTable.forPath(spark,"/FileStore/shared_uploads/ajay.k@diggibyte.com/delta_table_instances")

# COMMAND ----------

display(deltaInstance1.toDF())

# COMMAND ----------

deltaInstance2=DeltaTable.forPath(spark,"/FileStore/shared_uploads/ajay.k@diggibyte.com/delta_table_instances")

# COMMAND ----------

display(deltaInstance2.toDF())

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY KR_Vegetable_Market

# COMMAND ----------

display(deltaInstance2.history())

# COMMAND ----------

