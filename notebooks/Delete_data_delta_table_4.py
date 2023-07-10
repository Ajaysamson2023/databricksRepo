# Databricks notebook source
from delta.tables import *

DeltaTable.create(spark)\
    .tableName("employee_details")\
        .addColumn("emp_id","INT")\
            .addColumn("emp_name","STRING")\
                .addColumn("gender","STRING")\
                    .addColumn("salary","INT")\
                        .addColumn("department","STRING")\
                            .property("Description","Details of employee")\
                                .location("/FileStore/shared_uploads/ajay.k@diggibyte.com/deletion_delta_table")\
                                    .execute()

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from employee_details

# COMMAND ----------

# MAGIC %sql 
# MAGIC insert into employee_details values(100,"Ajith","male",23000,"Employee");
# MAGIC insert into employee_details values(200,"Malavi","female",26000,"HR");
# MAGIC insert into employee_details values(300,"Kala","male",24000,"CEO");
# MAGIC insert into employee_details values(400,"Santhosh","male",23000,"CTO");
# MAGIC insert into employee_details values(500,"Mohit","male",23000,"Employee");

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_details

# COMMAND ----------

# MAGIC %sql 
# MAGIC delete from employee_details where emp_id=100

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_details

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_details

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from employee_details order by emp_id

# COMMAND ----------

# MAGIC %sql 
# MAGIC DELETE FROM delta.'/FileStore/shared_uploads/ajay.k@diggibyte.com/tables/delta/employee_details' where emp_id=500

# COMMAND ----------



# COMMAND ----------

