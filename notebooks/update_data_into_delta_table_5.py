# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE employee_detail_1(
# MAGIC   emp_id INT,
# MAGIC   emp_name STRING,
# MAGIC   gender STRING,
# MAGIC   salary INT,
# MAGIC   Department STRING)
# MAGIC USING DELTA
# MAGIC LOCATION '/FileStore/shared_uploads/ajay.k@diggibyte.com/update_delta_table'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_detail_1

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employee_detail_1 values (100,"sara","female",20000,"Manager");
# MAGIC insert into employee_detail_1 values (200,"sara","male",23000,"Worker");
# MAGIC insert into employee_detail_1 values (300,"sara","female",26000,"Security");
# MAGIC insert into employee_detail_1 values (400,"sara","female",25000,"Worker");
# MAGIC insert into employee_detail_1 values (500,"sara","female",24000,"Worker");

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from employee_detail_1

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into employee_detail_1 values (600,"selin","female",20000,"Manager");
# MAGIC insert into employee_detail_1 values (700,"saran","male",23000,"Worker");
# MAGIC insert into employee_detail_1 values (800,"praba","female",20000,"Manager");
# MAGIC insert into employee_detail_1 values (900,"mano","male",23000,"Worker");
# MAGIC insert into employee_detail_1 values (1000,"dany","male",23000,"Worker");

# COMMAND ----------

# MAGIC %sql
# MAGIC update employee_detail_1 set salary=10000 where emp_id=300

# COMMAND ----------

