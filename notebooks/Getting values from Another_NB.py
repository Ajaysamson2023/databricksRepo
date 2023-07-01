# Databricks notebook source
dbutils.notebook.help('run')

# COMMAND ----------

dbutils.notebook.run('/Users/ajay.k@diggibyte.com/NB_Passing values_from_Another_NB',120,{'Paramcombobox':'combobox','Paramdropdown':'dropdownBox','ParamMultiselect':'MultiselectBox','ParamText':'TextBox'})

# COMMAND ----------

dbutils.notebook.run('/Users/ajay.k@diggibyte.com/NB_Passing values_from_Another_NB',120,{'Paramcombobox':'Apple','Paramdropdown':'Orange','ParamMultiselect':'Banana','ParamText':'Mango'})