# Databricks notebook source
dbutils.widgets.help()
dbutils.help('widgets')

# COMMAND ----------

dbutils.widgets.combobox(name='Paramcombobox',defaultValue='apple',choices=['apple','mango','orange'],label='Fruits ComboBox')
dbutils.widgets.dropdown(name='Paramdropdown',defaultValue='apple',choices=['apple','mango','orange'],label='Fruits ropdown')
dbutils.widgets.multiselect(name='ParamMultiselect',defaultValue='apple',choices=['apple','mango','orange'],label='Fruits Multiselect')
dbutils.widgets.text(name='ParamText',defaultValue='apple',label='Fruits TextBox')

# COMMAND ----------

print('ComboBox values is', dbutils.widgets.get('Paramcombobox'))
print('Dropdown values is', dbutils.widgets.get('Paramdropdown'))
print('Multiselect values is',dbutils.widgets.get('ParamMultiselect'))
print('TextBox values is', dbutils.widgets.get('ParamText'))