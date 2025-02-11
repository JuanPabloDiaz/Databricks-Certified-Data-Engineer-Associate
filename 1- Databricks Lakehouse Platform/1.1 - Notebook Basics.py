# Databricks notebook source
# MAGIC %md
# MAGIC Notebooks are popular because they take `markdown`, `python`, `sql`, `r` and `scala` in a single file. its like working on word for coders.

# COMMAND ----------

# MAGIC %md
# MAGIC Notebooks are a powerful tool for coders, combining the flexibility of a document editor with the interactivity of a coding environment. They make it easier to write, execute, and share code, making them a key part of modern coding workflows

# COMMAND ----------

# Python
print("Hello World!")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hello world from SQL!"

# COMMAND ----------

# MAGIC %md
# MAGIC # Title 1
# MAGIC ## Title 2
# MAGIC ### Title 3
# MAGIC
# MAGIC text with a **bold** and *italicized* in it.
# MAGIC
# MAGIC Ordered list
# MAGIC 1. first
# MAGIC 1. second
# MAGIC 1. third
# MAGIC
# MAGIC Unordered list
# MAGIC * coffee
# MAGIC * tea
# MAGIC * milk
# MAGIC
# MAGIC
# MAGIC Images:
# MAGIC ![Associate-badge](https://www.databricks.com/wp-content/uploads/2022/04/associate-badge-eng.svg)
# MAGIC
# MAGIC And of course, tables:
# MAGIC
# MAGIC | user_id | user_name |
# MAGIC |---------|-----------|
# MAGIC |    1    |    Adam   |
# MAGIC |    2    |    Sarah  |
# MAGIC |    3    |    John   |
# MAGIC
# MAGIC Links (or Embedded HTML): <a href="https://docs.databricks.com/notebooks/notebooks-manage.html" target="_blank"> Managing Notebooks documentation</a>

# COMMAND ----------

# MAGIC %md
# MAGIC - You select the **main** language (kernel) for the notebook.
# MAGIC - **Magic commands** (prefixed with % or %%) `provide special functionality within cells`. They do not change the fundamental language of the cell (except for the special case of %md which is shorthand for a Markdown cell).
# MAGIC - To use other languages like SQL or shell commands, you use cell magic (%%sql, %%sh), which tells the notebook to treat the content of that specific cell as code in that other language. Again, this doesn't change the overall notebook language.
# MAGIC
# MAGIC `%md`
# MAGIC `%python`
# MAGIC `%sql`
# MAGIC `%sh`
# MAGIC `%run`
# MAGIC `%fs`

# COMMAND ----------

# MAGIC %run ../Includes/Setup

# COMMAND ----------

print(full_name)

# COMMAND ----------

# MAGIC %md
# MAGIC `%fs` is used to interact with the Databricks File System (DBFS).

# COMMAND ----------

# MAGIC %fs ls '/databricks-datasets'

# COMMAND ----------

dbutils.fs.ls('databricks-datasets/atlas_higgs')

# COMMAND ----------

# MAGIC %md
# MAGIC ### dbutils
# MAGIC This is the main library in Databricks that provides utilities for various tasks
# MAGIC
# MAGIC `dbutils` is more common than `%fs` since we can use _dbutils_ as part of Python code

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

files = dbutils.fs.ls('/databricks-datasets')
print(files)

# COMMAND ----------

# MAGIC %md
# MAGIC use `display()` function to see data in a clean way

# COMMAND ----------

display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC display() function only preview 100.000 records
