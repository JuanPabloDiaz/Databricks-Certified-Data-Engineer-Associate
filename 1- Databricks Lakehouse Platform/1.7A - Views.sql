-- Databricks notebook source
-- MAGIC %md
-- MAGIC **view** is a `virtual table`
-- MAGIC
-- MAGIC NOT PHYSICAL DATA
-- MAGIC
-- MAGIC View is just a SQL Query

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Preparing Sample Data

-- COMMAND ----------

USE CATALOG hive_metastore;

CREATE TABLE IF NOT EXISTS smartphones
(id INT, name STRING, brand STRING, year INT);

INSERT INTO smartphones
VALUES (1, 'iPhone 14', 'Apple', 2022),
      (2, 'iPhone 13', 'Apple', 2021),
      (3, 'iPhone 6', 'Apple', 2014),
      (4, 'iPad Air', 'Apple', 2013),
      (5, 'Galaxy S22', 'Samsung', 2022),
      (6, 'Galaxy Z Fold', 'Samsung', 2022),
      (7, 'Galaxy S9', 'Samsung', 2016),
      (8, '12 Pro', 'Xiaomi', 2022),
      (9, 'Redmi 11T Pro', 'Xiaomi', 2022),
      (10, 'Redmi Note 11', 'Xiaomi', 2021)

-- COMMAND ----------

INSERT INTO smartphones
VALUES (11, 'iPhone 14 Pro', 'Apple', 2022);

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC > Types of views...

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![](./viewsComparison.jpg)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating Stored Views

-- COMMAND ----------

CREATE VIEW view_apple_phones
AS  SELECT * 
    FROM smartphones 
    WHERE brand = 'Apple';

-- COMMAND ----------

SELECT * FROM view_apple_phones;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Creating Temporary Views

-- COMMAND ----------

CREATE TEMP VIEW temp_view_phones_brands
AS  SELECT DISTINCT brand
    FROM smartphones;

SELECT * FROM temp_view_phones_brands;

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC its a temporary object and its not persistance in database

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Creating Global Temporary Views

-- COMMAND ----------

CREATE GLOBAL TEMP VIEW global_temp_view_latest_phones
AS SELECT * FROM smartphones
    WHERE year > 2020
    ORDER BY year DESC;

-- COMMAND ----------

SELECT * FROM global_temp.global_temp_view_latest_phones;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC _global_temp_ need to be added to a select statement

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC global view is not listed

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - _SHOW TABLES_ ---> list of tables.
-- MAGIC - _DESCRIBE TABLE_ gives you details about a specific table's structure. (metadata)
