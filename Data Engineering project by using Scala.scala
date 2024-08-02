// Databricks notebook source
// MAGIC %md
// MAGIC # Goal
// MAGIC - customer who bought airpod after buying iphone
// MAGIC - customes who bought both iPhone and airpods
// MAGIC - %run "./motebook_name --to run parallel notebook with this notebook like requirement or reference

// COMMAND ----------

import org.apache.spark.sql.{Row,SparkSession}
import org.apache.spark.sql.functions._


// COMMAND ----------

val read_dfcust = spark.read.format("delta")
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .load("dbfs:/user/hive/warehouse/customer_updated")

// COMMAND ----------

val read_dfprod = spark.read.format("delta")
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .load("dbfs:/user/hive/warehouse/products_updated")

// COMMAND ----------

val read_dftrans = spark.read.format("delta")
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .load("dbfs:/user/hive/warehouse/transaction_updated")

// COMMAND ----------

read_dfcust.show(truncate=true)

// COMMAND ----------

read_dfprod.show()

// COMMAND ----------

read_dftrans.show()

// COMMAND ----------

// MAGIC %md customer who bought airpod after buying iphone

// COMMAND ----------

import org.apache.spark.sql.functions.col
//after joining there are lot of problrm occur take only that column that u want to solve bussinees logic so us eselect clause
val groupcust_trans = read_dfcust
  .join(read_dftrans, read_dfcust("customer_id") === read_dftrans("customer_id"), "inner")
  .select(read_dfcust("customer_id"),read_dfcust("customer_name"),read_dftrans("transaction_date"),read_dftrans("product_name"))
  .orderBy(read_dftrans("transaction_date"))

// COMMAND ----------

groupcust_trans.show()

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lead}

val window1 = Window.partitionBy("customer_id").orderBy("transaction_date")
val lead1 = groupcust_trans.withColumn("lead_product_name", lead(col("product_name"), 1).over(window1))

// COMMAND ----------

lead1.show()

// COMMAND ----------

val airpodafteriphone=lead1.filter(col("product_name")==="iPhone" && col("lead_product_name")==="AirPods")

// COMMAND ----------

val result1=airpodafteriphone.select(col("customer_id"),col("customer_name"),col("transaction_date"))

// COMMAND ----------

result1.show()

// COMMAND ----------

temp_table_name="customer_delta_table"
result1.createOrReplaceTempView(temp_table_name)

// COMMAND ----------

result1.write.format("parquet").saveAsTable("temp_table_name")
//if we want to store by location then we use partition bylocation it will create diff file accordingly

// COMMAND ----------

// MAGIC %md customes who bought both iPhone and airpods

// COMMAND ----------

val bothas_group = groupcust_trans.groupBy("customer_name").agg(collect_list("product_name").alias("product_buy"))

// COMMAND ----------

bothas_group.show()

// COMMAND ----------

import org.apache.spark.sql.functions.{array_contains, col, size}

val filterboth = bothas_group
  .filter(array_contains(col("product_buy"), "AirPods") &&
          array_contains(col("product_buy"), "iPhone") &&
          size(col("product_buy")) === 2)

// COMMAND ----------

filterboth.show()
