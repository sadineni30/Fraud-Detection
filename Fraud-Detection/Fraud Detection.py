# Databricks notebook source
# MAGIC %md
# MAGIC **Fraud Detection in Financial Transactions with Time Windowing**
# MAGIC

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.unmount("/mnt/s3-transaction-history")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

Acess_key = dbutils.secrets.get(scope="secret-scope", key="aws-access-key")
Secret_key = dbutils.secrets.get(scope="secret-scope", key="aws-secret-key")
Bucket_name = "fraud-detection-in-financial-transcations"
Mount_point = "s3-transaction-history"
encoded_secret_key = Secret_key.replace("/", "%2F")
dbutils.fs.mount("s3a://%s:%s@%s" % (Acess_key, encoded_secret_key, Bucket_name), "/mnt/%s" % Mount_point)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/%s" % Mount_point))

# COMMAND ----------

df = spark.read.csv("dbfs:/mnt/s3-transaction-history/fraud_detection_transactions.csv", header=True, inferSchema=True)

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Data cleaning

# COMMAND ----------

cleaned_df = df.filter(df.TransactionAmount > 0).dropna()
display(cleaned_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum, avg, lag, expr
# Data Transformation:
# Use a time-based window to calculate the total transaction amount per customer in the last 7 days.
# Calculate the average transaction amount for the past 30 days using a sliding window.
# Use the lag() function to compare the latest transaction with the previous one for sudden spikes



# Define a window partitioned by customer_id and ordered by transaction_date
window_spec = Window.partitionBy("CustomerID").orderBy("TransactionDate")

# Calculate the total transaction amount in the last 7 days
transformed_data = cleaned_df.withColumn(
    "7_day_total",
    sum("TransactionAmount").over(window_spec.rowsBetween(-6, 0))
)

# Calculate the 30-day moving average
transformed_data = transformed_data.withColumn(
    "30_day_avg",
    avg("TransactionAmount").over(window_spec.rowsBetween(-29, 0))
)

# Compare the latest transaction with the previous one
transformed_data = transformed_data.withColumn(
    "prev_transaction",
    lag("TransactionAmount").over(window_spec)
)

display(transformed_data)

# COMMAND ----------

# Flag suspicious transactions
enriched_data = transformed_data.withColumn(
    "is_suspicious",
    col("TransactionAmount") > 3 * col("30_day_avg")
)

display(enriched_data)

# COMMAND ----------

# Write the enriched data into a Delta table
enriched_data.write.format("delta").mode("overwrite").save("dbfs:/FileStore/FileStore/delta_table")

# COMMAND ----------

# write the data to s3 bucket again
enriched_data.write \
    .format("delta") \
    .mode("overwrite") \
    .save("dbfs:/mnt/s3-transaction-history/fraud_detection_transformed")

# COMMAND ----------

A_key = dbutils.secrets.get(scope = "secret-scope", key = "aws-access-key")
print(A_key)
S_key = dbutils.secrets.get(scope = "secret-scope", key = "aws-secret-key")
print(S_key)

# COMMAND ----------


