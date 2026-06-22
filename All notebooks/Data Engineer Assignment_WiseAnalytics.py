# Databricks notebook source
print("hi")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/"))

# COMMAND ----------

# Copy file from DBFS to local filesystem
dbutils.fs.cp("dbfs:/FileStore/tables/online_retail.zip", "file:/tmp/online_retail.zip", True)

print("File copied to local storage.")

# COMMAND ----------

import zipfile

# Define paths
local_zip_path = "/tmp/online_retail.zip"  # Now it's in local storage
local_extract_path = "/tmp/online_retail_extracted/"  # Extract locally first

# Extract ZIP file
with zipfile.ZipFile(local_zip_path, "r") as zip_ref:
    zip_ref.extractall(local_extract_path)

print("Extraction complete!")

# COMMAND ----------

# Move extracted CSV back to DBFS
dbutils.fs.cp("file:/tmp/online_retail_extracted/Online Retail.xlsx", "dbfs:/tmp/online_retail_extracted/Online Retail.csv", True)

print("Extracted file moved to DBFS!")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/"))

# COMMAND ----------

!pip install openpyxl

# COMMAND ----------

# Copy file from DBFS to a local temporary directory
dbutils.fs.cp("dbfs:/FileStore/tables/Online_Retail-1.xlsx", "file:/tmp/Online_Retail-1.xlsx")

import pandas as pd
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("OnlineRetailETL").getOrCreate()

# Define new path after copying
local_path = "/tmp/Online_Retail-1.xlsx"

# Read Excel file using pandas
pandas_df = pd.read_excel(local_path, sheet_name=0)

# Convert pandas DataFrame to Spark DataFrame
df = spark.createDataFrame(pandas_df)

# Show first few rows
df.show(truncate = 5)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.count()

# COMMAND ----------

df1 = df.dropDuplicates()

# COMMAND ----------

df1.count()

# COMMAND ----------

df_cleaned = df1.dropna(subset=["InvoiceNo", "StockCode", "Quantity", "InvoiceDate", "UnitPrice"])

# COMMAND ----------

df_cleaned.count()

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.types import IntegerType, DoubleType, StringType, TimestampType, StructField, StructType

df_cleaned = df_cleaned.withColumn("InvoiceNo", col("InvoiceNo").cast(StringType())) \
                   .withColumn("StockCode", col("StockCode").cast(StringType())) \
                   .withColumn("Quantity", col("Quantity").cast(IntegerType())) \
                   .withColumn("InvoiceDate", col("InvoiceDate").cast(TimestampType())) \
                   .withColumn("UnitPrice", col("UnitPrice").cast(DoubleType())) \
                   .withColumn("CustomerID", col("CustomerID").cast(IntegerType())) \
                   .withColumn("Country", col("Country").cast(StringType()))
                   .withColumn("Country", col("Country").cast(StringType()))

df_cleaned.printSchema()

# COMMAND ----------

# 1. Required fields validation

# Define required columns for fields validations
required_columns = ["InvoiceNo", "StockCode", "Quantity", "InvoiceDate", "UnitPrice"]

# Identify columns those having NULL values
null_counts = {col_name: df_cleaned.filter(col(col_name).isNull()).count() for col_name in required_columns}

# Filter only columns that have NULL values
columns_with_nulls = {col_name: count for col_name, count in null_counts.items() if count > 0}

# Display columns that have NULL values
if columns_with_nulls:
    for col_name, count in columns_with_nulls.items():
        print(f"Column {col_name} has {count} missing values.")
else:
    print("No NULL values found in required fields.")



# COMMAND ----------

# 2. Data type verification

# Define expected data types
expected_datatypes = {
    "InvoiceNo": StringType(),
    "StockCode": StringType(),
    "Quantity": IntegerType(),
    "InvoiceDate": TimestampType(),
    "UnitPrice": DoubleType(),
    "CustomerID": IntegerType(),
    "Country": StringType()
}

# Identify incorrect columns
incorrect_columns = []
for col_name, expected_type in expected_datatypes.items():
    actual_type = df_cleaned.schema[col_name].dataType
    if not isinstance(actual_type, type(expected_type)):
        incorrect_columns.append(col_name)
        print(f"Column {col_name} has incorrect type: {actual_type}. Expected: {expected_type}")

# Convert incorrect columns to expected data types dynamically
for col_name in incorrect_columns:
    df_cleaned = df_cleaned.withColumn(col_name, col(col_name).cast(expected_datatypes[col_name]))

# Display final schema after correction
df_cleaned.printSchema()


# COMMAND ----------

# 3. Basic business rules (e.g., positive quantities)
# Filter records where Quantity and UnitPrice are positive
df_positive = df_cleaned.filter((col("Quantity") > 0) & (col("UnitPrice") > 0))
df_invalid = df_cleaned.filter((col("Quantity") <= 0) & (col("UnitPrice") <= 0))

# Show the positive records
print(f"Total valid records with positive Quantity & UnitPrice: {df_positive.count()}")
print(f"Total valid records with positive Quantity & UnitPrice: {df_invalid.count()}")
df_positive.show(truncate=False)
df_negetive.show(truncate=False)

# Save the cleaned data for further processing
# df_positive.write.mode("overwrite").csv("dbfs:/mnt/data/clean/positive_records.csv")


# COMMAND ----------

display(df_cleaned)

# COMMAND ----------

display(df_positive)

# COMMAND ----------

output_path = "file:///C:/Users/subba/csv_files"  # Use forward slashes `/`

df_cleaned.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print(f"Files saved in folder: {output_path}")



# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark
spark = SparkSession.builder.appName("DataQualityChecks").getOrCreate()

# Define the error log schema explicitly
error_schema = StructType([
    StructField("Column", StringType(), True),
    StructField("Issue", StringType(), True),
    StructField("Sample_Value", StringType(), True)
])

# Create an empty DataFrame with the correct schema
error_log = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=error_schema)

# Define expected data types
expected_datatypes = {
    "InvoiceNo": StringType(),
    "StockCode": StringType(),
    "Description": StringType(),
    "Quantity": IntegerType(),
    "InvoiceDate": TimestampType(),  
    "UnitPrice": IntegerType(), # Assume it is integertype
    "CustomerID": IntegerType(),
    "Country": StringType()
}

# Assume df_cleaned is already loaded and cleaned
df_cleaned.show(5)  # Just to verify initial data

# 1️⃣ Required Field Validation (Check NULLs)
required_fields = ["InvoiceNo", "StockCode", "Quantity", "InvoiceDate", "UnitPrice"]
for field in required_fields:
    df_nulls = df_cleaned.filter(col(field).isNull()).select(
        lit(field).alias("Column"),
        lit("NULL value found").alias("Issue"),
        lit(None).cast(StringType()).alias("Sample_Value")
    )
    error_log = error_log.union(df_nulls)

# 2️⃣ Data Type Validation
for col_name, expected_type in expected_datatypes.items():
    actual_type = df_cleaned.schema[col_name].dataType
    if not isinstance(actual_type, type(expected_type)):
        error_entry = [(col_name, f"Expected {expected_type}, found {actual_type}", None)]
        error_df = spark.createDataFrame(error_entry, schema=error_schema)
        error_log = error_log.union(error_df)

# 3️⃣ Business Rule Validation (Quantity & UnitPrice must be positive)
df_invalid_business = df_cleaned.filter((col("Quantity") <= 0) | (col("UnitPrice") <= 0)) \
    .select(
        lit("Quantity or UnitPrice").alias("Column"),
        lit("Must be positive").alias("Issue"),
        col("Quantity").cast(StringType()).alias("Sample_Value")  # Convert to string for logging
    )
error_log = error_log.union(df_invalid_business)

# Show Final Error Log
if error_log.count() > 0:
    print("Errors Found! Logging them...")
    error_log.show(truncate=False)
else:
    print("No Errors Found")



# COMMAND ----------

from pyspark.sql import SparkSession

# Set the path where you moved the JDBC JAR file
jdbc_driver_path = r"C:\Users\subba\mssql-jdbc-12.8.1.jre11.jar"  # Windows

# Initialize SparkSession with JDBC driver
spark = SparkSession.builder \
    .appName("SQLServerIntegration") \
    .config("spark.jars", jdbc_driver_path) \
    .getOrCreate()

print("SparkSession initialized successfully!")


# COMMAND ----------

jdbc_url = "jdbc:sqlserver://CHANTI_LOCAL:1433;databaseName=OnlineRetailDB;encrypt=false"

properties = {
    "user": "venkat",
    "password": "Azuresql1234@",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

df = spark.read.jdbc(url=jdbc_url, table="dbo.online_retail", properties=properties)
df.show()


# COMMAND ----------

from pyspark.sql import SparkSession

# Define the JDBC driver path
jdbc_driver_path = r"C:\Users\subba\mssql-jdbc-12.8.1.jre11.jar"

# Create a Spark session
spark = SparkSession.builder \
    .appName("SQLServerConnection") \
    .config("spark.jars", jdbc_driver_path) \
    .getOrCreate()


# COMMAND ----------

from fastapi import FastAPI, HTTPException
import pymssql
import logging

# Enable logging
logging.basicConfig(level=logging.DEBUG)

# Initialize FastAPI app
app = FastAPI()

# Database connection details
server = "CHANTI_LOCAL"
database = "OnlineRetailDB"
user = "venkat"
password = "Azuresql1234@"

# Function to connect to the database
def get_db_connection():
    try:
        conn = pymssql.connect(server, user, password, database)
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Database connection error")

# API endpoint to get all customer summaries or a specific customer by CustomerID
@app.get("/Customer_Summary/")
@app.get("/Customer_Summary/{CustomerID}")
def get_Customer_Summary(CustomerID: int = None):
    logging.info(f"Fetching Customer Summary for CustomerID: {CustomerID if CustomerID else 'ALL'}")

    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)

    try:
        if CustomerID:
            query = "SELECT * FROM Customer_Summary WHERE CustomerID = %s"
            cursor.execute(query, (CustomerID,))
        else:
            query = "SELECT * FROM Customer_Summary"
            cursor.execute(query)

        result = cursor.fetchall()

        if not result:
            logging.warning(f"No customer found for CustomerID: {CustomerID}")
            raise HTTPException(status_code=404, detail="Customer not found")

        return {"Customer_Summary": result}

    finally:
        cursor.close()
        conn.close()

# API endpoint to get all product summaries or a specific product by StockCode
@app.get("/Product_Summary/")
@app.get("/Product_Summary/{StockCode}")
def get_product_summary(StockCode: int = None):
    logging.info(f"Fetching Product Summary for Stock Code: {StockCode if StockCode else 'ALL'}")

    conn = get_db_connection()
    cursor = conn.cursor(as_dict=True)

    try:
        if StockCode:
            query = "SELECT * FROM Product_Summary WHERE StockCode = %s"
            cursor.execute(query, (StockCode,))
        else:
            query = "SELECT * FROM Product_Summary"
            cursor.execute(query)

        result = cursor.fetchall()

        if not result:
            logging.warning(f"No product found for Stock Code: {StockCode}")
            raise HTTPException(status_code=404, detail="Product not found")

        return {"Product_Summary": result}

    finally:
        cursor.close()
        conn.close()

