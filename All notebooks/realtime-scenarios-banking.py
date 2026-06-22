# Databricks notebook source
# Simulating the customers data with 20 rows
data_customers = [
    (1, "John Doe", 28, "1234 Elm St, Springfield"),
    (2, "Jane Smith", 35, "5678 Oak St, Springfield"),
    (3, "Alice Brown", 22, "1357 Pine St, Rivertown"),
    (4, "Bob Johnson", 50, "2468 Maple St, Hilltop"),
    (5, "Charlie Davis", 40, "7890 Birch St, Lakeside"),
    (6, "David Clark", 33, "1357 Cedar St, Rivertown"),
    (7, "Eva Adams", 45, "2468 Birch St, Lakeside"),
    (8, "Frank Miller", 60, "5678 Pine St, Hilltop"),
    (9, "Grace Wilson", 27, "7890 Oak St, Springfield"),
    (10, "Hannah Moore", 38, "1234 Pine St, Rivertown"),
    (11, "Ian Lewis", 50, "1357 Oak St, Lakeside"),
    (12, "Jack White", 31, "2468 Elm St, Springfield"),
    (13, "Karen Hall", 41, "5678 Birch St, Hilltop"),
    (14, "Leo Harris", 55, "7890 Maple St, Rivertown"),
    (15, "Megan Walker", 29, "1234 Birch St, Springfield"),
    (16, "Nathan King", 48, "5678 Cedar St, Lakeside"),
    (17, "Olivia Scott", 36, "1357 Maple St, Hilltop"),
    (18, "Paul Young", 44, "2468 Cedar St, Rivertown"),
    (19, "Quinn Baker", 32, "7890 Cedar St, Springfield"),
    (20, "Rachel Gonzalez", 58, None),
    (20, "Rachelisa", 52, "345 vhd j, riverton")
]

columns_customers = ["customer_id", "name", "age", "address"]
df_customers = spark.createDataFrame(data_customers, columns_customers)

df_customers.show()


# COMMAND ----------

# Simulating accounts data with 20 rows
data_accounts = [
    (101, 1, 10000.0, "Savings", 1),
    (102, 2, 5000.0, "Current", 2),
    (103, 3, 15000.0, "Salary", 3),
    (104, 4, 25000.0, "Salary", 4),
    (105, 5, 0.0, "Savings", 1),
    (106, 6, -7000.0, "Salary", 2),
    (107, 7, 22000.0, "Current", 3),
    (108, 8, 30000.0, "Salary", 4),
    (109, 9, 8000.0, "Current", 1),
    (110, 10, 0.0, "Salary", 2),
    (111, 11, 50000.0, "Salary", 3),
    (112, 12, 4500.0, "Current", 4),
    (113, 13, 15000.0, "Savings", 1),
    (114, 14, 0.0, "Savingg", 2),
    (115, 15, 17000.0, "Savings", 3),
    (116, 16, 19000.0, "Current", 4),
    (117, 17, 23000.0, "Savings", 1),
    (118, 18, 13000.0, "Savings", 2),
    (119, 19, 16000.0, "Salary", 3),
    (120, 20, 14000.0, "Salary", 4)
]

columns_accounts = ["account_id", "customer_id", "balance", "account_type", "branch_id"]
df_accounts = spark.createDataFrame(data_accounts, columns_accounts)

df_accounts.show()


# COMMAND ----------

# Simulating transactions data with 20 rows
data_transactions = [
    (1, 101, "2024-12-01", 500.0, "Deposit"),
    (2, 102, "2024-12-02", 1000.0, "Withdrawal"),
    (3, 103, "2024-12-03", 250.0, "Transfer"),
    (4, 104, "2024-12-04", 700.0, "Deposit"),
    (5, 105, "2024-12-05", None, "Deposit"),
    (6, 106, "2024-12-06", 600.0, "Deposit"),
    (7, 107, "2024-12-07", 400.0, "Transfer"),
    (8, 108, None, 800.0, None),
    (9, 109, "2024-12-09", 200.0, "Withdrawal"),
    (10, 110, "2024-12-10", 150.0, "Deposit"),
    (11, 111, "2024-12-11", 1200.0, "Deposit"),
    (12, 112, "2024-12-12", 350.0, "Withdrawal"),
    (13, 113, "2024-12-13", 600.0, "Transfer"),
    (14, 114, "2024-12-14", 500.0, "Deposit"),
    (15, 115, "2024-12-15", 700.0, "Withdrawal"),
    (16, 116, "2024-12-16", 800.0, "Transfer"),
    (17, 117, "2024-12-17", 1000.0, "Deposit"),
    (18, 118, "2024-12-18", 150.0, "Withdrawal"),
    (19, 119, "2024-12-19", 450.0, "Transfer"),
    (20, 120, "2024-12-20", 500.0, "Deposit")
]

columns_transactions = ["transaction_id", "account_id", "transaction_date", "amount", "transaction_type"]
df_transactions = spark.createDataFrame(data_transactions, columns_transactions)

df_transactions.show(20)


# COMMAND ----------

# Simulating loans data with 20 rows
data_loans = [
    (1, 1, 20000.0, 5.0, "2020-01-01", "2025-01-01"),
    (2, 2, 15000.0, 4.5, "2021-01-01", "2026-01-01"),
    (3, 3, 30000.0, 6.0, "2019-01-01", "2024-01-01"),
    (4, 4, 40000.0, 5.5, "2022-01-01", "2027-01-01"),
    (5, 5, 10000.0, 4.0, "2023-01-01", "2028-01-01"),
    (6, 6, 15000.0, 5.5, "2020-05-01", "2025-05-01"),
    (7, 7, 25000.0, 6.0, "2021-06-01", "2026-06-01"),
    (8, 8, 35000.0, 4.7, "2022-07-01", "2027-07-01"),
    (9, 9, 12000.0, 5.0, "2023-08-01", "2028-08-01"),
    (10, 10, 18000.0, 4.8, "2020-09-01", "2025-09-01"),
    (11, 11, 22000.0, 5.2, "2021-10-01", "2026-10-01"),
    (12, 12, 16000.0, 4.9, "2022-11-01", "2027-11-01"),
    (13, 13, 27000.0, 6.1, "2023-12-01", "2028-12-01"),
    (14, 14, 29000.0, 5.4, "2020-02-01", "2025-02-01"),
    (15, 15, 21000.0, 5.6, "2021-03-01", "2026-03-01"),
    (16, 16, 26000.0, 5.3, "2022-04-01", "2027-04-01"),
    (17, 17, 30000.0, 6.2, "2023-05-01", "2028-05-01"),
    (18, 18, 23000.0, 5.1, "2020-06-01", "2025-06-01"),
    (19, 19, 28000.0, 5.7, "2021-07-01", "2026-07-01"),
    (20, 20, 24000.0, 4.8, "2022-08-01", "2027-08-01")
]

columns_loans = ["loan_id", "customer_id", "loan_amount", "interest_rate", "loan_start_date", "loan_end_date"]
df_loans = spark.createDataFrame(data_loans, columns_loans)

df_loans.show(20)


# COMMAND ----------

# Simulating branches data with 20 rows
data_branches = [
    (1, "Springfield Branch", "Springfield"),
    (2, "Rivertown Branch", "Rivertown"),
    (3, "Hilltop Branch", "Hilltop"),
    (4, "Lakeside Branch", "Lakeside"),
    (5, "Oakwood Branch", "Oakwood"),
    (6, "Maplewood Branch", "Maplewood"),
    (7, "Cedar Grove Branch", "Cedar Grove"),
    (8, "Pinehill Branch", "Pinehill"),
    (9, "Birchwood Branch", "Birchwood"),
    (10, "Riverstone Branch", "Riverstone"),
    (11, "Westfield Branch", "Westfield"),
    (12, "Eastside Branch", "Eastside"),
    (13, "Northgate Branch", "Northgate"),
    (14, "Southpark Branch", "Southpark"),
    (15, "Central Park Branch", "Central Park"),
    (16, "Greenfield Branch", "Greenfield"),
    (17, "Broadway Branch", "Broadway"),
    (18, "Sunset Branch", "Sunset"),
    (19, "Valley Branch", "Valley"),
    (20, "Hillcrest Branch", "Hillcrest")
]

columns_branches = ["branch_id", "branch_name", "branch_location"]
df_branches = spark.createDataFrame(data_branches, columns_branches)

df_branches.show(20)


# COMMAND ----------

df_customers.printSchema()
df_accounts.printSchema()
df_transactions.printSchema()
df_loans.printSchema()
df_branches.printSchema()

# COMMAND ----------

# Check for null values in each DataFrame
from pyspark.sql.functions import *
df_customers.select([count(when(col(c).isNull(), c)).alias(c) for c in df_customers.columns]).show()
df_accounts.select([count(when(col(c).isNull(), c)).alias(c) for c in df_accounts.columns]).show()
df_transactions.select([count(when(col(c).isNull(), c)).alias(c) for c in df_transactions.columns]).show()
df_loans.select([count(when(col(c).isNull(), c)).alias(c) for c in df_loans.columns]).show()
df_branches.select([count(when(col(c).isNull(), c)).alias(c) for c in df_branches.columns]).show()


# COMMAND ----------

# drop duplicates rows
df_customers = df_customers.dropDuplicates()
df_customers.show(truncate = False)

# COMMAND ----------

# validate banking rules, those who account has negetive balence
df_accounts.filter(col("balance") < 0).show()

# COMMAND ----------

# Validate customer age (must be 18 or older) if less than just filter and store in another dataframe

df_customers_greaterthan18 = df_customers.filter(col("age") >= 18)
df_customers_greaterthan18.show()

# COMMAND ----------

# delete rows where any column has null values
df_customers_cleaned = df_customers.dropna()
df_customers_cleaned.show()

# COMMAND ----------

# Fill missing values with default values
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType

def replace_nulls(df_transactions):
    for column in df_transactions.columns:
        column_type = dict(df_transactions.dtypes)[column]
        if column_type == "string":
            df_transactions = df_transactions.withColumn(column, coalesce(col(column), lit("N/A")))
        elif column_type == "int":
            df_transactions = df_transactions.withColumn(column, coalesce(col(column), lit(0)))
        elif column_type == "double":
            df_transactions = df_transactions.withColumn(column, coalesce(col(column), lit(0.0)))
        elif column_type == "date":
            df_transactions = df_transactions.withColumn(column, coalesce(col(column), lit(None).cast(DateType())))
    return df_transactions


df_transactions_cleaned = replace_nulls(df_transactions)
df_transactions_cleaned.show()


# COMMAND ----------

# remoe the duplicates based on column, suppose id is duplicate then delete entire row having that duplicate id

df_customers = df_customers.dropDuplicates(["customer_id"])
df_customers.show()

# COMMAND ----------

# Calculate the age of loan from loan starting date
from pyspark.sql.functions import *
df_loans = df_loans.withColumn("loan_age_tillnow", (datediff(current_date(), col("loan_start_date")) / 365).cast("int"))
df_loans.show()

# COMMAND ----------

df_customers = df_customers.withColumn("name", initcap(trim(col("name"))))
df_customers.show()

# COMMAND ----------

df_customers = df_customers.withColumn("age_group", when(col("age") < 18, "Teenagers").when((col("age") > 18) & (col("age") < 50), "Adults").otherwise("Olders"))
df_customers.show()

# COMMAND ----------


