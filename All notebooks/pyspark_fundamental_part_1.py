# Databricks notebook source
df = spark.read.json(path='dbfs:/FileStore/single_sample_json_file.json',multiLine=True)
df.printSchema()
df.show()

# COMMAND ----------

# if read all json files then syntax is
df = spark.read.json(path='dbfs:/FileStore/*.json',)
df.printSchema()
df.show()

# COMMAND ----------

columns = ["Seqno","Quote"]
data = [("1", "Be the change that you wish to see in the world"),
        ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
        ("3", "The purpose of our lives is to be happy."),
        ("4", "Be cool.")
       ]
df1 = spark.createDataFrame(data = data, schema = columns)
df1.show()

# COMMAND ----------

#Display full column contents

df1.show(truncate=False)

# COMMAND ----------


# Display 2 rows and full column contents

df1.show(2, truncate=False)


# COMMAND ----------


# Display 2 rows & column values 25 characters including spaces

df1.show(2, truncate=25)

# COMMAND ----------

# Display DataFrame rows & columns vertically

df1.show(n=3,truncate=25,vertical=True) 


# COMMAND ----------


# with columns 
data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]
df2 = spark.createDataFrame(data=data, schema = columns)
df2.show()
df2.printSchema()

# COMMAND ----------

# 1. Change DataType using PySpark withColumn()
from pyspark.sql.functions import when, struct, col
df2.withColumn('salary',col('salary').cast('Integer'))
df2.show()
df2.printSchema()

# COMMAND ----------

# 2. Update The Value of an Existing Column

df2.withColumn('salary',col('salary')*10).show()

# COMMAND ----------

# 3. Create a Column from an Existing

df2.withColumn('copied_salary', col('salary')/10).show()

# COMMAND ----------

# 4. Add a New Column using withColumn()
from pyspark.sql.functions import lit
df2.withColumn('country',lit('USA')).show()

# COMMAND ----------

# 5. Rename Column Name

df2.withColumnRenamed('gender','sex').show()

# COMMAND ----------

# 6. Drop Column From PySpark DataFrame

df2.drop('gender').show()

# COMMAND ----------

# select() function:

data = [("James","Smith","INDIA","ANDHRA"),
        ("Michael","Rose","INDIA","KARNATAKA"),
        ("Robert","Williams","INDIA","TAMILNADU"),
        ("Maria","Jones","INDIA","PUNE")
       ]
columns = ["firstname","lastname","country","state"]
df3 = spark.createDataFrame(data = data, schema = columns)
df3.show()

# COMMAND ----------

# Below are ways to select single, multiple or all columns.

df3.select("firstname","lastname").show()

# COMMAND ----------

df3.select(df3.firstname,df3.lastname).show()

# COMMAND ----------

df3.select(df3['firstname'],df3['lastname']).show()

# COMMAND ----------

 # Select All Columns From List
    
df3.select(*columns).show()

# COMMAND ----------

df3.select([column for column in df3.columns]).show()

# COMMAND ----------

df3.select('*').show()

# COMMAND ----------

#Selects first 2 columns and top 2 rows

df3.select(df3.columns[1:3]).show(2)

# COMMAND ----------

# filter() or where() function


from pyspark.sql.types import *
data = [
    (("James","","Smith"),["Java","Scala","C++"],"Odissa","M"),
    (("Anna","Rose",""),["Spark","Java","C++"],"Nepal","F"),
    (("Julia","","Williams"),["CSharp","VB"],"Odissa","F"),
    (("Maria","Anne","Jones"),["CSharp","VB"],"Nepal","M"),
    (("Jen","Mary","Brown"),["CSharp","VB"],"Nepal","M"),
    (("Mike","Mary","Williams"),["Python","VB"],"Odissa","M")
 ]
        
schema = StructType([
     StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
     ])),
     StructField('languages', ArrayType(StringType()), True),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
 ])

df4 = spark.createDataFrame(data = data, schema = schema)
df4.printSchema()
df4.show(truncate=False)


# COMMAND ----------

df4.filter(df4.state == 'Odissa').show(truncate=False)

# COMMAND ----------

df4.filter(df4.state != 'Odissa').show(truncate=False)

# COMMAND ----------

# using sql expression

df4.filter("state == 'Odissa'").show(truncate=False)

# COMMAND ----------

df4.filter((df4.state == 'Odissa') & (df4.gender == 'M')).show(truncate=False)

# COMMAND ----------

# Filter Based on List Values

list_1 = ['Karnataka', 'Nepal', 'Mumbai','Andhra']
df4.filter(df4.state.isin(list_1)).show(truncate=False)

# COMMAND ----------

df4.filter(~df4.state.isin(list_1)).show(truncate=False)

# COMMAND ----------

df4.filter(df4.state.startswith('O')).show(truncate=False)

# COMMAND ----------

df4.filter(df4.state.endswith('l')).show(truncate=False)

# COMMAND ----------

data = [(2,"Michael Rose"),(3,"Robert Williams"),
         (4,"Rames Rose"),(5,"Rames rose")
        ]
df5 = spark.createDataFrame(data = data, schema = ["id","name"])
df5.filter(df5.name.like('%rose%')).show()

# COMMAND ----------

df5.filter(df5.name.rlike('(?i)^*rose$')).show()

# COMMAND ----------

# Filter on an Array column

from pyspark.sql.functions import array_contains

df4.filter(array_contains(df4.languages,'Java')).show(truncate=False)

# COMMAND ----------

# Filtering on Nested Struct columns

df4.filter(df4.name.lastname == "Williams").show(truncate=False)

# COMMAND ----------

# drop() & dropduplicts()

from pyspark.sql.functions import expr
data = [("James", "Sales", 3000),
        ("Michael", "Sales", 4600), \
        ("Robert", "Sales", 4100), \
        ("Maria", "Finance", 3000), \
        ("James", "Sales", 3000), \
        ("Scott", "Finance", 3300), \
        ("Jen", "Finance", 3900), \
        ("Jeff", "Marketing", 3000), \
        ("Kumar", "Marketing", 2000), \
        ("Saif", "Sales", 4100) \
       ]
columns= ["employee_name", "department", "salary"]
df6 = spark.createDataFrame(data = data, schema = columns)
df6.printSchema()
df6.show(truncate=False)


# COMMAND ----------

# to remove duplicated rows

distinctDF = df6.distinct()
print("Distinct Count : "+str(distinctDF.count()))
distinctDF.show(truncate=False)

# COMMAND ----------

dropDuplicates = df6.dropDuplicates()
print("Distinct Count : "+str(dropDuplicates.count()))
dropDuplicates.show(truncate=False)

# COMMAND ----------

# remove duplicates rows from selected columns

dropDuplicates = df6.dropDuplicates(['department','salary'])
print("count of rows after remove duplicates from the both columns is : "+str(dropDuplicates.count()))
dropDuplicates.show(truncate=False)

# COMMAND ----------

# orderBy() and sort() functions


data = [  ("James","Sales","NY",90000,34,10000), \
                ("Michael","Sales","NY",86000,56,20000), \
                ("Robert","Sales","CA",81000,30,23000), \
                ("Maria","Finance","CA",90000,24,23000), \
                ("Raman","Finance","CA",99000,40,24000), \
                ("Scott","Finance","NY",83000,36,19000), \
                ("Jen","Finance","NY",79000,53,15000), \
                ("Jeff","Marketing","CA",80000,25,18000), \
                ("Kumar","Marketing","NY",91000,50,21000) \
             ]
columns= ["employee_name","department","state","salary","age","bonus"]
df7 = spark.createDataFrame(data = data, schema = columns)
df7.printSchema()
df7.show(truncate=False)


# COMMAND ----------

df7.sort('department','state').show(truncate=False)

# COMMAND ----------

df7.orderBy('department','state').show(truncate=False)

# COMMAND ----------

df7.orderBy(df7.department.asc(),df7.state.desc()).show(truncate=False)

# COMMAND ----------

# groupBy() function

df7.groupBy(df7.department).sum('salary').show(truncate=False)

# COMMAND ----------

df7.groupBy(df7.department).count().show()

# COMMAND ----------

df7.groupBy("department").min('salary').show()

# COMMAND ----------

df7.groupBy('department').mean('salary').show()

# COMMAND ----------

# groupBy on multiple columns

df7.groupBy('department','state').sum("salary").show()

# COMMAND ----------

# running more aggregate functions at a time
from pyspark.sql.functions import sum, min
df7.groupBy("department")\
    .agg(sum("salary").alias("sum_salary"), \
         min("salary").alias("min_salary"))\
    .filter(sum("salary") >= 250000) \
.show(truncate=False)

# COMMAND ----------

df.write.format("delta").mode("overwrite").save(path)
df.write.partitionBy("region").parquet(path)
df.write.format("delta").mode("overwrite").save(path)
df.write.format("parquet").bucketBy(3, "region").saveAsTable("bucketed_region")
# read version2
df_version2 = spark.read.format("delta").option("versionAsOf", 2).load(path)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE CityInfo( MixedValue STRING);
# MAGIC
# MAGIC INSERT INTO CityInfo VALUES
# MAGIC ('chennai600001'),
# MAGIC ('bengaluru560001'),
# MAGIC ('kadapa');
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select MixedValue,
# MAGIC case when len(MixedValue) > 6 then left(MixedValue, len(MixedValue) - 6) else MixedValue end as cityname,
# MAGIC case when len(MixedValue) > 6 then right(MixedValue, 6) else MixedValue end as pincode
# MAGIC from CityInfo
