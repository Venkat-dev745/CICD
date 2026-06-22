# Databricks notebook source
emp =[(1,"Smith",-1,"2018","10","M",3000), \
      (2,"Rose",1,"2010","20","M",4000), \
      (3,"Williams",1,"2010","10","M",1000), \
      (4,"Jones",2,"2005","10","F",2000), \
      (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
     ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show(truncate=False)

dept = [("Finance",10),("Marketing",20),("Sales",30),("IT",40)]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

# COMMAND ----------

# PySpark Inner Join DataFrame

empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "inner").orderBy('emp_id').show(truncate=False)

# COMMAND ----------

# PySpark outer/full/fullouter Join DataFrame

empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "outer").orderBy('emp_id').show(truncate=False)
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "fullouter").orderBy('emp_id').show(truncate=False)

# COMMAND ----------

# left/leftouter join

empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, 'left').orderBy('emp_id').show(truncate=False)

# COMMAND ----------

#right/rightouter join

empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, 'right').orderBy('emp_id').show(truncate=False)

# COMMAND ----------

# leftsemi join :- returns columns from the only left dataset for the records match in the right dataset on join expression, records not matched on join expression are ignored from both left and right datasets.

empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, 'leftsemi').orderBy('emp_id').show(truncate=False)

# COMMAND ----------

# leftanti :- returns only columns from the left dataset for non-matched records.

empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, 'leftanti').orderBy('emp_id').show(truncate=False)

# COMMAND ----------

# PySpark SQL Join on multiple DataFrames

df1.join(df2,df1.id1 == df2.id2,"inner") \
   .join(df3,df1.id1 == df3.id3,"inner").show()

# COMMAND ----------

#union/union all :- both prints all records with duplicates

simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Michael","Sales","NY",86000,56,20000), \
    ("Robert","Sales","CA",81000,30,23000), \
    ("Maria","Finance","CA",90000,24,23000) \
  ]

columns= ["employee_name","department","state","salary","age","bonus"]
df1 = spark.createDataFrame(data = simpleData, schema = columns)
df1.printSchema()
df1.show(truncate=False)


# COMMAND ----------


simpleData = [("James","Sales","NY",90000,34,10000), \
    ("Maria","Finance","CA",90000,24,23000), \
    ("Jen","Finance","NY",79000,53,15000), \
    ("Jeff","Marketing","CA",80000,25,18000), \
    ("Kumar","Marketing","NY",91000,50,21000) \
  ]
columns= ["employee_name","department","state","salary","age","bonus"]

df2 = spark.createDataFrame(data = simpleData, schema = columns)

df2.printSchema()
df2.show(truncate=False)


# COMMAND ----------

# merging two dataframes

df1.union(df2).show(truncate=False)

# COMMAND ----------

# to remove duplicates using union(merge without duplicates)

df1.union(df2).distinct().show()

# COMMAND ----------

# pyspark UDF(user defined functions)

columns = ["Seqno","Name"]
data = [("1", "john jones"),
        ("2", "tracey smith"),
        ("3", "amy sanders")
       ]
df3= spark.createDataFrame(data=data,schema=columns)
df3.show(truncate=False)


# COMMAND ----------

def convertCase(str):
    newStr = ""
    list_1 = str.split(" ")
    for i in list_1:
        newStr = newStr + i[0:1].upper()+i[1:len(i)]+" "
    return newStr

# Converting function to UDF
convertUDF = udf(lambda z: convertCase(z))

df3.select(("Seqno"), \
    convertUDF("Name")) \
   .show(truncate=False)


# COMMAND ----------

def upperCase(z):
    return z.upper()
upperCaseUDF = udf(lambda z : upperCase(z))
df3.select(('Seqno'),upperCaseUDF('Name')).show()

# COMMAND ----------

# Creating UDF using annotation

from pyspark.sql.types import *
@udf(returnType = StringType())

def upperCase(z):
    return z.upper()
df3.select(('Seqno'),upperCaseUDF('Name')).show()

# COMMAND ----------

# map function

help(spark)

# COMMAND ----------


