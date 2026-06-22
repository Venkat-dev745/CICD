# Databricks notebook source
# when()

data = [("James","M",60000),("Michael","M",70000),
        ("Robert",None,400000),("Maria","F",500000),
        ("Jen","",None)
       ]

columns = ["name","gender","salary"]
df1 = spark.createDataFrame(data = data, schema = columns)
df1.show()


# COMMAND ----------

from pyspark.sql.functions import when

newCol = df1.select('name','salary', when(df1.gender == 'M', 'Male')
            .when(df1.gender == 'F', 'Female')
            .when(df1.gender.isNull(), '')
            .otherwise(df1.gender).alias('newGender'))
newCol.show()

# COMMAND ----------

# example

df1.select(*, when(('code' == 'a') | ('code' == 'd'), 'A')
          .when(('code' == 'b') & ('amt' == '4'), 'B')
          .otherwise('A1').alias('newColumn'))

# COMMAND ----------

# expr function :-

#Concatenate columns using || (sql like)
from pyspark.sql.functions import expr
data=[("James","Bond"),("Scott","Varsa")] 
df2=spark.createDataFrame(data=data, schema=["col1","col2"]) 
df2.withColumn("Name",expr(" col1 ||' '|| col2")).show()

# COMMAND ----------


from pyspark.sql.functions import expr, lit

data=[("2019-01-23",1),("2019-06-24",2),("2019-09-20",3)] 
df3=spark.createDataFrame(data).toDF("date","increment") 

#Add Month value from another column
df3.select(df3.date,df3.increment,
     expr("add_months(date,increment)")
  .alias("inc_date")).show()



# COMMAND ----------


# Using Cast() Function

df3.select("increment",expr("cast(increment as string) as str_increment")).printSchema()

# COMMAND ----------

df3.select(df3.date,df3.increment,
     expr('increment + 3 as new_increment')
  ).show()

# COMMAND ----------

# split function :- convert delimiter separated String to an Array (StringType to ArrayType) column on DataFrame
data = [("James, A, Smith","2018","M",3000),
        ("Michael, Rose, Jones","2010","M",4000),
        ("Robert,K,Williams","2010","M",4000),
        ("Maria,Anne,Jones","2005","F",4000),
        ("Jen,Mary,Brown","2010","",-1)
       ]

columns=["name","dob_year","gender","salary"]
df4=spark.createDataFrame(data,columns)
df4.printSchema()
df4.show()

# COMMAND ----------


from pyspark.sql.functions import split, col
df4 = df4.withColumn('name', split(col("name"),",")).show(truncate = False)


# COMMAND ----------

# concat_ws() function  syntax : concat_ws(separator, *cols)

columns = ["name","languagesAtSchool","currentState"]
data = [("James,,Smith",["Java","Scala","C++"],"CA"), \
        ("Michael,Rose,",["Spark","Java","C++"],"NJ"), \
        ("Robert,,Williams",["CSharp","VB"],"NV")
       ]

df5 = spark.createDataFrame(data=data,schema=columns)
df5.printSchema()
df5.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import concat_ws

df5.withColumn('languagesAtSchool', concat_ws(',','languagesAtSchool')).show()

# COMMAND ----------

# substring() function:- we can extract a substring or slice of a string from the DataFrame column by providing the position and length of                            the string you wanted to slice.
# syntax :- substring(str, pos, len)

data = [(1,"20200828"),(2,"20180525")]
columns=["id","date"]
df6=spark.createDataFrame(data,columns)
df6.printSchema()
df6.show(truncate=False)


# COMMAND ----------

from pyspark.sql.functions import substring

df6.withColumn('year', substring('date', 1,4))\
    .withColumn('month', substring('date', 5,2))\
    .withColumn('day', substring('date', 7,2)).show()

# COMMAND ----------

# translate() :- replace character by character of DataFrame column value

address = [ (1,"14851 Jeffrey Rd","DE"),
            (2,"43421 Margarita St","NY"),
            (3,"13111 Siemon Ave","CA")
          ]
df7 =spark.createDataFrame(address,["id","address","state"])
df7.show()


# COMMAND ----------

from pyspark.sql.functions import translate

df7.withColumn('address', translate('address', '123', 'ABC')).show(truncate = False)

# COMMAND ----------

# regexp_replace() :- replace column value with a value from another DataFrame column or character
data = [("ABCDE_XYZ", "XYZ","FGH")]
columns = ["col1", "col2","col3"]
df8 = spark.createDataFrame(data = data, schema = columns)
df8.printSchema()
df8.show()


# COMMAND ----------

from pyspark.sql.functions import expr
df8.withColumn("new_column",regexp_replace('col1', 'col2', 'col3')).show()

# COMMAND ----------

#Replace string column value conditionally

address = [(1,"14851 Jeffrey Rd","DE"),
    (2,"43421 Margarita St","NY"),
    (3,"13111 Siemon Ave","CA")]
df9 =spark.createDataFrame(address,["id","address","state"])
df9.show()


# COMMAND ----------

from pyspark.sql.functions import when, regexp_replace 
df9.withColumn('address', when(df9.address.endswith('Rd'), regexp_replace(df9.address, 'Rd', 'Road'))\
                         .when(df9.address.endswith('St'), regexp_replace(df9.address, 'St', 'Street'))\
                         .when(df9.address.endswith('Ave'), regexp_replace(df9.address, 'Ave', 'Avenue'))\
                         .otherwise(df9.address)).show(truncate = False)

# COMMAND ----------

#Overlay :- Replace column value with a string value from another column.

from pyspark.sql.functions import overlay
df10 = spark.createDataFrame([("ABCDE_XYZ", "FGHI")], ("col1", "col2"))
df10.select(overlay("col1", "col2", 6).alias("overlayed")).show()



# COMMAND ----------


from pyspark.sql.functions import *

df11=spark.createDataFrame(
        data = [ ("1","2019-06-24 12:01:19.000")],
        schema=["id","input_timestamp"])
df11.printSchema()

#Timestamp String to DateType
df11.withColumn("timestamp",to_timestamp("input_timestamp")) \
  .show(truncate=False)


# COMMAND ----------

df11.select(to_timestamp(lit('06-24-2019 12:01:19.000'),'MM-dd-yyyy HH:mm:ss.SSSS')).show()

# COMMAND ----------

# to_date()
from pyspark.sql.functions import *
#Timestamp String to DateType
df11.withColumn('date_type', to_date('input_timestamp')).show(truncate=False)

# COMMAND ----------

#Timestamp Type to DateType

df11.withColumn('date_type', to_date(current_timestamp())).show(truncate=False)

# COMMAND ----------

#Custom Timestamp format to DateType

df11.select(to_date(lit('06-24-2019 12:01:19.000'), 'MM-dd-yyyy HH:mm:ss.SSSS')).show()

# COMMAND ----------

# convert string to timestamp to date type

df11.withColumn('timestamp', to_timestamp(current_timestamp()))\
    .withColumn('date_type', to_date('timestamp'))\
    .drop('input_timestamp').show(truncate = False)

# COMMAND ----------

# date_format() :- 

df11.select(current_date().alias('current_date'), \
            date_format(current_timestamp(), 'yyyy MM dd').alias('yyyy MM dd'), \
            date_format(current_timestamp(), 'MM/dd/yyyy hh:mm').alias('MM/dd/yyyy hh:mm'), \
            date_format(current_timestamp(), 'yyyy MMM dd').alias('yyyy MMM dd'), \
            date_format(current_timestamp(), 'yyyy MMMM dd E').alias('yyyy MMMM dd E') \
           ).show(truncate = False)

# COMMAND ----------

# datediff() Function :- you can calculate the difference between two dates in days, months, and year

data = [("1","2019-07-01"),("2","2019-06-24"),("3","2019-08-24")]
df12=spark.createDataFrame(data=data,schema=["id","date"])
df12.show()


# COMMAND ----------

from pyspark.sql.functions import *
df12.select('date', current_date().alias('current_date'), datediff(current_date(), 'date').alias('date_diff')).show()

# COMMAND ----------

df12.withColumn('date_diff', datediff(current_date(), 'date')) \
    .withColumn('month_diff', months_between(current_date(), 'date')) \
    .withColumn('months_round', round(months_between(current_date(), 'date'), 0)) \
    .withColumn('year_diff', months_between(current_date(), 'date')/ lit(12)) \
    .withColumn('year_round', round(months_between(current_date(), 'date')/ lit(12), 0)).show()

# COMMAND ----------

# explode() :- PySpark explode function can be used to explode an Array of Array (nested Array) ArrayType(ArrayType(StringType)) columns to rows on PySpark DataFrame using python example.

arrayArrayData = [("James",[["Java","Scala","C++"],["Spark","Java"]]),
                  ("Michael",[["Spark","Java","C++"],["Spark","Java"]]),
                  ("Robert",[["CSharp","VB"],["Spark","Python"]])
                 ]

df13 = spark.createDataFrame(data=arrayArrayData, schema = ['name','subjects'])
df13.printSchema()
df13.show(truncate=False)


# COMMAND ----------

from pyspark.sql.functions import explode

df13.select('name', explode('subjects')).show(truncate=False)

# COMMAND ----------

# flatten() :- If you want to flatten the arrays, use flatten function which converts array of array columns to a single array on DataFrame.

from pyspark.sql.functions import flatten
df13.select('name', flatten('subjects')).show(truncate=False)

# COMMAND ----------


data = [
 ("James,,Smith",["Java","Scala","C++"],["Spark","Java"],"OH","CA"),
 ("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"],"NY","NJ"),
 ("Robert,,Williams",["CSharp","VB"],["Spark","Python"],"UT","NV")
]

from pyspark.sql.types import StringType, ArrayType,StructType,StructField
schema = StructType([ 
    StructField("name",StringType(),True), 
    StructField("languagesAtSchool",ArrayType(StringType()),True), 
    StructField("languagesAtWork",ArrayType(StringType()),True), 
    StructField("currentState", StringType(), True), 
    StructField("previousState", StringType(), True)
  ])

df14 = spark.createDataFrame(data=data,schema=schema)
df14.printSchema()
df14.show()

# COMMAND ----------

# array() :- create a new array column by merging the data from multiple columns. All input columns must have the same data type.
# syntax :- array(col1, col2, ....)
from pyspark.sql.functions import array
df14.select('name', array('currentState', 'previousState').alias('states')).show()

# COMMAND ----------

# array_contains() :- check if array column contains a value. Returns null if the array is null
# syntax :- array_contains(column name, value to be check)
from pyspark.sql.functions import array_contains
df14.select('name', 'languagesAtSchool', array_contains('languagesAtSchool', 'Java').alias('array_contains')).show()

# COMMAND ----------

from pyspark.sql.functions import *
simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
schema = ["employee_name", "department", "salary"]
df15 = spark.createDataFrame(data=simpleData, schema = schema)
df15.printSchema()
df15.show(truncate=False)


# COMMAND ----------

# collect() :-

#deptDF.collect() returns Array of Row type.
#deptDF.collect()[0] returns the first element in an array (1st row).
#deptDF.collect[0][0] returns the value of the first row & first column.

# 1) approx_count_distinct Aggregate Function :- 

print('approx_count_distinct is : ' + str(df15.select(approx_count_distinct('salary')).collect()[0][0]))

# COMMAND ----------

print('avg of salary is : ' + str(df15.select(avg('salary')).collect()[0][0]))

# COMMAND ----------

# collect_list Aggregate Function :- returns all values from an input column with duplicates.

df15.select(collect_list('salary')).show(truncate = False)

# COMMAND ----------

# collect_set() :- returns all values from an input column with duplicate values eliminated.

df15.select(collect_set('salary')).show(truncate = False)

# COMMAND ----------

# countDistinct
countDistinct = df15.select(countDistinct("department", "salary"))
countDistinct.show(truncate=False)
print("Distinct Count of Department & Salary : "+str(countDistinct.collect()[0][0]))


# COMMAND ----------

df15.select(first('salary').alias('first_element')).show(truncate=False)

# COMMAND ----------

df15.select(last('salary').alias('last_element')).show(truncate=False)

# COMMAND ----------

df15.select(kurtosis('salary')).show(truncate=False)

# COMMAND ----------

df15.select(max('salary')).show()

# COMMAND ----------

df15.select(min('salary')).show()

# COMMAND ----------

# create_map() :-  How to convert selected or all DataFrame columns to MapType similar to Python Dictionary (Dict) object

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

data = [("36636","Finance",3000,"USA"), 
        ("40288","Finance",5000,"IND"), 
        ("42114","Sales",3900,"USA"), 
        ("39192","Marketing",2500,"CAN"), 
        ("34534","Sales",6500,"USA") ]
schema = StructType([
     StructField('id', StringType(), True),
     StructField('dept', StringType(), True),
     StructField('salary', IntegerType(), True),
     StructField('location', StringType(), True)
     ])

df16 = spark.createDataFrame(data=data,schema=schema)
df16.printSchema()
df16.show(truncate=False)


# COMMAND ----------

from pyspark.sql.functions import *
mapType=df16.withColumn('propertiesMap', create_map(lit('salary'), 'salary', lit('location'), 'location'))
mapType.printSchema()
mapType.show(truncate = False)

# COMMAND ----------

# 1. Create PySpark MapType
# syntax :- MapType(keyType, valueType, valueContainsNull)

from pyspark.sql.types import StructField, StructType, StringType, MapType
schema = StructType([
    StructField('name', StringType(), True),
    StructField('properties', MapType(StringType(),StringType()),True)
])
dataDictionary = [
        ('James',{'hair':'black','eye':'brown'}),
        ('Michael',{'hair':'brown','eye':None}),
        ('Robert',{'hair':'red','eye':'black'}),
        ('Washington',{'hair':'grey','eye':'grey'}),
        ('Jefferson',{'hair':'brown','eye':''})
        ]
df17 = spark.createDataFrame(data=dataDictionary, schema = schema)
df17.printSchema()
df17.show(truncate=False)

# COMMAND ----------

# Access PySpark MapType Elements

accessElements = df17.rdd.map(lambda x : (x.name, x.properties['hair'], x.properties['eye'])).toDF(['name', 'hair', 'eye'])
accessElements.printSchema()
accessElements.show()

# COMMAND ----------

df17.withColumn("hair",df17.properties.getItem("hair")) \
    .withColumn("eye",df17.properties.getItem("eye")) \
    .drop("properties") \
    .show()

# COMMAND ----------

df17.withColumn("hair",df17.properties["hair"]) \
  .withColumn("eye",df17.properties["eye"]) \
  .drop("properties") \
  .show()

# COMMAND ----------

# map_keys() :- Get All Map Keys

from pyspark.sql.functions import map_keys
df17.select('name', map_keys('properties')).show()

# COMMAND ----------

from pyspark.sql.functions import map_values
df17.select('name', map_values('properties')).show()

# COMMAND ----------

# to print how many keys present in properties

from pyspark.sql.functions import explode
keysDF = df17.select(explode(map_keys('properties'))).distinct()
keysList = keysDF.rdd.map(lambda x:x[0]).collect()
print(keysList)


# COMMAND ----------

# check if column or row exist
print(df17.schema.fieldNames.contains("name"))
print(df17.schema.contains(StructField("name",StringType,true)))

# COMMAND ----------

# sum(), avg()

simpleData = [("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NV",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Raman","Finance","DE",99000,40,24000),
    ("Scott","Finance","NY",83000,36,19000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","NV",80000,25,18000),
    ("Kumar","Marketing","NJ",91000,50,21000)
  ]

schema = ["employee_name","department","state","salary","age","bonus"]
df18 = spark.createDataFrame(data=simpleData, schema = schema)
df18.printSchema()
df18.show(truncate=False)


# COMMAND ----------

# arrange the salary in descending order, where sum of salary in each state is greater than 100000

from pyspark.sql.functions import sum, desc, col
df18.groupBy('state') \
    .agg(sum('salary').alias('sum_salary')) \
    .filter(col('sum_salary') > 100000) \
    .sort(desc('sum_salary')) \
    .show()

# COMMAND ----------

# Window ranking functions :- 

simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3100),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4200), \
    ("Robu", "Finance", 3300)
  )
 
columns= ["employee_name", "department", "salary"]
df19 = spark.createDataFrame(data = simpleData, schema = columns)
df19.printSchema()
df19.show(truncate=False)


# COMMAND ----------

# row_number() :- used to give the sequential row number starting from 1 to the result of each window partition.

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

df19.withColumn('row_number', row_number().over(Window.partitionBy('department').orderBy('salary'))).show()

# COMMAND ----------

# rank() :- to provide a rank to the result within a window partition. This function leaves gaps in rank when there are ties.

from pyspark.sql.functions import rank
df19.withColumn('rank_number', rank().over(Window.partitionBy('department').orderBy('salary'))).show()

# COMMAND ----------

# dense_rank() :- used to get the result with rank of rows within a window partition without any gaps.
from pyspark.sql.functions import dense_rank
df19.withColumn('dense_rank', dense_rank().over(Window.partitionBy('department').orderBy('salary'))).show()

# COMMAND ----------

# percent_rank():-  same as rank function
    from pyspark.sql.functions import percent_rank
df19.withColumn('percent_rank', percent_rank().over(Window.partitionBy('department').orderBy('salary'))).show()

# COMMAND ----------

# window analytic function :- 
# cumu_dist() :- The CUME_DIST() function returns a value that represents the number of rows with values less than or equal to (<= ) the current row’s value divided by the total number of rows:
# formula :-
#N / total_rows
from pyspark.sql.functions import cume_dist
df19.withColumn('cume_dist', cume_dist().over(Window.partitionBy('department').orderBy('salary'))).show()

# COMMAND ----------

# lag():- suppose offeset 1 then first value is null to the department and lag one element
# syntax : lag(column, offset value)
from pyspark.sql.functions import lag
df19.withColumn('lag', lag('salary', 1).over(Window.partitionBy('department').orderBy('salary'))).show()

# COMMAND ----------

# lead():- opposite to lag

from pyspark.sql.functions import lead
df19.withColumn('lead', lead('salary', 1).over(Window.partitionBy('department').orderBy('salary'))).show()

# COMMAND ----------

# PySpark Window Aggregate Functions :- When working with Aggregate functions, we don’t need to use order by clause.

windowSpecAgg  = Window.partitionBy("department")
from pyspark.sql.functions import col,avg,sum,min,max,row_number 
df19.withColumn("row",row_number().over(Window.partitionBy('department').orderBy('salary'))) \
  .withColumn("avg", avg(col("salary")).over(windowSpecAgg)) \
  .withColumn("sum", sum(col("salary")).over(windowSpecAgg)) \
  .withColumn("min", min(col("salary")).over(windowSpecAgg)) \
  .withColumn("max", max(col("salary")).over(windowSpecAgg)) \
  .where(col("row")==1).select("department","avg","sum","min","max") \
  .show()


# COMMAND ----------

# PySpark JSON Functions :-

jsonString = """{'Zipcode' : 704, 'ZipcodeType' : 'STANDARD', 'City' : 'Parc Parque', 'state' : 'PR'}"""
df20 = spark.createDataFrame([(1,jsonString)], ['id', 'value'])
df20.show(truncate = False)

# COMMAND ----------

# 1) from_json() :- 
# 1.1) Spark Convert JSON Column to struct Column
from pyspark.sql.functions import from_json
from pyspark.sql.types import StringType, StructType
val schema = new StructType()
            .add('Zipcode', StringType, true)
            .add('ZipcodeType', StringType, true)
            .add('City', StringType, true)
            .add('state', StringType, true)
df21 = df20.withColumn('value', from_json('value', schema))
df21.show()

# COMMAND ----------

import org.apache.spark.sql.functions.{from_json,col}
import org.apache.spark.sql.types.{MapType, StringType}
val schema = new StructType()
    .add("Zipcode", StringType, true)
    .add("ZipCodeType", StringType, true)
    .add("City", StringType, true)
    .add("State", StringType, true)
val df21=df20.withColumn("value",from_json(col("value"),schema))
df21.printSchema()
df21.show(false)

# COMMAND ----------

# 2 Spark Convert JSON Column to Multiple Columns

val df22=df21.select(col("id"),col("value.*"))
df5.printSchema()
df5.show()

# COMMAND ----------

#Convert JSON string column to Map type

from pyspark.sql.types import MapType,StringType
from pyspark.sql.functions import from_json
Map_type=df20.withColumn("value",from_json(df20.value,MapType(StringType(),StringType())))
Map_type.printSchema()
Map_type.show(truncate=False)

# COMMAND ----------

# 2. to_json() :- convert DataFrame columns MapType or Struct type to JSON string

from pyspark.sql.functions import to_json, col
df20.withColumn("value",to_json("value")).show(truncate=False)

# COMMAND ----------


