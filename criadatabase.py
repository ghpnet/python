import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row
appName= "hive_pyspark"
master= "local"
spark = SparkSession.builder.master(master).appName(appName).enableHiveSupport().getOrCreate()
df=spark.sql("show databases")
df.show()
tables = spark.sql("show tables").show()
#criar o database faculdade
spark.sql("create database faculdade").show()
df=spark.sql("show databases")
print ("oi")
