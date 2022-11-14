import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row

appName= "hive_pyspark"
master= "local"
spark = SparkSession.builder.master(master).appName(appName).enableHiveSupport().getOrCreate()
spark.sql('use faculdade')
df=spark.sql("describe bolsafamilia").show()