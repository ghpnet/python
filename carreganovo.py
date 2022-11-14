import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row

appName= "hive_pyspark"
master= "local"
spark = SparkSession.builder.master(master).appName(appName).enableHiveSupport().getOrCreate()
spark.sql('use faculdade')

spark.sql("load data local inpath './bolsafamiliateste.csv'\
            overwrite into table bolsafamilia")
spark.sql("select max(valor_parcela),min(valor_parcela) from bolsafamilia where uf IN ('BA','CE','PI','PB','SE','RN','MA','AL')").show(truncate = False)

# spark.sql("select * from bolsafamilia").show(truncate = False)
#spark.sql("select uf,count(*) from bolsafamilia WHERE nome_favorecido like 'MARIA%' group by uf").show(truncate = False)
#spark.sql("select count(*) from bolsafamilia WHERE nome_favorecido like 'MARIA%' and uf IN ('PR','SC','RS')").show(truncate = False)
#print ("Dados selecionados")