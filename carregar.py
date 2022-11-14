import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import Row

appName= "hive_pyspark"
master= "local"
spark = SparkSession.builder.master(master).appName(appName).enableHiveSupport().getOrCreate()
spark.sql('use faculdade')

spark.sql("load data local inpath './bolsafamiliateste.csv'\
            overwrite into table bolsafamilia")
#spark.sql("select nome_favorecido, uf, from bolsafamilia WHERE nome_favorecido like 'MARIA%'").show(truncate = False)
#spark.sql("select uf,codigo_municipio,nome_municipio,cpf,nis_favorecido,nome_favorecido,valor_parcela from bolsafamilia WHERE nome_favorecido like 'MARIA%'").show(truncate = False)
#spark.sql("select uf,codigo_municipio,nome_municipio,cpf,nis_favorecido,nome_favorecido,valor_parcela, count(*) from bolsafamilia WHERE nome_favorecido like 'MARIA%' group by nome_favorecido").show(truncate = False)
#spark.sql("select * from bolsafamilia WHERE uf = 'AC' LIMIT 1000").show(truncate = False)
spark.sql("select count(*) from bolsafamilia WHERE nome_favorecido like 'MARIA%' and uf = 'PR'").show(truncate = False)
#spark.sql("select * from bolsafamilia WHERE nome_favorecido like 'MARIA%'").show(truncate = False)
#spark.sql("select nome_favorecido,valor_parcela from bolsafamilia AS decoded").show(truncate = False)
print ("Dados selecionados")
