from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

path_florentina = "D:\\PyCharm\\Blueshift-DatabricksSpark\\Material de Apoio\\florentina.txt"

florentinaRDD = spark.sparkContext.textFile(path_florentina)

wordFlorentinaRDD = florentinaRDD.flatMap(lambda x: x.split(" "))

filterFlorentinaRDD = wordFlorentinaRDD.filter(lambda x: 'Florentina' in x)

filterFlorentinaRDD.count()