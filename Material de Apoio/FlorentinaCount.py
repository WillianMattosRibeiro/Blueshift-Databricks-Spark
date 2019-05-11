from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

florentinaRDD = spark.sparkContext.textFile("D:\\PyCharm\\Blueshift-DatabricksSpark\\Material de Apoio\\florentina.txt")

wordFlorentinaRDD = florentinaRDD.flatMap(lambda x: x.split(" "))

filterFlorentinaRDD = wordFlorentinaRDD.filter(lambda x: 'Florentina' in x)

filterFlorentinaRDD.count()