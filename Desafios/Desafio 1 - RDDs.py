# Databricks notebook source
# MAGIC %md
# MAGIC ### Desafio 1: Manipulando RDDs (utilizar somente RDDs!) ###
# MAGIC 
# MAGIC Nesse desafio, iremos realizar uma série de transformações em RDDs, com o intuito de contar quantas vezes cada filme foi avaliado.
# MAGIC 
# MAGIC Os dois datasets utilizados foram retirados do site __[Movies Len](https://grouplens.org/datasets/movielens/)__
# MAGIC 
# MAGIC __Passo 1:__
# MAGIC Carregar o conteúdo dos dois arquivos como RDDs, onde cada elemento do RDD deve consistir em uma linha do arquivo de origem.
# MAGIC 
# MAGIC __Passo 2:__
# MAGIC Aplique no RDD a transformação necessária para que cada elemento passe de uma simples string para uma lista, delimitando os termos pela vírgula. 
# MAGIC 
# MAGIC Filtre (o primeiro elemento é o header) somente as colunas :
# MAGIC   * __"movieId" e "title"__ para o dataset movies.csv 
# MAGIC   * __"userId" e "movieId"__ para o dataset ratings.csv 
# MAGIC   
# MAGIC __Passo 3:__
# MAGIC Retire o cabeçalho do arquivo.
# MAGIC 
# MAGIC __Passo 4:__
# MAGIC Transforme os RDDs em Pair RDDs, onde a chave dos mesmos devem ser o campo movieId.
# MAGIC 
# MAGIC __Passo 5:__
# MAGIC Realize um Join entre os dois RDDs, gerando um terceiro RDD.
# MAGIC 
# MAGIC __Passo 6:__
# MAGIC Conte quantas vezes cada filme foi avaliado e ordene de forma decrescente
# MAGIC 
# MAGIC __Passo 7:__
# MAGIC Grave o conteúdo do RDD em um arquivo texto no diretório FileStore/count_movies/, onde as colunas devem estar separadas por "|"
# MAGIC 
# MAGIC __Obs: Os métodos .take() presentes final de cada célula serve apenas para vocês analisarem os RDDs em cada passo!__
# MAGIC 
# MAGIC __Obs 2: No lugar de "ESCREVA_SEU_CODIGO_AQUI", escreva o código necessário para alcançar o objetivo da célula.__
# MAGIC 
# MAGIC ### Exemplo de linhas do arquivo final: ###
# MAGIC 
# MAGIC Shrek (2001)|46826
# MAGIC 
# MAGIC Speed (1994)|46475
# MAGIC 
# MAGIC Ace Ventura: Pet Detective (1994)|45608
# MAGIC 
# MAGIC Mission: Impossible (1996)|45064
# MAGIC 
# MAGIC Titanic (1997)|44787
# MAGIC 
# MAGIC "Dark Knight|44741
# MAGIC 
# MAGIC Men in Black (a.k.a. MIB) (1997)|44287
# MAGIC 
# MAGIC Memento (2000)|43739

# COMMAND ----------

# DBTITLE 1,Baixando os Datasets
# MAGIC %sh wget http://files.grouplens.org/datasets/movielens/ml-latest.zip
# MAGIC 
# MAGIC unzip ml-latest.zip
# MAGIC 
# MAGIC mv ml-latest/ratings.csv /dbfs/FileStore/
# MAGIC 
# MAGIC mv ml-latest/movies.csv /dbfs/FileStore/
# MAGIC 
# MAGIC rm -r ml-latest*

# COMMAND ----------

# DBTITLE 1,Analisando os arquivos
# MAGIC %sh 
# MAGIC echo "DATASET MOVIES:"
# MAGIC echo ""
# MAGIC head /dbfs/FileStore/movies.csv
# MAGIC echo ""
# MAGIC echo "----"
# MAGIC 
# MAGIC echo ""
# MAGIC echo "DATASET RATINGS:"
# MAGIC echo ""
# MAGIC head /dbfs/FileStore/ratings.csv

# COMMAND ----------

# DBTITLE 1,Passo 1: Carregando os dois arquivos como RDDs
path_movies = "FileStore/movies.csv"
path_ratings = "FileStore/ratings.csv"

moviesRDD = <ESCREVA_SEU_CODIGO_AQUI>
ratingsRDD = <ESCREVA_SEU_CODIGO_AQUI>


# Visualizando os 10 primeiros elementos do RDD
(moviesRDD.take(5), ratingsRDD.take(5))

# COMMAND ----------

# DBTITLE 1,Passo 2: Aplicando o split pelo delimitador e filtrando somente as colunas necessárias
moviesRDD_2 =  moviesRDD.<ESCREVA_SEU_CODIGO_AQUI>
ratingsRDD_2 = ratingsRDD.<ESCREVA_SEU_CODIGO_AQUI>

(moviesRDD_2.take(5), ratingsRDD_2.take(5))

# COMMAND ----------

# DBTITLE 1,Passo 3: Retirando o cabeçalho do arquivo
moviesHeader = moviesRDD_2.first()
ratingsHeader = ratingsRDD_2.first()

moviesRDD_3 = moviesRDD_2.<ESCREVA_SEU_CODIGO_AQUI>
ratingsRDD_3 = ratingsRDD_2.<ESCREVA_SEU_CODIGO_AQUI>

(moviesRDD_3.take(5), ratingsRDD_3.take(5))

# COMMAND ----------

# DBTITLE 1,Criando a chave dos RDDs pelo campo movieId
moviesRDD_4 = moviesRDD_3.<ESCREVA_SEU_CODIGO_AQUI>
ratingsRDD_4 = ratingsRDD_3.<ESCREVA_SEU_CODIGO_AQUI>

(moviesRDD_4.take(5), ratingsRDD_4.take(5))

# COMMAND ----------

# DBTITLE 1,Realizando Join dos dois RDDs
joinedRDD = <ESCREVA_SEU_CODIGO_AQUI>

joinedRDD.take(10)

# COMMAND ----------

# DBTITLE 1,Contando a quantidade de vezes que cada filme foi avaliado ordenado de forma decrescente
countRatingsRDD = joinedRDD.<ESCREVA_SEU_CODIGO_AQUI>

countRatingsRDD.take(10)

# COMMAND ----------

# DBTITLE 1,Salvando o RDD em um arquivo texto
# Caso o diretório já exista em uma segunda tentativa, utilize o comando comentado abaixo
# dbutils.fs.rm("FileStore/count_movies/", recurse=True)

save_path = "FileStore/count_movies/"
countRatingsRDD.<ESCREVA_SEU_CODIGO_AQUI>
