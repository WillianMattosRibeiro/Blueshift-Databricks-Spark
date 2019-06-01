# Databricks notebook source
# DBTITLE 1,Imports
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

from threading import Thread
from datetime import datetime, timedelta, date , time

import shlex, subprocess
import _strptime
import argparse
import random
import uuid
import re
import sys
import time
import os

# COMMAND ----------

# DBTITLE 1, Global Functions
def previous_day(): #Função que gera a data D-2.
	from datetime import time
	current_day = date.today()
	day_previous1 = date.fromordinal(current_day.toordinal()-2) 
	day_previous = day_previous1.strftime('%Y-%m-%d')
	return day_previous


# COMMAND ----------

# DBTITLE 1,Read Database's 
# File location
file_location1 = "/FileStore/tables/countries_population.csv"
file_location2 = "/FileStore/tables/countries_regional_classification.csv"
file_location3 = "/FileStore/tables/countries_area.csv"
file_location4 = "/FileStore/tables/countries_CO2.csv"
file_location5 = "/FileStore/tables/countries_PIB.csv"
file_location6 = "/FileStore/tables/countries_polluton.csv"
file_location7 = "/FileStore/tables/countries_name.csv"
file_location8 = "/FileStore/tables/code_area.csv"

# File type
file_type = "csv"

# CSV options
infer_schema = "True"
first_row_is_header = "True"
delimiter = "|"


# The applied options are for CSV files. For other file types, these will be ignored.
countries_population = spark.read.format(file_type).option("inferSchema", infer_schema).option("header", first_row_is_header).option("sep", delimiter).load(file_location1)

countries_regional_classification = spark.read.format(file_type).option("inferSchema", infer_schema).option("header", first_row_is_header).option("sep", delimiter).load(file_location2)

countries_area = spark.read.format(file_type).option("inferSchema", infer_schema).option("header", first_row_is_header).option("sep", delimiter).load(file_location3)

countries_CO2 = spark.read.format(file_type).option("inferSchema", infer_schema).option("header", first_row_is_header).option("sep", delimiter).load(file_location4)

countries_PIB = spark.read.format(file_type).option("inferSchema", infer_schema).option("header", first_row_is_header).option("sep", delimiter).load(file_location5)

countries_polluton = spark.read.format(file_type).option("inferSchema", infer_schema).option("header", first_row_is_header).option("sep", delimiter).load(file_location6)

countries_name = spark.read.format(file_type).option("inferSchema", infer_schema).option("header", first_row_is_header).option("sep", delimiter).load(file_location7)

code_area = spark.read.format(file_type).option("inferSchema", infer_schema).option("header", first_row_is_header).option("sep", delimiter).load(file_location8)



# COMMAND ----------

# DBTITLE 1,Column Renamed from Dataframes

countries_PIB_2 = countries_PIB.withColumnRenamed('Rank(total)', 'Rank')\
                               .withColumnRenamed('Total(million US-$)','Us_million')\
                               .withColumnRenamed('per Capita(US-$)','Us_per_capita')

countries_population_2 = countries_population.withColumnRenamed('Classificação(total)','Rank')\
                                             .withColumnRenamed('País','Country')\
                                             .withColumnRenamed('Total(pessoas)','Total_people')\
                                             .withColumnRenamed('por km²(pessoas)', 'Km2_people')\
                                             .withColumnRenamed('Encontro', 'Date')

countries_regional_2 = countries_regional_classification.select('Country', 'Region')

countries_area_2 = countries_area.withColumnRenamed('Rank_Country', 'Rank')\
                                 .withColumnRenamed('Total(square_km)', 'Country')\
                                 .withColumnRenamed('per_Capita(square_m)', 'Km2_total') \
                                 .withColumnRenamed('Date', 'M2_people') 

countries_CO2_2 = countries_CO2.withColumnRenamed('Total(metric tons)', 'Metric_tons_total_CO2')\
                               .withColumnRenamed('per Capita(mt)', 'Metric_tons_people_CO2')

countries_polluton_2 = countries_polluton.withColumnRenamed('Pollution Index', 'Pollution_index')\
                                         .withColumnRenamed('Exp Pollution Index', 'Expected_pollution_index')

countries_name_2 = countries_name.withColumnRenamed('paisId', 'Rank')\
                                 .withColumnRenamed('paisNome', 'Name_BR')\
                                 .withColumnRenamed('paisName', 'Country')

# COMMAND ----------

# DBTITLE 1,Convert to Data 01 (withColumn, substring, regexp_replace, translate , Cast)
countries_PIB_3 = countries_PIB_2.withColumn('Date', substring('Date', 0, 4))\
                                 .withColumn('Us_million', translate(col('Us_million'), ',', '').cast('integer'))\
                                 .withColumn('Us_per_capita', translate(col('Us_per_capita'), ',', '').cast('integer'))


countries_population_3 = countries_population_2.withColumn('Date', substring('Date', 0, 4)) \
                                               .withColumn('Total_people', regexp_replace(col('Total_people'), ',', '.')) \
                                               .withColumn('Total_people', translate(col('Total_people'), '.', '')) \
                                               .withColumn('Km2_people', translate(col('Km2_people'), ',', '.')) \
                                               .withColumn('Km2_people', translate(col('Km2_people'), '.', '')) \
                                               .withColumn('Km2_people', (col('Km2_people') /100).cast('decimal(10,2)')) 
  
countries_CO2_3 = countries_CO2_2.withColumn('Date', substring('Date', 0, 4))


countries_polluton_3 = countries_polluton_2.withColumn('Pollution_index', col('Pollution_index').cast('decimal(10,2)')) \
                                           .withColumn('Expected_pollution_index', col('Expected_pollution_index').cast('decimal(10,2)'))

countries_area_3 = countries_area_2.withColumn('M2_people', regexp_replace(col('M2_people'), ',', '.').cast('decimal(10,2)')) \
                                   .withColumn('Km2_total', regexp_replace(col('Km2_total'), ',', '.')) \
                                   .withColumn('Km2_total', translate(col('Km2_total'), '.', '')) \
                                   .withColumn('Km2_total', (col('Km2_total') /10).cast('decimal(20,1)'))

# COMMAND ----------

# DBTITLE 1,Convert to Data 02 (SelectExpr)
countries_regional_3 = countries_regional_2.selectExpr('Country', 'Region')\
                                           .withColumn('Country', upper(col('Country')))

countries_area_4 = countries_area_3.selectExpr('Country', 'M2_people' , 'Km2_total')\
                                   .withColumn('Country', upper(col('Country')))


countries_population_4 = countries_population_3.selectExpr('Country as Country_population', 'Total_people', 'Km2_people')\
                                               .withColumn('Country_population', upper(col('Country_population')))\

countries_PIB_4 =countries_PIB_3.selectExpr('Country', 'Us_million', 'Us_per_capita')\
                                .withColumn('Country', upper(col('Country')))

countries_CO2_4 = countries_CO2_3.selectExpr('Country', 'Metric_tons_total_CO2', 'Metric_tons_people_CO2')\
                                 .withColumn('Country', upper(col('Country')))

countries_polluton_4 = countries_polluton_3.selectExpr('Country', 'Pollution_index', 'Expected_pollution_index')\
                                           .withColumn('Country', upper(col('Country')))

code_area_2 = code_area.withColumn('Country', upper(col('Country')))

# COMMAND ----------

# DBTITLE 1,Joins
#Conditions
condition_join = [countries_name_2.Name_BR  == countries_population_4.Country_population] 

df1 = countries_name_2.join(countries_regional_3, ['Country'], how='left')\
                      .join(countries_area_4, ['Country'] , how='left')\
                      .join(countries_population_4, condition_join, how='left')\
                      .join(countries_polluton_4, ['Country'] , how='left')\
                      .join(countries_PIB_4, ['Country'], how='left')\
                      .join(countries_CO2_4, ['Country'], how='left')\
                      .join(code_area_2, ['Country'], how='left')

df2 = df1.selectExpr('Code' \
                    ,'Rank' \
                    ,'Country' \
                    ,'Region' \
                    ,'Km2_total' \
                    ,'M2_people' \
                    ,'Total_people' \
                    ,'Km2_people' \
                    ,'Us_million' \
                    ,'Us_per_capita' \
                    ,'Pollution_index' \
                    ,'Expected_pollution_index' \
                    ,'Metric_tons_total_CO2'\
                    ,'Metric_tons_people_CO2')



# COMMAND ----------

df2.registerTempTable('yyy')


# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC select Code, Km2_total, Region from yyy where Code is not null order by Km2_total desc

# COMMAND ----------

# DBTITLE 1,Using Machine Learning in PySpark
import numpy as np
import pandas as pd
from fbprophet import *



df1 = countries_population_2.withColumn('date2', lit('2016-02-01'))

m = Prophet()

m.fit(df1)



