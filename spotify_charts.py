# Installing and importing packages

#!pip install pyspark
import pyspark
import pandas as pd
import numpy as np

#from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f

#In this project, we're going to use Spark (large dataset, with more than 28 milions lines) and SQL to explore the dataset.
#The first step working with Spark, is create a instance of class "Spark Session".

# Building a Spark session - entry point to underlying Spark funtionality
spark = (SparkSession.builder.config("spark.driver.memory","4g").config("spark.driver.maxResultSize", "4g").getOrCreate())

# Importing dataset (with spark)
data_spk = spark.read.csv(path = 'charts.csv', inferSchema = True, header = True)

data_spk.head(2)
#data_spk.dtypes()