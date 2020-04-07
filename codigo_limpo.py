#!/usr/bin/env python
# coding: utf-8


from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import split, regexp_extract, col

from pyspark.sql.functions import *


sc = SparkContext("local", "First App")
sqlCon = SQLContext(sc)

path_hdfs = "hdfs:///semantix/"

NASA_access = sqlCon.read.text(path_hdfs).cache()


df = NASA_access.select(regexp_extract('value', r'^([^\s]+\s)', 1).alias('HOST'),
                        regexp_extract('value', r'^.*\[(\d\d/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]', 1).alias('DATA'),
                        regexp_extract('value', r'^.*"\w+\s+([^\s]+)\s+HTTP.*"', 1).alias('URL'),
                        regexp_extract('value', r'^.*"\s+([^\s]+)', 1).cast('integer').alias('COD_HTTP'),
                        regexp_extract('value', r'^.*\s+(\d+)$', 1).cast('integer').alias('BYTE'))

df = df.withColumn("timestamp", df["DATA"].cast('date')).drop("DATA").withColumnRenamed("timestamp", "DATA")
df = df.withColumn("codigo", df["COD_HTTP"].cast('bigint')).drop("COD_HTTP").withColumnRenamed("codigo", "COD_HTTP")
df = df.withColumn("tamanho", df["BYTE"].cast('bigint')).drop("BYTE").withColumnRenamed("tamanho", "BYTE")


host_unicos = df.groupBy('HOST').count().filter('count = 1').select('HOST').unique()
host_unicos.head()


error_404 = df.groupBy('COD_HTTP').count().filter('COD_HTTP = "404"')
error_404.head()


top_error_404 = df.filter('COD_HTTP = "404"').groupBy('URL').count().sort(col("count").desc())
top_error_404.show(5)


qt_error_day = df.filter('COD_HTTP = "404"').groupBy('DATA').count()
qt_error_day.head()


total_byte = df.select('BYTE').groupBy().sum()
total_byte.head()


# FIM
