# -*- coding: utf-8 -*-
"""
Created on Fri Aug 16 16:22:55 2019

@author: sougata.a.biswas
"""

from pathlib import Path
import pandas as pd
import numpy as np
import findspark
# Local Spark
# findspark.init('C:/spark/spark-2.4.3-bin-hadoop2.7')

# Cloudera cluster Spark
findspark.init(spark_home='C:/spark/spark-2.4.3-bin-hadoop2.7')
##Getting PySpark Shell
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('example_app').master('local[*]').getOrCreate()
spark.sql("show databases").show()
#Pandas->Spark
air_quality_df = pd.read_hdf('C:/Users/sougata.a.biswas/Desktop/Hadoop_Python/data/madrid.h5', key='28079008')
air_quality_df.head()
air_quality_df.reset_index(inplace=True)
air_quality_df['date'] = air_quality_df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
#We can simply load from pandas to Spark with createDataFrame
air_quality_sdf = spark.createDataFrame(air_quality_df)
air_quality_sdf.head()
air_quality_sdf.dtypes
air_quality_sdf.select('date', 'NOx').show(5)

#pandas -> spark -> hive
air_quality_sdf.createOrReplaceTempView("air_quality_sdf")

sql_drop_table = """
drop table if exists analytics.pandas_spark_hive
"""

sql_drop_database = """
drop database if exists analytics cascade
"""

sql_create_database = """
create database if not exists analytics
location '/user/cloudera/analytics/'
"""

sql_create_table = """
create table if not exists analytics.pandas_spark_hive
using parquet
as select to_timestamp(date) as date_parsed, *
from air_quality_sdf
"""

print("dropping database...")
result_drop_db = spark.sql(sql_drop_database)

print("creating database...")
result_create_db = spark.sql(sql_create_database)

print("dropping table...")
result_droptable = spark.sql(sql_drop_table)

print("creating table...")
result_create_table = spark.sql(sql_create_table)

spark.sql("select * from analytics.pandas_spark_hive").select("date_parsed", "O_3").show(5)

#Apache Arrow
import pyarrow as pa
import os
#Establish connection
os.environ['ARROW_LIBHDFS_DIR'] = '/opt/cloudera/parcels/CDH-5.14.4-1.cdh5.14.4.p0.3/lib64/'
#facing error in the below line
hdfs_interface = pa.hdfs.connect(host='localhost', port=8020, user='cloudera')
#List files in HDFS
##facing error in the below line
hdfs_interface.ls('/user/cloudera/analytics/pandas_spark_hive/')
#Reading parquet files directly from HDFS
table = hdfs_interface.read_parquet('C:/user/cloudera/analytics/pandas_spark_hive')
##HDFS -> pandas
table_df = table.to_pandas()
table_df.head()




























