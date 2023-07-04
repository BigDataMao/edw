# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

from get_subentry import get_subentry

# spark入口
spark = SparkSession.builder \
    .appName("HiveTest") \
    .config("spark.sql.warehouse.dir", "hdfs://567d88c67dac:9000/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

udf_get_subentry = spark.udf.register('udf_get_subentry', get_subentry, StringType())

spark.sql('''
    --select udf_get_subentry('1', '2', '3')
    --show databases
    select * from mao.employee
''').show()
