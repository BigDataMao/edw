# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from UDF.get_subentry import get_subentry
# from pyspark.sql.types import StringType
import argparse  # 导入argparse模块，用于解析命令行参数
from pyspark.sql.functions import *

parser = argparse.ArgumentParser()  # 创建解析对象
parser.add_argument('--busi_date', help='business date parameter', default=None)  # 添加参数细节
busi_date = parser.parse_args().busi_date  # 获取参数

# spark入口
spark = SparkSession.builder \
    .appName("HiveTest") \
    .config("spark.sql.warehouse.dir", "hdfs://567d88c67dac:9000/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

udf_get_subentry = spark.udf.register('udf_get_subentry', get_subentry, StringType())

# DSL风格
df = spark.table("ods.t_ctp20_department_d").select(
    trim(col("departmentid")).alias("branch_id"),
    trim(col("departmentid")).alias("branch_no"),
    trim(col("departmentname")).alias("branch_name"),
    when(trim(col("departmentid")) == 'FU', '00')
    .when(length(trim(col("departmentid"))) == 2, 'FU')
    .when(length(trim(col("departmentid"))) == 4, substring(trim(col("departmentid")), 1, 2))
    .when(length(trim(col("departmentid"))) == 6, substring(trim(col("departmentid")), 1, 4))
    .otherwise(substring(trim(col("departmentid")), 1, 6)).alias("up_branch_no"),
    lit('0').alias("branch_type"),
    lit('00').alias("data_source"),
    col("brokerid").alias("brokers_id"),
    lit(busi_date).alias("ds_date")
)

# 获取目标表的元数据信息
target_columns = [c.name for c in spark.table("edw.h02_branch_d").schema]

# 添加缺失的列并设置默认值
for c in target_columns:
    if c not in df.columns:
        df = df.withColumn(c, lit(None))

# 覆盖edw.h02_branch_d表中的数据
df.write.mode('overwrite').insertInto("edw.h02_branch_d")
