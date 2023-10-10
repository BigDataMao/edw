# -*- coding: utf-8 -*-
from pyspark.sql.types import StringType
from pyspark.sql.functions import *
from UDF.get_subentry import get_subentry
from UDF.create_env import create_env
from UDF.parse_arguments import parse_arguments

busi_date = parse_arguments()  # 解析命令行参数
spark = create_env()  # spark入口
udf_get_subentry = spark.udf.register('udf_get_subentry', get_subentry, returnType=StringType())

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
