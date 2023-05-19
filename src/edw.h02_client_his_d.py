from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from UDF.get_subentry import get_subentry
from UDF.parse_arguments import parse_arguments

busi_date = parse_arguments()

# spark入口
spark = SparkSession.builder \
    .appName("HiveTest") \
    .config("spark.sql.warehouse.dir", "hdfs://567d88c67dac:9000/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()
udf_get_subentry = spark.udf.register('udf_get_subentry', get_subentry, StringType())

spark.table('ods.t_ctp20_investor_d').select(

)