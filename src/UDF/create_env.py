from pyspark.sql import SparkSession


# spark入口
def create_env():
    spark = SparkSession.builder \
        .appName("HiveTest") \
        .config("spark.sql.warehouse.dir", "hdfs://567d88c67dac:9000/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark
