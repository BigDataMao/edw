from pyspark.sql import SparkSession


# spark入口
def create_env():
    spark = SparkSession.builder \
        .appName("HiveTest") \
        .master("local") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("hive.metastore.uris", "thrift://master:9083") \
        .config("hive.exec.scratchdir", "/user/hive/tmp") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark
