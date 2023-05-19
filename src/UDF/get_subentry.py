# -*- coding: utf-8 -*-
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# 字典:仅包含映射值不同的部分
dict_entry_map = {
    "SQ63": {
        "ct_ctp.productclass": {
            "1": "11",
            "2": "12",
            "6": "12"
        },
        "ct_ctp.uoaidcardtype": {
            "A": "a",
            "B": "b",
            "C": "c",
            "D": "d",
            "F": "f",
            "G": "g",
            "H": "h",
            "I": "i",
            "J": "j",
            "K": "k",
            "L": "l",
            "M": "P",
            "N": "y",
            "O": "N"
        },
        "ct_ctp.trade_type": {
            "31": "2",
            "11": "0",
            "12": "0",
            "21": "1"
        },
        "ct_ctp.bankflag": {
            "Z": "z"
        },
        "ct_ctp.currencyid": {
            "AUD": "7",
            "CAD": "6",
            "CHF": "5",
            "CNY": "0",
            "EUR": "8",
            "GBP": "4",
            "HKD": "2",
            "JPY": "3",
            "OTH": "9",
            "USD": "1"
        }
    }
}


# 定义python查字典函数
def get_subentry(data_source, sc_entry_no, sc_subentry):
    if data_source in dict_entry_map and \
            sc_entry_no in dict_entry_map[data_source] and \
            sc_subentry in dict_entry_map[data_source][sc_entry_no]:
        return dict_entry_map[data_source][sc_entry_no][sc_subentry]
    else:
        return sc_subentry  # 字典里面没有就取自身


# 注册到sparksql
# udf_get_subentry = udf(get_subentry, StringType())


# # spark入口
# spark = SparkSession.builder \
#     .appName("HiveTest") \
#     .config("spark.sql.warehouse.dir", "hdfs://2eb73e38ef63:9000/user/hive/warehouse") \
#     .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
#     .enableHiveSupport() \
#     .getOrCreate()
#
