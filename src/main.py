from src.UDF.create_env import create_env


def main():
    # 创建 SparkContext
    spark = create_env()

    # 运行 a.py 任务
    spark.runPyFile("edw.h02_branch_d.py")

if __name__ == "__main__":
    main()