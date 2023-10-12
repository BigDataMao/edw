from src.UDF.task_env import create_env


def main():
    # 创建 SparkContext
    spark = create_env()

    # 运行 a.py 任务
    spark.run_task('a')


if __name__ == "__main__":
    main()
