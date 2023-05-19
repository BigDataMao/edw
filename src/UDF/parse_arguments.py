import argparse


def parse_arguments():
    # 解析命令行所传参数
    parser = argparse.ArgumentParser()  # 创建解析参数的对象
    parser.add_argument('--busi_date', help='business date parameter', default=None)  # 添加参数细节
    busi_date = parser.parse_args().busi_date  # 获取参数
    return busi_date
