from pyspark.sql.functions import *

from UDF.task_env import create_env, return_to_hive
from UDF.get_subentry import get_subentry
from UDF.parse_arguments import parse_arguments
from pyspark.sql.types import StringType


target_table = 'edw.h02_client_qh_d'  # 目标表
insert_mode = "overwrite"  # 插入模式
busi_date = parse_arguments()  # 解析命令行参数
spark = create_env()  # spark入口
udf_get_subentry = spark.udf.register('udf_get_subentry', get_subentry, StringType())  # 注册自定义函数

# 客户ctp主表
df_ctp = spark.table('ods.t_ctp20_investor_d')
# 客户cap主表-优先级高,但多一些脏数据,investorid is null
df_cap = spark.table('ods.t_cap_investor_d')
# 与主表一对一,查冻结状态,规范状态,激活日期等
df_stat = spark.table('ods.t_ctp20_investorunifyproperty_d')
# 存密码,一般无数据
df_pw = spark.table('ods.t_ctp20_account_d') \
    .select(trim(col('accountid')).alias('investorid'),
            trim(col('password')).alias('password'),
            coalesce(trim(col('currencyid')), lit('CNY')).alias('currencyid')) \
    .filter(col('currencyid') == 'CNY')
# 存地址信息
df_addr = spark.table('ods.t_ctp20_investorex_d') \
    .select(trim(col('investorid')).alias('investorid'),
            trim(col('idcardaddress')).alias('idcardaddress'))
# 查学历,一般无数据
df_edu = spark.table('ods.t_ctp20_investorproprietyexinfo_d') \
    .select(trim(col('investorid')).alias('investorid'),
            trim(col('education')).alias('education'))

# 连表
df = df_ctp.join(df_cap, on='investorid', how='left') \
    .join(df_stat, on='investorid', how='inner') \
    .join(df_addr, on='investorid', how='left') \
    .join(df_pw, on='investorid', how='left') \
    .join(df_edu, on='investorid', how='left')

# 筛数据
df_result = df.select(
    trim(df_ctp['investorid']).alias('client_id'),  # 客户号
    trim(df_ctp['brokerid']).alias('brokers_id'),  # 经济公司
    coalesce(trim(df_cap['departmentid']), trim(df_ctp['departmentid'])).alias('branch_id'),  # 分支机构
    coalesce(trim(df_cap['investorname']), trim(df_ctp['investorname'])).alias('client_name'),  # 客户名
    coalesce(trim(df_cap['investorfullname']), trim(df_ctp['investorfullname'])).alias('client_fname'),  # 客户全名
    udf_get_subentry(lit('SQ63'),
                     lit('ct_ck.client_type'),
                     coalesce(df_cap['investortype'], df_ctp['investortype'])).alias('client_type'),  # 客户类型
    udf_get_subentry(lit('SQ63'),
                     lit('ct_ck.id_type'),
                     coalesce(df_cap['identifiedcardtype'], df_ctp['identifiedcardtype'])).alias('id_type'),  # 证件类型
    coalesce(trim(df_cap['identifiedcardno']), trim(df_ctp['identifiedcardno'])).alias('id_no'),  # 证件号码
    udf_get_subentry(lit('SQ63'),
                     lit('ct_ck.gender'),
                     coalesce(df_cap['sex'], df_ctp['sex'])).alias('gender'),  # 性别
    udf_get_subentry(lit('SQ63'),
                     lit('ct_ck.profession'),
                     df_cap['occupation']).alias('profession_code'),  # 职业
    udf_get_subentry(lit('SQ63'),
                     lit('ct_ck.nationality'),
                     coalesce(df_cap['nationality'], df_ctp['national'], df_ctp['country'])).alias('nationality'),  # 国籍
    coalesce(trim(df_cap['zipcode']), trim(df_ctp['zipcode'])).alias('zipcode'),  # 邮编
    coalesce(trim(df_cap['mobile']), trim(df_ctp['mobile'])).alias('mobile'),  # 手机
    coalesce(trim(df_cap['telephone']), trim(df_ctp['telephone'])).alias('home_tel'),  # 家庭电话
    coalesce(trim(df_cap['email']), trim(df_ctp['email'])).alias('email'),  # 电子邮箱
    coalesce(trim(df_cap['fax']), trim(df_ctp['fax'])).alias('fax'),  # 传真
    coalesce(trim(df_cap['opendate']), trim(df_ctp['opendate']), lit('20010101')).alias('open_date'),  # 开户日期
    coalesce(trim(df_cap['canceldate']), trim(df_ctp['canceldate'])).alias('close_date'),  # 销户日期
    coalesce(trim(df_cap['idcardvaliditystart']), trim(df_ctp['idcardvaliditystart'])).alias('id_begin_date'),
    # 证件有效起始日
    coalesce(trim(df_cap['idcardaddress']), trim(df_addr['idcardaddress'])).alias('id_address'),  # 证件地址
    udf_get_subentry(lit('SQ63'),
                     lit('ct_ck.risk_level'),
                     coalesce(df_cap['risklevel'], df_ctp['risklevel'])).alias('risk_level'),  # 风险等级
    lit('CTP20').alias('data_source'),  # 数据来源
    coalesce(trim(df_cap['freezestatus']), trim(df_stat['freezestatus'])).alias('frozen_status'),  # 冻结状态
    coalesce(trim(df_cap['standardstatus']), trim(df_stat['standardstatus'])).alias('standard_status'),  # 规范状态
    coalesce(trim(df_cap['freezedate']), trim(df_stat['freezedate'])).alias('frozen_date'),  # 冻结日期
    coalesce(trim(df_cap['activedate']), trim(df_stat['activedate'])).alias('active_date'),  # 激活日期
    udf_get_subentry(lit('SQ63'),
                     lit('ct_ck.isactive'),
                     coalesce(df_cap['isactive'], df_ctp['isactive'])).alias('isactive'),  # 状态是否活跃
    trim(df_pw['password']).alias('password'),  # 密码
    coalesce(trim(df_cap['province']), trim(df_ctp['province'])).alias('pro_code'),  # 省
    coalesce(trim(df_cap['city']), trim(df_ctp['city'])).alias('city_code'),  # 市
    coalesce(trim(df_cap['phoneareacode']), trim(df_ctp['region'])).alias('area_code'),  # 区
    coalesce(trim(df_ctp['investorgroupid']), lit('0')).alias('group_id'),  # 分组
    coalesce(trim(df_cap['clientregion']), trim(df_ctp['clientregion'])).alias('client_region'),  # 开户客户地域
    coalesce(trim(df_cap['classify']), trim(df_ctp['classify'])).alias('classify'),  # 投资者客户分类码
    coalesce(trim(df_cap['appropriatype']), trim(df_ctp['appropriatype'])).alias('appropriate_type'),  # 适当性投资者类型
    coalesce(trim(df_cap['contractcode']), trim(df_ctp['contractcode'])).alias('contract_no'),  # 合同号
    trim(df_edu['education']).alias('education_code'),  # 学历
    trim(df_ctp['commmodelid']).alias('commmodelid'),  # 手续费率模板代码
    trim(df_ctp['marginmodelid']).alias('margin_commmodelid'),  # 保证金率模板代码
    coalesce(trim(df_cap['linkman']), trim(df_ctp['linkman'])).alias('linkman'),  # 联系人
    udf_get_subentry(lit('SQ63'),
                     lit('ct_ck.assetmanagetype'),
                     coalesce(df_cap['assetmgrclienttype'], df_ctp['assetmgrclienttype'], lit('1'))).alias('assetmanagetype'),  # 资管类型
    coalesce(trim(df_cap['clientmode']), trim(df_ctp['clientmode']), lit('0')).alias('clientmode'),  # 开户模式
    coalesce(trim(df_cap['idcardaddress']), trim(df_addr['idcardaddress'])).alias('card_address'),  # 身份证件人所在详细地址
    coalesce(trim(df_cap['birthday']), trim(df_ctp['birthday'])).alias('client_birthday'),  # 客户生日
    coalesce(trim(df_cap['institutionextracode']), trim(df_ctp['institutionextracode'])).alias('extra_code'),  # 附加码
    when(coalesce(trim(df_cap['identifiedcardtype']), trim(df_ctp['identifiedcardtype'])) == '0',
         coalesce(trim(df_cap['identifiedcardno']), trim(df_ctp['identifiedcardno'])))
    .otherwise('').alias('org_code'),  # 组织代码(自然人置为空)
    trim(df_cap['occupation']).alias('profession_id'),
    udf_get_subentry(lit('SQ63'),
                     lit('ct_ck.province_cn'),
                     coalesce(df_cap['province'], df_ctp['province'])).alias('addr_province'),  # 省_cn
    udf_get_subentry(lit('SQ63'),
                     lit('ct_ck.city_cn'),
                     coalesce(df_cap['city'], df_ctp['city'])).alias('addr_city'),  # 市_cn
    coalesce(trim(df_cap['licenseno']), trim(df_ctp['licenseno'])).alias('license_no'),  # 许可证
)

# 写入hive
return_to_hive(spark, df_result, target_table, insert_mode)
