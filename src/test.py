from UDF.create_env import create_env

create_env().sql('show databases').show(5)
create_env().table('ods.t_ctp20_investor_d').show(5)
