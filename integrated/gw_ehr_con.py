from query import oracle_get_ehr_con_sql, pymysql_get_ehr_con_sql
import cx_Oracle # Oracle DB 연동
import pymysql # Maria DB 연동 
import db,os,lib,datetime
import platform
import sys, traceback
from datetime import datetime
import pandas as pd
import lib

os_name=platform.system() # 운영체제 정보 (Windows/Linux)
if os_name=='Windows':
    os.environ["PATH"]=f'{os.environ["PATH"]};{db.check_location(os_name)}'
        
elif os_name=='Linux':
    os.environ["PATH"]=f'../{db.check_location(os_name)}'
    
else: # Windows나 리눅스가 아닐 때 강제 종료
    print('Unknown OS version. \nProgram exit')
    sys.exit() 
    
try:
    print('Try to access Oracle DB...')
    ora_conn = cx_Oracle.connect(db.cx_info.get('id'),db.cx_info.get('pw'),db.cx_info.get('host')) 
    ora_cur = ora_conn.cursor() # oracle 접속시 사용할 cursor
    print('Oracle access successfully!')
    
    print('Try to access Maria DB')
    mysql_conn=pymysql.connect(**db.pymysql_info)
    mysql_cur=mysql_conn.cursor() # mariadb 접속시 사용할 cursor
    print('MariDB access successfully!')

    ora_cur.execute(oracle_get_ehr_con_sql)     #연차정보,
    x=ora_cur.fetchall()
    first_table=pd.DataFrame(x)
    first_table.columns=db.col_first_table
    first_table = first_table.drop(index=first_table.loc[first_table.dayoff_rest_time == None].index)
    first_table.reset_index(inplace=True, drop=False)
    print(pymysql_get_ehr_con_sql.format(*lib.month_interval(datetime.now())))
    mysql_cur.execute(pymysql_get_ehr_con_sql.format(*lib.month_interval(datetime.now())))
    x=mysql_cur.fetchall()
    second_table=pd.DataFrame(x)
    second_table.columns=db.col_second_table
    
    first_table=pd.merge(first_table, second_table)
    first_table=first_table[db.col_first_merged_table]
    first_table['TOTAL_OVERTIME']=first_table['TOTAL_OVERTIME'].map(lambda x: lib.min_to_str(int(str(x).zfill(3))))
    first_table['dayoff_rest_time']=first_table['dayoff_rest_time'].map(lambda x: str(x))
    
    fisrt_table=first_table.reset_index(inplace=True, drop=True)
    
    parameters='%s, '*10
    
    mysql_cur.execute('truncate table connect.gw_ehr_con')
    
    for i in range(len(first_table)):
        print(list(map(str,list(first_table.loc[i]))))
        
        sql=f"INSERT INTO connect.gw_ehr_con values ({parameters[:-2]})" #날짜별 NUM(사번연번) + 27개의 parameters
        print(sql)
        mysql_cur.execute(sql, list(map(str,list(first_table.loc[i]))))

except Exception as e:
    print(e)
finally:
    pass