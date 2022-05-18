from query import oracle_get_ehr_con_sql
import cx_Oracle # Oracle DB 연동
import pymysql # Maria DB 연동 
import db,os,lib,datetime
import platform
import sys, traceback
from datetime import datetime

os_name=platform.system() # 운영체제 정보 (Windows/Linux)
if os_name=='Windows':
    os.environ["PATH"]=f'{os.environ["PATH"]};{db.check_location(os_name)}'
        # 오늘날짜 -> 월을 알아내고 -> 시작, 끝 날짜 정하면 됨
    interval_sta = '20220501'
    interval_end = '20220531'
    interval_sta = datetime.strptime(interval_sta, "%Y%m%d")
    interval_end = datetime.strptime(interval_end, "%Y%m%d")
        
elif os_name=='Linux':
    os.environ["PATH"]=f'../{db.check_location(os_name)}'
    interval_sta=datetime.now()
    interval_end=datetime.now()
    
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
    for i in ora_cur:
        print(i)
        
    
except Exception as e:
    print(e)
finally:
    pass