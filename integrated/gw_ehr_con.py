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

    ora_cur.execute(oracle_get_ehr_con_sql) # 개인별 연차잔여정보, 개인/기관 교육시간 조회
    first_table=pd.DataFrame(ora_cur.fetchall())
    
    first_table.columns=db.col_first_table # 컬럼 설정
    first_table = first_table.drop(index=first_table.loc[first_table.dayoff_rest_time == None].index) # 잘못된 연차잔여정보 삭제
    first_table.reset_index(inplace=True, drop=False) # 삭제 후 꼭 index 재정렬을 해줘야 함
    
    mysql_cur.execute(pymysql_get_ehr_con_sql.format(*lib.month_interval(datetime.now()))) # 개인별 초과근무시간 조회
    
    second_table=pd.DataFrame(mysql_cur.fetchall())
    second_table.columns=db.col_second_table # 컬럼 설정
    
    first_table=pd.merge(first_table, second_table); # oracle, mysql에서 받아온 각각의 정보 병합
    first_table=first_table[db.col_first_merged_table]; # 컬럼 추가
    first_table=first_table.fillna('0.0'); # 테스트계정같은 nan(비어있는 데이터) 존재 시 0.0으로 초기화
    
    # 총 초과근무 시간, 잔여 연차 시간 전처리
    first_table['TOTAL_OVERTIME']=first_table['TOTAL_OVERTIME'].map(lambda x: lib.min_to_str(int(str(x).zfill(3))))
    first_table['dayoff_rest_time']=first_table['dayoff_rest_time'].map(lambda x: str(int(float(x)*100)))
    
    parameters='%s, '*10
    
    mysql_cur.execute('truncate table connect.gw_ehr_con') # 테이블 비운 후 넣기
    
    for i in range(len(first_table)):        
        sql=f"INSERT INTO connect.gw_ehr_con values ({parameters[:-2]})" # 연번 + 10개의 parameters
        mysql_cur.execute(sql, list(map(str,list(first_table.loc[i]))))

except Exception as e:
    print(e)
finally:
    pass