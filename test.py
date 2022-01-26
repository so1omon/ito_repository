import cx_Oracle         # 오라클과 연동하는 패키지
import os
import pymysql             # mariadb와 연동하는 패키지
import pandas as pd
from datetime import datetime
from login_info import cx_Oracle_info as cxinfo, pymysql_info as mysqlinfo

LOCATION = "..\instantclient-basic-windows.x64-21.3.0.0.0\instantclient_21_3"         # 오라클 연동하는 프로그램의 위치 필요.
os.environ["PATH"] = LOCATION + ";" + os.environ["PATH"]
OracleConnect = cx_Oracle.connect(cxinfo['id'], cxinfo['pw'], cxinfo['host'])       # 오라클 연동 정보입력
OracleCursor = OracleConnect.cursor()  #오라클 sql문 쓰기 위한 커서

conn=pymysql.connect(host=mysqlinfo['host'], user=mysqlinfo['user'], password=mysqlinfo['password'], 
                     db=mysqlinfo['db'], charset=mysqlinfo['charset'])       # mariadb 연동 정보입력
cur=conn.cursor() #pymysql 커서

days=int(input("몇일 전 데이터를 가져오는지?"))
print(days)
oracleSql = f"""
 select NVL(A.trg_emp_id, 'NULL') AS EMP_ID, A.appr_ymd, NVL(NVL(B.ymd, C.ymd), 'NULL') AS YMD, NVL(NVL(B.sta_hm, C.sta_hm), 'NULL') AS STA_HM, 
    NVL(NVL(B.end_hm, C.end_hm),'NULL') AS END_HM, NVL(A.appl_type, 'NULL') AS TYPE, a.appl_id AS APPL_ID, 
    NVL(NVL(b.del_yn, c.del_yn), 'NULL') AS DEL_YN, NVL(a.BF_APPL_ID, 'NULL') AS BF_APPL_ID, a.appl_txt as APPL_TXT, NVL(B.reward_type, 'NULL') AS REWARD_TYPE
    from ehr2011060.sy7010 A
    left join (select appl_id, ymd, sta_hm, end_hm, del_yn, reward_type
    from ehr2011060.tam2215) B
    on a.appl_id = b.appl_id
    left join(select ymd, attend_cd, sta_hm, end_hm, appl_id, del_yn
    from ehr2011060.tam5450) C
    on a.appl_id = c.appl_id
    where a.appl_stat_cd = '900' and (( (a.appl_type='1002' or a.appl_type='1004' or a.appl_type='1008' or a.appl_type='1010') 
    and NVL(B.ymd, C.ymd) between (SELECT TO_CHAR(SYSDATE-22, 'YYYYMMDD')AS YYYYMMDD FROM DUAL) and (SELECT TO_CHAR(SYSDATE-1, 'YYYYMMDD')AS YYYYMMDD FROM DUAL)) 
    or a.appl_type='1044' and substr(a.appl_txt,5,10) between (SELECT TO_CHAR(SYSDATE-22, 'YYYYMMDD')AS YYYYMMDD FROM DUAL) and (SELECT TO_CHAR(SYSDATE-1, 'YYYYMMDD')AS YYYYMMDD FROM DUAL)) and a.appl_type='1008'
    order by EMP_ID, YMD
"""
OracleCursor.execute(oracleSql)
origin_table=pd.DataFrame()

befor_data={}
count=0
for line in OracleCursor:
    data={'EMP_ID':line[0], 'APPR_YMD':line[1], 'YMD':line[2], 'STA_HM':line[3], 'END_HM':line[4], 'TYPE':line[5], 
        'APPL_ID':line[6], 'DEL_YN':line[7], 'BF_APPL_ID':line[8], 'APPL_TXT':line[9], 'REWARD_TYPE':line[10]}
    if count==0:
        before_data=dict(data)
        count+=1
        continue
    else:
        if before_data['YMD']==data['YMD'] and before_data['EMP_ID']==data['EMP_ID']:
            print(before_data)
            print(data)
            print()
        before_data=dict(data)
        count+=1
    