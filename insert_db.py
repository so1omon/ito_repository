import cx_Oracle         # 오라클과 연동하는 패키지
import os
import pymysql             # mariadb와 연동하는 패키지

LOCATION = ".\instantclient-basic-windows.x64-21.3.0.0.0\instantclient_21_3"         # 오라클 연동하는 프로그램의 위치 필요.
os.environ["PATH"] = LOCATION + ";" + os.environ["PATH"]
OracleConnect = cx_Oracle.connect("silver", "silver", "192.168.20.13:1521/IDTCORA")       # 오라클 연동 정보입력
OracleCursor = OracleConnect.cursor()           #오라클 sql문 쓰기 위한 커서

conn=pymysql.connect(host='192.168.20.19', user='root', password='Azsxdc123$', db='connect', charset='utf8')       # mariadb 연동 정보입력
cur=conn.cursor()                # mariadb sql문 쓰기 위한 커서

oracleSql = f"""
select NVL(A.trg_emp_id, 'NULL') AS EMP_ID, A.appl_ymd, NVL(NVL(B.ymd, C.ymd), 'NULL') AS YMD, NVL(NVL(B.sta_hm, C.sta_hm), 'NULL') AS STA_HM, 
NVL(NVL(B.end_hm, C.end_hm),'NULL') AS END_HM, NVL(A.appl_type, 'NULL') AS TYPE, a.appl_id AS APPL_ID, NVL(NVL(b.del_yn, c.del_yn), 'NULL') AS DEL_YN, NVL(a.BF_APPL_ID, 'NULL') AS BF_APPL_ID
from ehr2011060.sy7010 A
left join (select appl_id, ymd, sta_hm, end_hm, del_yn
from ehr2011060.tam2215) B
on a.appl_id = b.appl_id
left join(select ymd, attend_cd, sta_hm, end_hm, appl_id, del_yn
from ehr2011060.tam5450) C
on a.appl_id = c.appl_id
where a.appl_stat_cd = '900' and a.appl_ymd=(SELECT TO_CHAR(SYSDATE, 'YYYYMMDD')AS YYYYMMDD FROM DUAL)
"""                                          # 오라클 sql문 
OracleCursor.execute(oracleSql)               # sql문 실행


# for line in OracleCursor:
        
        # sql4 ="INSERT INTO `db_test` (`EMP_ID`, `APPL_YMD`, `YMD`, `STA_HM`, `END_HM`, `TYPE`, `APPL_ID`, `DEL_YN`,`BF_APPL_ID`) VALUES"+" ("+'"'+line[0]+'"'+","+"'"+line[1]+"'"+","+"'"+line[2]+"'"+","+"'"+line[3]+"'"+","+"'"+line[4]+"'"+","+"'"+line[5]+"'"+","+"'"+line[6]+"'"+","+"'"+line[7]+"'"+","+"'"+line[8]+"'"+")"        
sql3 ="INSERT INTO `db_test` (`EMP_ID`, `APPR_YMD`, `YMD`, `STA_HM`, `END_HM`, `TYPE`, `APPL_ID`, `DEL_YN`,`BF_APPL_ID`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"        
cur.executemany(sql3, OracleCursor)
    
conn.commit()            # mariadb 변경사항 저장
conn.close()            # mariadb 종료