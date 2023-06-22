import cx_Oracle # Oracle 연동 모듈
import os
import pymysql # Maria DB 연동 모듈
import pandas as pd # Dataframe 사용
import sys, traceback # 예외처리 시 사용
from datetime import datetime, timedelta # 시간처리 시 사용
from login_info import cx_Oracle_info as oracle_info, pymysql_info as mysql_info # DB 로그인 정보

try:
    pass
    
except Exception as e:
    print(e)
    traceback.print_exc()
    sys.exit()
finally:
    conn.commit()
    conn.close()
    OracleConnect.close()
