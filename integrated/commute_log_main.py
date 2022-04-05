import cx_Oracle # Oracle DB 연동
import pymysql # Maria DB 연동 
import pandas as pd # dataframe 사용을 위한 패키지
import os, sys, traceback
import platform
from login_info import cx_Oracle_info as cx_info, pymysql_info as mysql_info, check_location
from validate import isDate
from datetime import timedelta, datetime
import query

os_name=platform.system() # 운영체제 정보 (Windows/Linux)
# os_name='Linux' # 테스트용

interval_sta=interval_end='' # 기록 생성 시작 및 끝기간
one_day=timedelta(days=1)

if os_name=='Windows':
    
    print('Commute log 생성 기간을 설정해주세요. 특정 날짜의 기록만 생성하려면 동일한 날짜를 입력해주세요.\n')
    while True:
        interval_sta=input('commute log 생성 시작 기간을 설정해주세요. (YYYY-MM-DD)\n').strip()
        interval_end=input('commute log 생성 끝 기간을 설정해주세요. (YYYY-MM-DD)\n').strip()
        if isDate(interval_sta, interval_end):
            print(f'{interval_sta}~{interval_end} 기록 생성')
            interval_sta=datetime.strptime(interval_sta,"%Y-%m-%d")
            interval_end=datetime.strptime(interval_end,"%Y-%m-%d")
            break
        
elif os_name=='Linux':
    interval_sta=datetime.now()-one_day
    interval_end=datetime.now()-one_day
    print(f"{interval_sta.strftime('%Y-%m-%d')}~{interval_end.strftime('%Y-%m-%d')} 기록 생성")
    
else: # Windows나 리눅스가 아닐 때 강제 종료
    print('Unknown OS version. \nProgram exit')
    sys.exit() 
    
   
    
try:
    # 환경변수 세팅 및 DB 접속
    os.environ["PATH"]=f'{os.environ["PATH"]};{check_location(os_name)}' #프로그램 실행 중인 동안만 PATH 환경변수에 instant client path 추가
    
    print('Try to access Oracle DB...')
    ora_conn = cx_Oracle.connect(cx_info.get('id'),cx_info.get('pw'),cx_info.get('host')) 
    ora_cur = ora_conn.cursor() # oracle 접속시 사용할 cursor
    print('Oracle access successfully!')
    
    print('Try to access Maria DB')
    mysql_conn=pymysql.connect(**mysql_info)
    mysql_cur=mysql_conn.cursor() # mariadb 접속시 사용할 cursor
    print('MariDB access successfully!')
    
    # 날짜별 루프 돌기
    while interval_sta<=interval_end: 
        print(f'{interval_sta} 시작')
        
        days_offset=(interval_end-interval_sta).days # 오늘기준 날짜랑 얼마나 차이나는지
        interval_sta=interval_sta+one_day # 시작기간 하루 늘리기
        
        
        ora_cur.execute(query.oracle_get_appl_sql.format(days_offset)) 
        print(list(ora_cur))
        
    
except Exception as e:
    print(e)
    traceback.print_exc()
    sys.exit()

finally:
    # DB connection close()
    pass





