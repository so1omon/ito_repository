import cx_Oracle # Oracle DB 연동
import pymysql # Maria DB 연동 
import pandas as pd # dataframe 사용을 위한 패키지
import os, sys, traceback
import platform
import lib, merge, db, query, test, create
from datetime import timedelta, datetime

os_name=platform.system() # 운영체제 정보 (Windows/Linux)
# os_name='Linux' # 테스트용

interval_sta=interval_end='' # 기록 생성 시작 및 끝기간
one_day=timedelta(days=1)
today=''

if os_name=='Windows':
    print('Commute log 생성 기간을 설정해주세요. 특정 날짜의 기록만 생성하려면 동일한 날짜를 입력해주세요.\n')
    while True:
        interval_sta='20220401'#input('commute log 생성 시작 기간을 설정해주세요. (YYYYMMDD)\n').strip()
        interval_end='20220401'#input('commute log 생성 끝 기간을 설정해주세요. (YYYYMMDD)\n').strip()
        if lib.isDate(interval_sta, interval_end):
            print(f'{interval_sta}~{interval_end} 기록 생성')
            interval_sta=datetime.strptime(interval_sta,"%Y%m%d")
            interval_end=datetime.strptime(interval_end,"%Y%m%d")
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
    os.environ["PATH"]=f'{os.environ["PATH"]};{db.check_location(os_name)}' #프로그램 실행 중인 동안만 PATH 환경변수에 instant client path 추가
    
    print('Try to access Oracle DB...')
    ora_conn = cx_Oracle.connect(db.cx_info.get('id'),db.cx_info.get('pw'),db.cx_info.get('host')) 
    ora_cur = ora_conn.cursor() # oracle 접속시 사용할 cursor
    print('Oracle access successfully!')
    
    print('Try to access Maria DB')
    mysql_conn=pymysql.connect(**db.pymysql_info)
    mysql_cur=mysql_conn.cursor() # mariadb 접속시 사용할 cursor
    print('MariDB access successfully!')
    
    while interval_sta<=interval_end: # 날짜별 루프 돌기
        
        today=interval_sta.strftime('%Y%m%d') # 루프가 돌아가는 일자
        print(f'{today} 시작')
        
        days_offset=(datetime.now()-interval_sta).days # 현재날짜와 interval_sta간 날짜 차이
        interval_sta=interval_sta+one_day # 시작기간 하루 늘리기
        
        ora_cur.execute(query.oracle_get_appl_sql.format(days_offset)) # 신청정보 가져오는 쿼리 수행
        x=ora_cur.fetchall() # 레코드를 리스트 형식으로 저장
        
        origin_table=pd.DataFrame(x) 
        origin_table.columns=db.col_origin_table # origin table 컬럼 설정
        
        origin_table = origin_table.drop(index=origin_table.loc[origin_table.DEL_YN == 'Y'].index) # DEL_YN이 Y인 행 삭제
        origin_table = origin_table.drop(index=origin_table.loc[origin_table.BF_APPL_ID != 'None'].index)  # 삭제 대상이 되는 행 삭제 
        # bf_appl_id가 NULL이 아닌 행 삭제
        origin_table = origin_table.drop(index=origin_table.loc[(origin_table.TYPE=='1010') & (origin_table.YMD == 'None')].index)
        
        origin_table.drop(['DEL_YN', 'BF_APPL_ID'], axis = 'columns', inplace= True)
        origin_table.loc[origin_table.TYPE =='1044','YMD']=today # TYPE '1044'=> 근무유형(재택근무)신청
        origin_table.reset_index(inplace=True, drop=False) # drop으로 유실된 index 다시 채우기
        
        #merge table 생성
        mysql_cur.execute(f"SELECT {today}, emp_id, emp_nm, org_nm FROM connect.hr_info") # 직원정보 가져오는 쿼리 수행
        x=mysql_cur.fetchall()
        merge_table=pd.DataFrame(x)
        
        merge_table.columns=db.col_merge_table[:4]  #['YMD','EMP_ID','NAME','ORG_NM']
        
        ora_cur.execute(query.oracle_insert_table.format(days_offset)) # SHIFT_CD, WORK_TYPE 가져오는 쿼리 수행
        x=ora_cur.fetchall()
        insert_table=pd.DataFrame(x) #근무유형 데이터프레임
        insert_table.columns=db.col_insert_table
    
        merge_table=pd.merge(merge_table, insert_table) # merge_table에 SHIFT_CD, WORK_TYPE 삽입
        
        for col in db.col_merge_table[6:]: # merge table columns 추가
            merge_table[col]='None'
            
        
        pd.set_option('display.max_row', 500)  # df 최대 출력 행 개수 설정  
        merge_table=merge.origin_to_merge(origin_table, merge_table) # origin table 정보 merge table로 합쳐주기
        merge_table.drop(merge_table[merge_table['SHIFT_CD']=='None'].index, inplace=True) # shift_cd 정보가 없는 행 삭제
        merge_table=merge_table.reset_index(drop=True)
        
        
        # 계획시간 만들기 # <- 박소현
        
        # 기록기시간 만들기 # <- 김솔민
        create.insert_inout(today,merge_table, mysql_cur)
        # 확정시간 만들기 # <- 공동작업
        
        # 초과근무시간 판별 # <- 원래있던거 쓰기
        
        # 급량비 지급여부 판별 # <- 원래있던거 쓰기

except Exception as e:
    print(e)
    traceback.print_exc()
    sys.exit()

finally:
    # DB connection close()
    pass





