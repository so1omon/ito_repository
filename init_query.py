import cx_Oracle 
import os
import pymysql 
import pandas as pd
from datetime import datetime
from login_info import cx_Oracle_info as cxinfo, pymysql_info as mysqlinfo, date_diff #date_diff:날짜 차이 구하기 위한 객체

LOCATION = "..\instantclient-basic-windows.x64-21.3.0.0.0\instantclient_21_3"         # 오라클 연동하는 프로그램의 위치 필요.
os.environ["PATH"] = LOCATION + ";" + os.environ["PATH"]
OracleConnect = cx_Oracle.connect(cxinfo['id'], cxinfo['pw'], cxinfo['host'])       # 오라클 연동 정보입력
OracleCursor = OracleConnect.cursor()  #오라클 sql문 쓰기 위한 커서

conn=pymysql.connect(host=mysqlinfo['host'], user=mysqlinfo['user'], password=mysqlinfo['password'], 
                     db=mysqlinfo['db'], charset=mysqlinfo['charset'])       # mariadb 연동 정보입력
cur=conn.cursor() #pymysql 커서

for days in range(1,date_diff.days+1): #1월 3일부터 실행시간 기준으로 어제까지
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
    and NVL(B.ymd, C.ymd)=(SELECT TO_CHAR(SYSDATE-{days}, 'YYYYMMDD')AS YYYYMMDD FROM DUAL)) 
    or a.appl_type='1044' and substr(a.appl_txt,5,10)=(SELECT TO_CHAR(SYSDATE-{days}, 'YYYY.MM.DD')AS YYYYMMDD FROM DUAL))
    """
    OracleCursor.execute(oracleSql)
    origin_table=pd.DataFrame()

    for line in OracleCursor:
        data={'EMP_ID':line[0], 'APPR_YMD':line[1], 'YMD':line[2], 'STA_HM':line[3], 'END_HM':line[4], 'TYPE':line[5], 
            'APPL_ID':line[6], 'DEL_YN':line[7], 'BF_APPL_ID':line[8], 'APPL_TXT':line[9], 'REWARD_TYPE':line[10]}
        origin_table=origin_table.append(data,ignore_index=True)

    today=origin_table.at[1, 'YMD']

    #1. bf_appl_list와 매칭되는 행 삭제
    origin_table = origin_table.drop(index=origin_table.loc[origin_table.DEL_YN == 'Y'].index)
    #2. DEL_YN이 Y인 행 삭제
    origin_table = origin_table.drop(index=origin_table.loc[origin_table.BF_APPL_ID != 'NULL'].index)
    #3. bf_appl_id가 NULL이 아닌 행 삭제
    origin_table = origin_table.drop(index=origin_table.loc[(origin_table.TYPE=='1010') & (origin_table.YMD == 'NULL')].index)

    origin_table['TIME']=origin_table['STA_HM']+'~'+origin_table['END_HM']
    #STA_HM과 END_HM concat
    
    origin_table.drop(['DEL_YN', 'BF_APPL_ID'], axis = 'columns', inplace= True)

    origin_table.loc[origin_table.TYPE =='1044','YMD']=datetime.now().strftime('%Y%m%d')
    origin_table.loc[origin_table.TIME =='NULL~NULL','TIME']='0000~0000'

    # pd.set_option('display.max_row', 2000) # 최대 출력 행 설정

    column=['YMD', 'EMP_ID', 'DAYOFF1_TIME','DAYOFF1_ID','DAYOFF2_TIME','DAYOFF2_ID','DAYOFF3_TIME','DAYOFF3_ID','DAYOFF4_TIME','DAYOFF4_ID',
    'OVER1_TIME','OVER1_ID','OVER2_TIME','OVER2_ID','OVER3_TIME','OVER3_ID','OVER4_TIME','OVER4_ID','BUSI_TRIP1_TIME','BUSI_TRIP1_ID',
    'BUSI_TRIP2_TIME','BUSI_TRIP2_ID','BUSI_TRIP3_TIME','BUSI_TRIP3_ID','BUSI_TRIP4_TIME','BUSI_TRIP4_ID',
    'HOME_ID','ETC_INFO','ETC_ID','REWARD_TIME','REWARD_ID']

    cur.execute('SELECT emp_id FROM connect.hr_info')

    merge_table = pd.DataFrame(columns=column)

    for emp_id in cur:#사번정보 로드
        data={
            'EMP_ID': emp_id[0],
            'YMD': today
        }
        merge_table=merge_table.append(data, ignore_index=True)


    merge_table.fillna('None', inplace=True) #NaN값 None으로 채우기

    temp_time='' #시작~끝 문자열 저장
    dayoff_string_list=['DAYOFF1_TIME','DAYOFF2_TIME','DAYOFF3_TIME','DAYOFF4_TIME']
    dayoff_string_id_list=['DAYOFF1_ID','DAYOFF2_ID','DAYOFF3_ID','DAYOFF4_ID']
    overtime_string_list=['OVER1_TIME','OVER2_TIME','OVER3_TIME','OVER4_TIME']
    overtime_string_id_list=['OVER1_ID','OVER2_ID','OVER3_ID','OVER4_ID']
    busitrip_string_list=['BUSI_TRIP1_TIME','BUSI_TRIP2_TIME','BUSI_TRIP3_TIME','BUSI_TRIP4_TIME']
    busitrip_string_id_list=['BUSI_TRIP1_ID','BUSI_TRIP2_ID','BUSI_TRIP3_ID','BUSI_TRIP4_ID']

    origin_table.reset_index(inplace=True, drop=False)

    insert_flag=0 # 값이 삽입되었다는 것을 알리는 플래그. 1이 되면 string list 탐색을 중지하고 다음 origin table튜플을 탐색
    
    # 연차 / 초과근무 / 출장
    for idx in range(len(origin_table)):
        rows_origin=origin_table.loc[idx] #origin table 행
        cond_emp_id=merge_table['EMP_ID']==rows_origin['EMP_ID']
        merge_index=merge_table.loc[cond_emp_id,'EMP_ID'].keys()[0] #merge table과 사번 일치하는 행 인덱스넘버

        insert_flag=0 # 값이 삽입되었다는 것을 알리는 플래그. 1이 되면 string list 탐색을 중지하고 다음 origin table튜플을 탐색
        
        if rows_origin['TYPE']=='1008': #초과근무
            if rows_origin['STA_HM']=='NULL': # 널값 들어 있을 때
                continue
            temp_time=rows_origin['STA_HM']+'~'+rows_origin['END_HM']
            
            if rows_origin['REWARD_TYPE']!='NULL': # reward type이 정의되어 있을 때(주말)
                
                merge_table.at[merge_index,'REWARD_TIME']=temp_time
                merge_table.at[merge_index,'REWARD_ID']=rows_origin['REWARD_TYPE']
                continue
            
            for isvalue in overtime_string_list: #merge table 슬롯 하나씩 채우기
                if insert_flag==1:
                    break
                
                if merge_table.loc[merge_index][isvalue]=='None':
                    insert_flag=1
                    merge_table.at[merge_index,isvalue]=temp_time
                    merge_table.at[merge_index,overtime_string_id_list[overtime_string_list.index(isvalue)]]=rows_origin['APPL_ID']

        elif rows_origin['TYPE']=='1002': #연차
            if rows_origin['STA_HM']=='NULL':
                continue
            temp_time=rows_origin['STA_HM']+'~'+rows_origin['END_HM']
            for isvalue in dayoff_string_list:
                if insert_flag==1:
                    break
                
                if merge_table.loc[merge_index][isvalue]=='None':
                    insert_flag=1
                    merge_table.at[merge_index,isvalue]=temp_time
                    merge_table.at[merge_index,dayoff_string_id_list[dayoff_string_list.index(isvalue)]]=rows_origin['APPL_ID']
                    
        elif rows_origin['TYPE']=='1010': #출장
            temp_time=rows_origin['STA_HM']+'~'+rows_origin['END_HM']
            for isvalue in busitrip_string_list:
                if insert_flag==1:
                    break
                
                if merge_table.loc[merge_index][isvalue]=='None':
                    insert_flag=1
                    merge_table.at[merge_index,isvalue]=temp_time
                    merge_table.at[merge_index,busitrip_string_id_list[busitrip_string_list.index(isvalue)]]=rows_origin['APPL_ID']                

        elif rows_origin['TYPE']=='1044': #재택
            merge_table.at[merge_index, 'HOME_ID']=rows_origin['APPL_ID']
            
        elif rows_origin['TYPE']=='1044': #재택
            merge_table.at[merge_index, 'HOME_ID']=rows_origin['APPL_ID']
            
        elif rows_origin['TYPE']=='1004': #기타휴가 
            merge_table.at[merge_index, 'ETC_INFO']=rows_origin['APPL_TXT']
            merge_table.at[merge_index, 'ETC_ID']=rows_origin['APPL_ID']

    parameters='%s,'*31 

    for i in range(len(merge_table)):
        sql=f"INSERT INTO connect.ehr_cal values ({str(i+1)}, {parameters[:-1]})" #날짜별 NUM(사번연번) + 31개의 parameters
        cur.execute(sql, list(merge_table.loc[i]))
    conn.commit()
    
conn.close()
OracleConnect.close()
