import cx_Oracle         # 오라클과 연동하는 패키지
import os
import pymysql             # mariadb와 연동하는 패키지
import pandas as pd
from datetime import datetime
from login_info import cx_Oracle_info as cxinfo, pymysql_info as mysqlinfo

LOCATION = ".\instantclient-basic-windows.x64-21.3.0.0.0\instantclient_21_3"         # 오라클 연동하는 프로그램의 위치 필요.
os.environ["PATH"] = LOCATION + ";" + os.environ["PATH"]
OracleConnect = cx_Oracle.connect(cxinfo['id'], cxinfo['pw'], cxinfo['host'])       # 오라클 연동 정보입력
OracleCursor = OracleConnect.cursor()  #오라클 sql문 쓰기 위한 커서

conn=pymysql.connect(host=mysqlinfo['host'], user=mysqlinfo['user'], password=mysqlinfo['password'], 
                    db=mysqlinfo['db'], charset=mysqlinfo['charset'])       # mariadb 연동 정보입력
cur=conn.cursor() #pymysql 커서

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
and NVL(B.ymd, C.ymd)=(SELECT TO_CHAR(SYSDATE-1, 'YYYYMMDD')AS YYYYMMDD FROM DUAL)) 
or a.appl_type='1044' and substr(a.appl_txt,5,10)=(SELECT TO_CHAR(SYSDATE-1, 'YYYY.MM.DD')AS YYYYMMDD FROM DUAL))
"""
OracleCursor.execute(oracleSql)
origin_table=pd.DataFrame()

for line in OracleCursor:
    data={'EMP_ID':line[0], 'APPR_YMD':line[1], 'YMD':line[2], 'STA_HM':line[3], 'END_HM':line[4], 'TYPE':line[5], 
        'APPL_ID':line[6], 'DEL_YN':line[7], 'BF_APPL_ID':line[8], 'APPL_TXT':line[9], 'REWARD_TYPE':line[10]}
    origin_table=origin_table.append(data,ignore_index=True)

today=origin_table.at[1, 'YMD']

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

column=['YMD','EMP_ID','ORG_NM','SHIFT_CD','WORK_TYPE','PLAN', 'INOUT', 'FIX','DAYOFF1_TIME','DAYOFF1_ID','DAYOFF2_TIME','DAYOFF2_ID','DAYOFF3_TIME','DAYOFF3_ID','DAYOFF4_TIME','DAYOFF4_ID',
'OVER1_TIME','OVER1_ID','OVER2_TIME','OVER2_ID','OVER3_TIME','OVER3_ID','OVER4_TIME','OVER4_ID','BUSI_TRIP1_TIME','BUSI_TRIP1_ID',
'BUSI_TRIP2_TIME','BUSI_TRIP2_ID','BUSI_TRIP3_TIME','BUSI_TRIP3_ID','BUSI_TRIP4_TIME','BUSI_TRIP4_ID',
'HOME_ID','ETC_INFO','ETC_ID','REWARD_TIME','REWARD_ID']

cur.execute('SELECT emp_id, org_nm FROM connect.hr_info')

merge_table = pd.DataFrame(columns=column)

for emp_id in cur:#사번정보 로드
    data={
        'EMP_ID': emp_id[0],
        'ORG_NM': emp_id[1],
        'YMD': today
    }
    merge_table=merge_table.append(data, ignore_index=True)

emp_id=merge_table['EMP_ID']

merge_table.fillna('None', inplace=True) #NaN값 None으로 채우기

temp_time='' #시작~끝 문자열 저장
dayoff_string_list=['DAYOFF1_TIME','DAYOFF2_TIME','DAYOFF3_TIME','DAYOFF4_TIME']
dayoff_string_id_list=['DAYOFF1_ID','DAYOFF2_ID','DAYOFF3_ID','DAYOFF4_ID']
overtime_string_list=['OVER1_TIME','OVER2_TIME','OVER3_TIME','OVER4_TIME']
overtime_string_id_list=['OVER1_ID','OVER2_ID','OVER3_ID','OVER4_ID']
busitrip_string_list=['BUSI_TRIP1_TIME','BUSI_TRIP2_TIME','BUSI_TRIP3_TIME','BUSI_TRIP4_TIME']
busitrip_string_id_list=['BUSI_TRIP1_ID','BUSI_TRIP2_ID','BUSI_TRIP3_ID','BUSI_TRIP4_ID']

origin_table.reset_index(inplace=True, drop=False)

insert_flag=0

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
        
    elif rows_origin['TYPE']=='1004': #기타휴가 
        merge_table.at[merge_index, 'ETC_INFO']=rows_origin['APPL_TXT']
        merge_table.at[merge_index, 'ETC_ID']=rows_origin['APPL_ID']

    #근무유형 삽입하기
OracleCursor.execute("SELECT EMP_ID,SHIFT_CD,WORK_TYPE FROM EHR2011060.TAM5400_V WHERE YMD =(SELECT TO_CHAR(SYSDATE-1, 'YYYYMMDD')AS YYYYMMDD FROM DUAL)")
insert_table=pd.DataFrame() #근무유형 데이터프레임

for line in OracleCursor:
    data={'EMP_ID':line[0], 'SHIFT_CD':line[1], 'WORK_TYPE':line[2]}
    insert_table=insert_table.append(data,ignore_index=True)

for idx in range(len(insert_table)): #근무유형 삽입
    rows_insert=insert_table.loc[idx] #insert table 행
    cond_emp_id=merge_table['EMP_ID']==rows_insert['EMP_ID']
    comp_index=merge_table.loc[cond_emp_id,'EMP_ID'].keys()[0] #merge table과 사번 일치하는 행 인덱스넘버

    merge_table.loc[comp_index]['SHIFT_CD']=insert_table.loc[idx]['SHIFT_CD']
    merge_table.loc[comp_index]['WORK_TYPE']=insert_table.loc[idx]['WORK_TYPE']

std_start = '0900'
std_end = '1800'

    # 초과근무 적용하여 계획시간 만들기
for i in range(len(merge_table)):
    overtime = ''
    if merge_table.loc[i]['OVER1_TIME'] != 'None':
        over_start, over_end = merge_table.loc[i]['OVER1_TIME'].split('~')
        if over_start < std_start:
            overtime = over_start
        else:
            overtime = std_start
        overtime = overtime + '~'
        if over_end > std_end:
            overtime = overtime + over_end
        else:
            overtime = overtime + std_end
        merge_table.loc[i]['PLAN'] = overtime   #새로 컬럼 만들어서 넣어야 함



    # 기록기 시간 만들기
for i in range(len(emp_id)):
    inout = ''
    tmp_end = ''
    cur.execute("SELECT WORK_INFO_CLOCK FROM connect.at_att_inout AS T WHERE " + emp_id[i] + " = T.EMP_CODE AND (DATE_FORMAT(CURDATE()-1, '%Y%m%d')) = T.WORK_DATE AND T.WORK_CD = 'IN' ORDER BY T.WORK_INFO_CLOCK LIMIT 1")
    for line in cur:
        inout = line[0]
    cur.execute("SELECT WORK_INFO_CLOCK FROM connect.at_att_inout AS T WHERE " + emp_id[i] + " = T.EMP_CODE AND (DATE_FORMAT(CURDATE()-1, '%Y%m%d')) = T.WORK_DATE AND T.WORK_CD = 'OUT' ORDER BY T.WORK_INFO_CLOCK DESC LIMIT 1")
    inout = inout + '~'
    for line in cur:
        tmp_end = line[0]
        inout = inout + tmp_end
    merge_table.loc[i]['INOUT'] = inout

    # 확정시간 만들기(0900~1800 근무자) 시차출퇴근자, 휴일 등 추가 적용 필요
for i in range(len(merge_table)):
    if merge_table.loc[i]['WORK_TYPE']=='None': # NULL 값 제외
        pass

    elif merge_table.loc[i]['WORK_TYPE']=='0060': # 휴일 근무 계산
        pass

    else:
        if merge_table.loc[i]['SHIFT_CD']=='0030': #기본출퇴근자
            if merge_table.loc[i]['OVER1_TIME'] == 'None': #Plan 값 설정
                merge_table.loc[i]['PLAN'] = '0900~1800'
            if merge_table.loc[i]['INOUT'] == '~':
                merge_table.loc[i]['INOUT'] = '0900~1800'
            elif merge_table.loc[i]['INOUT'][0] == '~':
                merge_table.loc[i]['INOUT']= '0900'+ merge_table.loc[i]['INOUT']
            elif merge_table.loc[i]['INOUT'][-1] == '~':
                merge_table.loc[i]['INOUT'] += '1800'
            if merge_table.loc[i]['WORK_TYPE']=='0030': #기본출퇴근
                fixtime = ''
                inout_start, inout_end = merge_table.loc[i]['INOUT'].split('~')
                # 시간 비교를 위해 분단위로 고쳐주기
                inout_start_hour = inout_start[:2]
                inout_start_min = inout_start[-2:]
                inout_end_hour = inout_end[:2]
                inout_end_min = inout_end[-2:]
                inout_start_cal = int(inout_start_hour)*60 + int(inout_start_min)
                inout_end_cal = int(inout_end_hour)*60 + int(inout_end_min)
                #출근
                if merge_table.loc[i]['PLAN'][:4] < '0900': #출근이전 시간외 신청
                    if 510 >= inout_start_cal: #30분 빼고 비교
                        fixtime = inout_start
                        if merge_table.loc[i]['PLAN'][:4] > inout_start:
                            fixtime = merge_table.loc[i]['PLAN'][:4]
                    else:
                        fixtime = '0900'
                elif merge_table.loc[i]['PLAN'][:4] == '0900': #출근이전 시간외 미신청
                    fixtime = '0900'
                fixtime = fixtime + '~'
                #퇴근
                if merge_table.loc[i]['PLAN'][-4:] > '1800': #퇴근이후 시간외 신청
                    if inout_end_cal >= 1110:
                        if merge_table.loc[i]['PLAN'][-4:] < inout_end:
                            fixtime = fixtime + merge_table.loc[i]['PLAN'][-4:]
                        else:
                            fixtime += inout_end                  
                    else:
                        fixtime = fixtime + '1800'
                elif merge_table.loc[i]['PLAN'][-4:] == '1800': #퇴근이전 시간외 미신청
                    fixtime = fixtime + '1800'
                merge_table.loc[i]['FIX'] = fixtime

            elif merge_table.loc[i]['WORK_TYPE']=='0290': #기본출퇴근자-재택
                inout=''
                inout_in = merge_table.loc[i]['INOUT'][:4]
                inout_out = merge_table.loc[i]['INOUT'][5:]

                if inout_in<='0900':
                    inout='0900~'
                else:
                    inout=inout_in + '~'
                if inout_out >='1800':
                    inout += '1800'
                else:
                    inout += inout_out
                merge_table.loc[i]['FIX']= inout

        elif merge_table.loc[i]['SHIFT_CD']=='0020': #시차출퇴근(8-17),시차출퇴근(8-17)_재택
            if merge_table.loc[i]['OVER1_TIME'] == 'None': #Plan 값 설정
                merge_table.loc[i]['PLAN'] = '0800~1700'
            if merge_table.loc[i]['INOUT'][0] == '~':
                pass
            elif merge_table.loc[i]['INOUT'][-1] == '~':
                pass
            else:
                fixtime = ''
                inout_start, inout_end = merge_table.loc[i]['INOUT'].split('~')
                # 시간 비교를 위해 분단위로 고쳐주기
                inout_start_hour = inout_start[:2]
                inout_start_min = inout_start[-2:]
                inout_end_hour = inout_end[:2]
                inout_end_min = inout_end[-2:]
                inout_start_cal = int(inout_start_hour)*60 + int(inout_start_min)
                inout_end_cal = int(inout_end_hour)*60 + int(inout_end_min)
                #출근
                if merge_table.loc[i]['PLAN'][:4] < '0800': #출근이전 시간외 신청
                    if 450 >= inout_start_cal: #30분 빼고 비교
                        fixtime = inout_start
                        if merge_table.loc[i]['PLAN'][:4] > inout_start:
                            fixtime = merge_table.loc[i]['PLAN'][:4]
                    elif  450 <= inout_start_cal <= 480:
                        fixtime = '0800'
                    else:
                        fixtime= merge_table.loc[i]['INOUT'][:4]
                elif merge_table.loc[i]['PLAN'][:4] == '0800': #출근이전 시간외 미신청
                    if merge_table.loc[i]['INOUT'][:4] < '0800':
                        fixtime = '0800'
                    else:
                        fixtime = merge_table.loc[i]['INOUT'][:4]
                fixtime = fixtime + '~'
                #퇴근
                if merge_table.loc[i]['PLAN'][-4:] > '1700': #퇴근이후 시간외 신청
                    if inout_end_cal >= 1050:
                        if merge_table.loc[i]['PLAN'][-4:] < inout_end:
                            fixtime = fixtime + merge_table.loc[i]['PLAN'][-4:]
                        else:
                            fixtime += inout_end                  
                    else:
                        fixtime = fixtime + '1700'
                elif merge_table.loc[i]['PLAN'][-4:] == '1700': #퇴근이전 시간외 미신청
                    if inout_end_cal >= 1020:
                        fixtime +='1700'
                    else:
                        fixtime += merge_table.loc[i]['INOUT'][-4:]
                merge_table.loc[i]['FIX'] = fixtime

        elif merge_table.loc[i]['SHIFT_CD']=='0040': #시차출퇴근(10-19), 시차출퇴근(10-19)_재택
            if merge_table.loc[i]['OVER1_TIME'] == 'None': #Plan 값 설정
                merge_table.loc[i]['PLAN'] = '1000~1900'
            if merge_table.loc[i]['INOUT'][0] == '~':
                pass
            elif merge_table.loc[i]['INOUT'][-1] == '~':
                pass
            else:
                fixtime = ''
                inout_start, inout_end = merge_table.loc[i]['INOUT'].split('~')
                # 시간 비교를 위해 분단위로 고쳐주기
                inout_start_hour = inout_start[:2]
                inout_start_min = inout_start[-2:]
                inout_end_hour = inout_end[:2]
                inout_end_min = inout_end[-2:]
                inout_start_cal = int(inout_start_hour)*60 + int(inout_start_min)
                inout_end_cal = int(inout_end_hour)*60 + int(inout_end_min)
                #출근
                if merge_table.loc[i]['PLAN'][:4] < '1000': #출근이전 시간외 신청
                    if 570 >= inout_start_cal: #30분 빼고 비교
                        fixtime = inout_start
                        if merge_table.loc[i]['PLAN'][:4] > inout_start:
                            fixtime = merge_table.loc[i]['PLAN'][:4]
                    elif  570 <= inout_start_cal <= 600:
                        fixtime = '1000'
                    else:
                        fixtime= merge_table.loc[i]['INOUT'][:4]
                elif merge_table.loc[i]['PLAN'][:4] == '1000': #출근이전 시간외 미신청
                    if merge_table.loc[i]['INOUT'][:4] < '1000':
                        fixtime = '1000'
                    else:
                        fixtime = merge_table.loc[i]['INOUT'][:4]
                fixtime = fixtime + '~'
                #퇴근
                if merge_table.loc[i]['PLAN'][-4:] > '1900': #퇴근이후 시간외 신청
                    if inout_end_cal >= 1170:
                        if merge_table.loc[i]['PLAN'][-4:] < inout_end:
                            fixtime = fixtime + merge_table.loc[i]['PLAN'][-4:]
                        else:
                            fixtime += inout_end                  
                    else:
                        fixtime = fixtime + '1900'
                elif merge_table.loc[i]['PLAN'][-4:] == '1900': #퇴근이전 시간외 미신청
                    if inout_end_cal >= 1140:
                        fixtime +='1900'
                    else:
                        fixtime += merge_table.loc[i]['INOUT'][-4:]
                merge_table.loc[i]['FIX'] = fixtime

        elif merge_table.loc[i]['SHIFT_CD']=='0440':
            if merge_table.loc[i]['OVER1_TIME'] == 'None': #Plan 값 설정
                merge_table.loc[i]['PLAN'] = '0800~1500'
            if merge_table.loc[i]['INOUT'][0] == '~':
                pass
            elif merge_table.loc[i]['INOUT'][-1] == '~':
                pass
            else:
                fixtime = ''
                inout_start, inout_end = merge_table.loc[i]['INOUT'].split('~')
                # 시간 비교를 위해 분단위로 고쳐주기
                inout_start_hour = inout_start[:2]
                inout_start_min = inout_start[-2:]
                inout_end_hour = inout_end[:2]
                inout_end_min = inout_end[-2:]
                inout_start_cal = int(inout_start_hour)*60 + int(inout_start_min)
                inout_end_cal = int(inout_end_hour)*60 + int(inout_end_min)
                #출근
                if merge_table.loc[i]['PLAN'][:4] < '0800': #출근이전 시간외 신청
                    if 450 >= inout_start_cal: #30분 빼고 비교
                        fixtime = inout_start
                        if merge_table.loc[i]['PLAN'][:4] > inout_start:
                            fixtime = merge_table.loc[i]['PLAN'][:4]
                    elif  450 <= inout_start_cal <= 480:
                        fixtime = '0800'
                    else:
                        fixtime= merge_table.loc[i]['INOUT'][:4]
                elif merge_table.loc[i]['PLAN'][:4] == '0800': #출근이전 시간외 미신청
                    if merge_table.loc[i]['INOUT'][:4] < '0800':
                        fixtime = '0800'
                    else:
                        fixtime = merge_table.loc[i]['INOUT'][:4]
                fixtime = fixtime + '~'
                #퇴근
                if merge_table.loc[i]['PLAN'][-4:] > '1500': #퇴근이후 시간외 신청
                    if inout_end_cal >= 930:
                        if merge_table.loc[i]['PLAN'][-4:] < inout_end:
                            fixtime = fixtime + merge_table.loc[i]['PLAN'][-4:]
                        else:
                            fixtime += inout_end                  
                    else:
                        fixtime = fixtime + '1500'
                elif merge_table.loc[i]['PLAN'][-4:] == '1500': #퇴근이전 시간외 미신청
                    if inout_end_cal >= 900:
                        fixtime +='1500'
                    else:
                        fixtime += merge_table.loc[i]['INOUT'][-4:]
                merge_table.loc[i]['FIX'] = fixtime

        elif merge_table.loc[i]['SHIFT_CD']=='0170':
            if merge_table.loc[i]['OVER1_TIME'] == 'None': #Plan 값 설정
                merge_table.loc[i]['PLAN'] = '1000~1700'
            if merge_table.loc[i]['INOUT'][0] == '~':
                pass
            elif merge_table.loc[i]['INOUT'][-1] == '~':
                pass
            else:
                fixtime = ''
                inout_start, inout_end = merge_table.loc[i]['INOUT'].split('~')
                # 시간 비교를 위해 분단위로 고쳐주기
                inout_start_hour = inout_start[:2]
                inout_start_min = inout_start[-2:]
                inout_end_hour = inout_end[:2]
                inout_end_min = inout_end[-2:]
                inout_start_cal = int(inout_start_hour)*60 + int(inout_start_min)
                inout_end_cal = int(inout_end_hour)*60 + int(inout_end_min)
                #출근
                if merge_table.loc[i]['PLAN'][:4] < '1000': #출근이전 시간외 신청
                    if 570 >= inout_start_cal: #30분 빼고 비교
                        fixtime = inout_start
                        if merge_table.loc[i]['PLAN'][:4] > inout_start:
                            fixtime = merge_table.loc[i]['PLAN'][:4]
                    elif  570 <= inout_start_cal <= 600:
                        fixtime = '1000'
                    else:
                        fixtime= merge_table.loc[i]['INOUT'][:4]
                elif merge_table.loc[i]['PLAN'][:4] == '1000': #출근이전 시간외 미신청
                    if merge_table.loc[i]['INOUT'][:4] < '1000':
                        fixtime = '1000'
                    else:
                        fixtime = merge_table.loc[i]['INOUT'][:4]
                fixtime = fixtime + '~'
                #퇴근
                if merge_table.loc[i]['PLAN'][-4:] > '1700': #퇴근이후 시간외 신청
                    if inout_end_cal >= 1050:
                        if merge_table.loc[i]['PLAN'][-4:] < inout_end:
                            fixtime = fixtime + merge_table.loc[i]['PLAN'][-4:]
                        else:
                            fixtime += inout_end                  
                    else:
                        fixtime = fixtime + '1700'
                elif merge_table.loc[i]['PLAN'][-4:] == '1700': #퇴근이전 시간외 미신청
                    if inout_end_cal >= 1020:
                        fixtime +='1700'
                    else:
                        fixtime += merge_table.loc[i]['INOUT'][-4:]
                merge_table.loc[i]['FIX'] = fixtime
            

parameters='%s,'*37

# print(merge_table.head(40))

for i in range(len(merge_table)):
    sql=f"INSERT INTO good.ehr_cal_test2 values ({str(i+1)}, {parameters[:-1]})" #날짜별 NUM(사번연번) + 37개의 parameters
    cur.execute(sql, list(merge_table.loc[i]))
conn.commit()
conn.close()
OracleConnect.close()